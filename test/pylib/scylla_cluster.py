#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Scylla clusters and management for Python tests"""

import aiohttp
import asyncio
import itertools
import logging
import os
import pathlib
import shutil
import tempfile
import time
import uuid
from typing import Optional, Dict, List, Set, Callable, Awaitable
from cassandra import InvalidRequest                    # type: ignore
from cassandra import OperationTimedOut                 # type: ignore
from cassandra.auth import PlainTextAuthProvider        # type: ignore
from cassandra.cluster import Cluster, NoHostAvailable  # type: ignore
from cassandra.cluster import Session                   # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore
from test.pylib.pool import Pool
from test.pylib.artifact_registry import ArtifactRegistry
from test.pylib.host_registry import HostRegistry

#
# Put all Scylla options in a template file. Sic: if you make a typo in the
# configuration file, Scylla will boot fine and ignore the setting.
# Always check the error log after modifying the template.
#
SCYLLA_CONF_TEMPLATE = """cluster_name: {cluster_name}
developer_mode: true

# Allow testing experimental features. Following issue #9467, we need
# to add here specific experimental features as they are introduced.

enable_user_defined_functions: true
experimental: true
experimental_features:
    - raft
    - udf

data_file_directories:
    - {workdir}/data
commitlog_directory: {workdir}/commitlog
hints_directory: {workdir}/hints
view_hints_directory: {workdir}/view_hints

listen_address: {host}
rpc_address: {host}
api_address: {host}
prometheus_address: {host}
alternator_address: {host}

seed_provider:
    - class_name: org.apache.cassandra.locator.simple_seed_provider
      parameters:
          - seeds: {seeds}

skip_wait_for_gossip_to_settle: 0
ring_delay_ms: 0
num_tokens: 16
flush_schema_tables_after_modification: false
auto_snapshot: false

# Significantly increase default timeouts to allow running tests
# on a very slow setup (but without network losses). Note that these
# are server-side timeouts: The client should also avoid timing out
# its own requests - for this reason we increase the CQL driver's
# client-side timeout in conftest.py.

range_request_timeout_in_ms: 300000
read_request_timeout_in_ms: 300000
counter_write_request_timeout_in_ms: 300000
cas_contention_timeout_in_ms: 300000
truncate_request_timeout_in_ms: 300000
write_request_timeout_in_ms: 300000
request_timeout_in_ms: 300000

# Set up authentication in order to allow testing this module
# and other modules dependent on it: e.g. service levels

authenticator: {authenticator}
authorizer: {authorizer}
strict_allow_filtering: true

permissions_update_interval_in_ms: 100
permissions_validity_in_ms: 100
"""

# Seastar options can not be passed through scylla.yaml, use command line
# for them. Keep everything else in the configuration file to make
# it easier to restart. Sic: if you make a typo on the command line,
# Scylla refuses to boot.
SCYLLA_CMDLINE_OPTIONS = [
    '--smp', '2',
    '-m', '1G',
    '--collectd', '0',
    '--overprovisioned',
    '--max-networking-io-control-blocks', '100',
    '--unsafe-bypass-fsync', '1',
    '--kernel-page-cache', '1',
]


class ScyllaServer:
    START_TIMEOUT = 300     # seconds

    def __init__(self, exe: str, vardir: str,
                 host_registry,
                 cluster_name: str, seed: Optional[str],
                 cmdline_options: List[str],
                 config_options: Dict[str, str] = {"authenticator": "AllowAllAuthenticator",
                                                   "authorizer": "AllowAllAuthorizer"}) -> None:
        self.exe = pathlib.Path(exe).resolve()
        self.vardir = pathlib.Path(vardir)
        self.host_registry = host_registry
        self.cmdline_options = cmdline_options
        self.cluster_name = cluster_name
        self.hostname = ""
        self.seed = seed
        self.cmd: Optional[asyncio.subprocess.Process] = None
        self.log_savepoint = 0
        self.control_cluster: Optional[Cluster] = None
        self.control_connection: Optional[Session] = None
        self.authenticator: str = config_options["authenticator"]
        self.authorizer: str = config_options["authorizer"]

    async def install_and_start(self) -> None:
        await self.install()

        logging.info("starting server at host %s in %s...", self.hostname,
                     self.workdir.name)

        await self.start()

        if self.cmd:
            logging.info("started server at host %s in %s, pid %d", self.hostname,
                         self.workdir.name, self.cmd.pid)

    @property
    def is_running(self) -> bool:
        return self.cmd is not None

    @property
    def host(self) -> str:
        return str(self.hostname)

    def find_scylla_executable(self) -> None:
        if not os.access(self.exe, os.X_OK):
            raise RuntimeError("{} is not executable", self.exe)

    async def install(self) -> None:
        """Create a working directory with all subdirectories, initialize
        a configuration file."""

        self.find_scylla_executable()

        # Scylla assumes all instances of a cluster use the same port,
        # so each instance needs an own IP address.
        self.hostname = await self.host_registry.lease_host()
        if not self.seed:
            self.seed = self.hostname
        # Use the last part in host IP 127.151.3.27 -> 27
        # There can be no duplicates within the same test run
        # thanks to how host registry registers subnets, and
        # different runs use different vardirs.
        shortname = pathlib.Path("scylla-" + self.host.split(".")[-1])
        self.workdir = self.vardir / shortname

        logging.info("installing Scylla server in %s...", self.workdir)

        self.log_filename = self.vardir / shortname.with_suffix(".log")

        self.config_filename = self.workdir / "conf/scylla.yaml"

        # Delete the remains of the previous run

        # Cleanup any remains of the previously running server in this path
        shutil.rmtree(self.workdir, ignore_errors=True)

        self.workdir.mkdir(parents=True, exist_ok=True)
        self.config_filename.parent.mkdir(parents=True, exist_ok=True)
        # Create a configuration file.
        fmt = {
              "cluster_name": self.cluster_name,
              "host": self.hostname,
              "seeds": self.seed,
              "workdir": self.workdir,
              "authenticator": self.authenticator,
              "authorizer": self.authorizer
        }
        with self.config_filename.open('w') as config_file:
            config_file.write(SCYLLA_CONF_TEMPLATE.format(**fmt))

        self.log_file = self.log_filename.open("wb")

    def take_log_savepoint(self) -> None:
        """Save the server current log size when a test starts so that if
        the test fails, we can only capture the relevant lines of the log"""
        self.log_savepoint = self.log_file.tell()

    def read_log(self) -> str:
        """ Return first 3 lines of the log + everything that happened
        since the last savepoint. Used to diagnose CI failures, so
        avoid a nessted exception."""
        try:
            with self.log_filename.open("r") as log:
                # Read the first 5 lines of the start log
                lines: List[str] = []
                for i in range(3):
                    lines.append(log.readline())
                # Read the lines since the last savepoint
                if self.log_savepoint and self.log_savepoint > log.tell():
                    log.seek(self.log_savepoint)
                return "".join(lines + log.readlines())
        except Exception as e:
            return "Exception when reading server log {}: {}".format(self.log_filename, str(e))

    async def cql_is_up(self) -> bool:
        """Test that CQL is serving (a check we use at start up)."""
        caslog = logging.getLogger('cassandra')
        oldlevel = caslog.getEffectiveLevel()
        # Be quiet about connection failures.
        caslog.setLevel('CRITICAL')
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        # auth::standard_role_manager creates "cassandra" role in an
        # async loop auth::do_after_system_ready(), which retries
        # role creation with an exponential back-off. In other
        # words, even after CQL port is up, Scylla may still be
        # initializing. When the role is ready, queries begin to
        # work, so rely on this "side effect".
        profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([self.hostname]),
                                   request_timeout=self.START_TIMEOUT)
        try:
            # In a cluster setup, it's possible that the CQL
            # here is directed to a node different from the initial contact
            # point, so make sure we execute the checks strictly via
            # this connection
            with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                         contact_points=[self.hostname],
                         # This is the latest version Scylla supports
                         protocol_version=4,
                         auth_provider=auth) as cluster:
                with cluster.connect() as session:
                    session.execute("CREATE KEYSPACE IF NOT EXISTS k WITH REPLICATION = {" +
                                    "'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                    session.execute("DROP KEYSPACE k")
                    self.control_cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                                                   contact_points=[self.hostname],
                                                   auth_provider=auth)
                    self.control_connection = self.control_cluster.connect()
                    return True
        except (NoHostAvailable, InvalidRequest, OperationTimedOut) as e:
            logging.debug("Exception when checking if CQL is up: {}".format(e))
            return False
        finally:
            caslog.setLevel(oldlevel)
        # Any other exception may indicate a problem, and is passed to the caller.

    async def rest_api_is_up(self) -> bool:
        """Test that the Scylla REST API is serving. Can be used as a
        checker function at start up."""
        try:
            async with aiohttp.ClientSession() as s:
                url = "http://{}:10000/".format(self.hostname)
                async with s.get(url):
                    return True
        except aiohttp.ClientConnectionError:
            return False
        # Any other exception may indicate a problem, and is passed to the caller.

    async def start(self) -> None:
        """Start an installed server. May be used for restarts."""

        # Add suite-specific command line options
        scylla_args = SCYLLA_CMDLINE_OPTIONS + self.cmdline_options
        env = os.environ.copy()
        env.clear()     # pass empty env to make user user's SCYLLA_HOME has no impact
        self.cmd = await asyncio.create_subprocess_exec(
            self.exe,
            *scylla_args,
            cwd=self.workdir,
            stderr=self.log_file,
            stdout=self.log_file,
            env=env,
            preexec_fn=os.setsid,
        )

        self.start_time = time.time()
        sleep_interval = 0.1

        while time.time() < self.start_time + self.START_TIMEOUT:
            if self.cmd.returncode:
                with self.log_filename.open('r') as log_file:
                    logging.error("failed to start server at host %s in %s",
                                  self.hostname, self.workdir.name)
                    logging.error("last line of {}:".format(self.log_filename))
                    log_file.seek(0, 0)
                    logging.error(log_file.readlines()[-1].rstrip())
                    h = logging.getLogger().handlers[0]
                    logpath = h.baseFilename if hasattr(h, 'baseFilename') else "?"  # type: ignore
                    raise RuntimeError("""Failed to start server at host {}.
Check the log files:
{}
{}""".format(self.hostname, logpath, self.log_filename))

            if await self.rest_api_is_up():
                if await self.cql_is_up():
                    return

            # Sleep and retry
            await asyncio.sleep(sleep_interval)

        raise RuntimeError("failed to start server {}, check server log at {}".format(
            self.host, self.log_filename))

    async def force_schema_migration(self) -> None:
        """This is a hack to change schema hash on an existing cluster node
        which triggers a gossip round and propagation of entire application
        state. Helps quickly propagate tokens and speed up node boot if the
        previous state propagation was missed."""
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([self.seed]),
                                   request_timeout=self.START_TIMEOUT)
        with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                     contact_points=[self.seed],
                     auth_provider=auth,
                     # This is the latest version Scylla supports
                     protocol_version=4,
                     ) as cluster:
            with cluster.connect() as session:
                session.execute("CREATE KEYSPACE IF NOT EXISTS k WITH REPLICATION = {" +
                                "'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                session.execute("DROP KEYSPACE k")

    async def shutdown_control_connection(self) -> None:
        if self.control_connection is not None:
            self.control_connection.shutdown()
            self.control_connection = None
        if self.control_cluster is not None:
            self.control_cluster.shutdown()
            self.control_cluster = None

    async def stop(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGKILL to
        stop, so is not graceful. Waits for the process to exit before return."""
        # Preserve for logging
        hostname = self.hostname
        logging.info("stopping server at host %s in %s", hostname,
                     self.workdir.name)
        if not self.cmd:
            return

        await self.shutdown_control_connection()
        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("stopped server at host %s in %s", hostname,
                             self.workdir.name)
            self.cmd = None

    async def stop_gracefully(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGTERM to
        stop, so it is graceful. Waits for the process to exit before return."""
        # Preserve for logging
        hostname = self.hostname
        logging.info("stopping server at host %s", hostname)
        if not self.cmd:
            return

        await self.shutdown_control_connection()
        try:
            self.cmd.terminate()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("stopped server at host %s", hostname)
            self.cmd = None

    async def uninstall(self) -> None:
        """Clear all files left from a stopped server, including the
        data files and log files."""

        if not self.hostname:
            return
        logging.info("Uninstalling server at %s", self.workdir)

        shutil.rmtree(self.workdir)
        self.log_filename.unlink(missing_ok=True)

        await self.host_registry.release_host(self.hostname)
        self.hostname = ""

    def write_log_marker(self, msg) -> None:
        self.log_file.seek(0, 2)  # seek to file end
        self.log_file.write(msg.encode())
        self.log_file.flush()

    def __str__(self):
        return self.hostname


class ScyllaCluster:
    """Manager for a cluster of Scylla servers"""
    def __init__(self, scylla_exe: str, replicas: int, test_base_dir: str,
                 cmdline_options: List[str], host_registry: HostRegistry,
                 cql_port: int = 9042) -> None:
        self.name = str(uuid.uuid1())
        self.scylla_exe = scylla_exe
        self.replicas = replicas
        self.cluster_dir = tempfile.mkdtemp(prefix="cluster-", dir=test_base_dir)
        self.cmdline_options = cmdline_options
        self.host_registry = host_registry
        self.started: Dict[str, ScyllaServer] = {}
        self.stopped: Dict[str, ScyllaServer] = {}
        self.removed: Set[str] = set()
        self.is_running: bool = True
        self.dirty: bool = False
        self.start_exception: Optional[Exception] = None
        self.keyspace_count = 0
        self.last_seed: Optional[str] = None     # id as IP Address like '127.1.2.3'

    async def install_and_start(self) -> None:
        try:
            for i in range(self.replicas):
                await self.add_server(mark_dirty=False)
            self.keyspace_count = self._get_keyspace_count()
        except (RuntimeError, NoHostAvailable, InvalidRequest, OperationTimedOut) as e:
            # If start fails, swallow the error to throw later,
            # at test time.
            self.start_exception = e
        logging.info("Created cluster %s", self)

    async def uninstall(self) -> None:
        """Stop running servers, uninstall all servers, and remove API socket"""
        logging.info("Uninstalling cluster")
        await self.stop_gracefully()
        await asyncio.gather(*(server.uninstall() for server in self.stopped.values()))
        shutil.rmtree(self.cluster_dir)

    async def stop(self) -> None:
        """Stop all running servers ASAP"""
        logging.info("Cluster %s stopping", self)
        # If self.started is empty, no-op
        await asyncio.gather(*(server.stop() for server in self.started.values()))
        self.stopped.update(self.started)
        self.started.clear()
        self.last_seed = None
        self.is_running = False

    async def stop_gracefully(self) -> None:
        """Stop all running servers in a clean way"""
        logging.info("Cluster %s stopping gracefully", self)
        self.dirty = True
        # If self.started is empty, no-op
        await asyncio.gather(*(server.stop_gracefully() for server in self.started.values()))
        self.stopped.update(self.started)
        self.started.clear()
        self.is_running = False
        self.last_seed = None

    async def add_server(self, mark_dirty:bool=True) -> str:
        """Add a new server to the cluster"""
        if mark_dirty:
            self.dirty = True
        server = ScyllaServer(
            exe=self.scylla_exe,
            vardir=self.cluster_dir,
            host_registry=self.host_registry,
            cluster_name=self.name,
            seed=self.last_seed,
            cmdline_options=self.cmdline_options)
        try:
            logging.info("Cluster %s adding server", self)
            await server.install_and_start()
        except Exception as e:
            logging.error("Failed to start Scylla server at host %s in %s: %s",
                          server.hostname, server.workdir.name, str(e))
        self.started[server.host] = server
        self.last_seed = server.host
        return server.host

    def __getitem__(self, i: int) -> ScyllaServer:
        assert i >= 0, "ScyllaCluster: cluster sub-index must be positive"
        return next(server for pos, server in enumerate(self.started.values()) if pos == i)

    def __str__(self):
        return f"{{{', '.join(str(c) for c in self.started)}}}"

    def _get_keyspace_count(self) -> int:
        """Get the current keyspace count"""
        assert(self.start_exception is None)
        assert self[0].control_connection is not None
        rows = self[0].control_connection.execute(
               "select count(*) as c from system_schema.keyspaces")
        keyspace_count = int(rows.one()[0])
        return keyspace_count

    def before_test(self, name) -> None:
        """Check that  the cluster is ready for a test. If
        there was a start error, throw it here - the server is
        started when it's added to the pool, which can't be attributed
        to any specific test, throwing it here would stop a specific
        test."""
        if self.start_exception:
            raise self.start_exception

        for server in self.started.values():
            server.write_log_marker(f"------ Starting test {name} ------\n")

    def after_test(self, name) -> None:
        """Check that the cluster is still alive and the test
        hasn't left any garbage."""
        assert(self.start_exception is None)
        if self._get_keyspace_count() != self.keyspace_count:
            raise RuntimeError("Test post-condition failed, "
                               "the test must drop all keyspaces it creates.")
        for server in itertools.chain(self.started.values(), self.stopped.values()):
            server.write_log_marker(f"------ Ending test {name} ------\n")

    def update_last_seed(self, removed_host: str) -> None:
        """Update last seed when removing a host"""
        if self.last_seed == removed_host:
            self.last_seed = next(iter(self.started.keys())) if self.started else None

    async def node_stop(self, server_id: str, gracefully: bool) -> bool:
        """Stop a server. No-op if already stopped."""
        self.dirty = True
        logging.info("Cluster %s stopping server %s", self, server_id)
        if server_id in self.stopped or server_id in self.removed:
            return True
        if server_id not in self.started:
            return False
        server = self.started.pop(server_id)
        if gracefully:
            await server.stop_gracefully()
        else:
            await server.stop()
        self.update_last_seed(server.host)
        self.stopped[server_id] = server
        return True

    async def node_start(self, server_id: str) -> bool:
        """Start a stopped node"""
        self.dirty = True
        logging.info("Cluster %s starting server", self)
        server = self.stopped.pop(server_id, None)
        if server is None:
            return False
        if self.last_seed is None:
            self.last_seed = server.host
        server.seed = self.last_seed
        await server.start()
        self.started[server_id] = server
        return True

    async def node_restart(self, server_id: str) -> bool:
        """Restart a running node"""
        self.dirty = True
        logging.info("Cluster %s restarting server %s", self, server_id)
        server = self.started.get(server_id, None)
        if server is None:
            return False
        await server.stop_gracefully()
        await server.start()
        return True

    async def node_remove(self, server_id: str) -> bool:
        """Remove a specified server"""
        self.dirty = True
        logging.info("Cluster %s removing server %s", self, server_id)
        if server_id in self.started:
            server = self.started.pop(server_id)
            await server.stop_gracefully()
            self.update_last_seed(server.host)
        elif server_id in self.stopped:
            server = self.stopped.pop(server_id)
        else:
            return False
        await server.uninstall()
        self.removed.add(server_id)
        return True

    async def node_replace(self, old_node_id) -> Optional[str]:
        """Replace a specified server with a new one"""
        self.dirty = True
        logging.info("Cluster %s replacing server %s", self, old_node_id)
        if old_node_id in self.started:
            server = self.started.pop(old_node_id)
            await server.stop_gracefully()
            self.update_last_seed(server.host)
        elif old_node_id in self.stopped:
            server = self.stopped.pop(old_node_id)
        else:
            return None
        await server.uninstall()
        self.removed.add(old_node_id)
        return await self.add_server()


class Harness:
    """Manages a pool of Scylla clusters for running test cases against"""
    def __init__(self, scylla_exe: str, pool_size, test_base_dir: str, cmdline_options: List[str],
                 topology: dict, save_log: bool, host_registry: HostRegistry,
                 cql_port: int = 9042) -> None:
        self.scylla_exe = scylla_exe
        self.pool_size = pool_size
        self.test_base_dir = test_base_dir
        if isinstance(cmdline_options, str):
            cmdline_options = [cmdline_options]
        self.cmdline_options = cmdline_options
        self.host_registry = host_registry
        self.save_log = save_log
        self.create_cluster = self.topology_for_class(topology["class"], topology)
        self.cql_port = cql_port
        self.clusters = Pool(pool_size, self.create_cluster)
        self.cluster: Optional[ScyllaCluster] = None  # Current cluster
        self.is_before_test_ok: bool = False
        self.is_after_test_ok: bool = False
        self.is_running: bool = False

    async def start(self) -> None:
        """Start Harness, setup API, start first cluster"""
        if not self.is_running:
            await self._get_cluster()
            self.is_running = True

    async def stop(self) -> None:
        """Stop Harness, stops last cluster if present"""
        await self._cluster_finish()
        self.is_running = False

    async def _get_cluster(self) -> None:
        assert self.cluster is None, "Previous cluster should be stopped"
        self.cluster = await self.clusters.get()
        self.is_before_test_ok = False
        self.is_after_test_ok = False

    async def _cluster_finish(self) -> None:
        if self.cluster is not None:
            await self.cluster.stop()
            if not self.save_log:
                # If a test fails, we might want to keep the data dir.
                await self.cluster.uninstall()
            self.cluster = None

    def topology_for_class(self, class_name: str, cfg: dict) -> Callable[[], Awaitable]:
        """Create an async function to build a cluster for topology"""

        if class_name.lower() == "simple":
            async def create_cluster():
                cluster = ScyllaCluster(self.scylla_exe, int(cfg["replication_factor"]),
                                        self.test_base_dir, self.cmdline_options,
                                        self.host_registry, self.cql_port)
                await cluster.install_and_start()
                return cluster

            return create_cluster
        else:
            raise RuntimeError("Unsupported topology name")

    async def before_test(self, test_name: str) -> None:
        if self.cluster is not None and self.cluster.dirty:
            await self._cluster_finish()
        if self.cluster is None:
            await self._get_cluster()
        assert self.cluster is not None
        # TODO: should we do this for all servers?
        self.cluster[0].take_log_savepoint()
        self.cluster.before_test(test_name)
        self.is_before_test_ok = True
        logging.info("Leasing Scylla cluster %s for test %s", self.cluster, test_name)

    async def after_test(self, test_name: str) -> None:
        assert self.cluster is not None
        self.cluster.after_test(test_name)
        self.is_after_test_ok = True
