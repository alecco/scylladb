#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import logging
import os
import pathlib
import re
import shutil
import time
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

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

authenticator: PasswordAuthenticator
strict_allow_filtering: true
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
    # Regular expression to scan the log
    STARTUP_MSG_RE = re.compile(".*Scylla.*initialization completed")

    def __init__(self, exe=None, vardir=None, host_registry=None,
                 cluster_name=None, seed=None, cmdline_options=None):
        self.exe = os.path.abspath(exe)
        self.vardir = vardir
        self.host_registry = host_registry
        self.cmdline_options = cmdline_options
        self.cfg = {
            "cluster_name": cluster_name,
            "host": None,
            "seeds": seed,
            "workdir": None,
        }
        self.cmd = None

        async def stop_server():
            if self.is_running:
                await self.stop()

        async def uninstall_server():
            await self.uninstall()

        self.stop_artifact = stop_server
        self.uninstall_artifact = uninstall_server

    async def install_and_start(self):
        await self.install()

        logging.info("starting server at host %s...", self.cfg["host"])

        await self.start()

        logging.info("started server at host %s, pid %d", self.cfg["host"], self.cmd.pid)

    @property
    def is_running(self):
        return self.cmd is not None

    @property
    def host(self):
        return self.cfg["host"]

    def find_scylla_executable(self):
        if not os.access(self.exe, os.X_OK):
            raise RuntimeError("{} is not executable", self.exe)

    async def install(self):
        """Create a working directory with all subdirectories, initialize
        a configuration file."""

        self.find_scylla_executable()

        # Scylla assumes all instances of a cluster use the same port,
        # so each instance needs an own IP address.
        self.cfg["host"] = await self.host_registry.lease_host()
        self.cfg["seeds"] = self.cfg.get("seeds") or self.cfg["host"]
        # Use the last part in host IP 127.151.3.27 -> 27
        self.shortname = "scylla-" + self.cfg["host"].split(".")[-1]
        self.cfg["workdir"] = os.path.join(self.vardir, self.shortname)

        logging.info("installing Scylla server in %s...", self.cfg["workdir"])

        self.log_file_name = os.path.join(self.vardir, self.shortname+".log")

        self.config_file_name = os.path.join(self.cfg["workdir"], "conf/scylla.yaml")

        # Use tmpdir (likely tmpfs) to speed up scylla start up, but create a
        # link to it to keep everything in one place under testlog/
        self.tmpdir = os.getenv('TMPDIR', '/tmp')
        self.tmpdir = os.path.join(self.tmpdir, 'scylla-'+self.cfg["host"])

        # Cleanup any remains of the previously running server in this path
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        if os.path.lexists(self.cfg["workdir"]):
            os.unlink(self.cfg["workdir"])

        pathlib.Path(self.tmpdir).mkdir(parents=True, exist_ok=True)
        os.symlink(self.tmpdir, self.cfg["workdir"])
        pathlib.Path(os.path.dirname(self.config_file_name)).mkdir(parents=True, exist_ok=True)
        # Create a configuration file.
        with open(self.config_file_name, 'w') as config_file:
            config_file.write(SCYLLA_CONF_TEMPLATE.format(**self.cfg))

        self.log_file = open(self.log_file_name, "wb")

    def find_log_file_pattern(self, fil, pattern_re):
        for line in fil.readlines():
            if pattern_re.match(line):
                return True
        return False

    async def start(self):
        """Start an installed server. May be used for restarts."""
        START_TIMEOUT = 300     # seconds

        # Add suite-specific command line options
        scylla_args = SCYLLA_CMDLINE_OPTIONS + self.cmdline_options
        self.cmd = await asyncio.create_subprocess_exec(
            self.exe,
            *scylla_args,
            cwd=self.cfg["workdir"],
            stderr=self.log_file,
            stdout=self.log_file,
            # pass empty env to make user user's SCYLLA_HOME has no impact
            env={},
            preexec_fn=os.setsid,
        )

        self.start_time = time.time()

        with open(self.log_file_name, 'r') as log_file:
            while time.time() < self.start_time + START_TIMEOUT:
                if self.cmd.returncode:
                    logging.error("failed to start server at host %s", self.cfg["host"])
                    logging.error("last line of {}:".format(self.log_file_name))
                    log_file.seek(0, 0)
                    logging.error(log_file.readlines()[-1].rstrip())
                    raise RuntimeError("""Failed to start server at host {}.
Check the log files:
{}
{}""".format(
                        self.cfg["host"],
                        logging.getLogger().handlers[0].baseFilename,
                        self.log_file_name))

                if self.find_log_file_pattern(log_file, ScyllaServer.STARTUP_MSG_RE):
                    return

                # Sleep 10 milliseconds and retry
                await asyncio.sleep(0.1)
                if self.cfg["seeds"] != self.cfg["host"]:
                    await self.force_schema_migration()

        raise RuntimeError("failed to start server {}, check server log at {}".format(
            self.host, self.log_file_name))

    async def force_schema_migration(self):
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        with Cluster(contact_points=[self.cfg["seeds"]], auth_provider=auth) as cluster:
            with cluster.connect() as session:
                session.execute("CREATE KEYSPACE k WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                session.execute("DROP KEYSPACE k")

    async def stop(self):
        """Stop a running server. No-op if not running. Uses SIGKILL to
        stop, so is not graceful. Waits for the process to exit before return."""
        # Preserve for logging
        host = self.cfg["host"]
        logging.info("stopping server at host %s", host)
        if not self.cmd:
            return

        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("stopped server at host %s", host)
            self.cmd = None

    async def uninstall(self):
        """Clear all files left from a stopped server, including the
        data files and log files."""

        if not self.cfg["host"]:
            return
        logging.info("Uninstalling server at %s", self.cfg["workdir"])

        shutil.rmtree(self.tmpdir)
        if os.path.lexists(self.cfg["workdir"]):
            os.unlink(self.cfg["workdir"])
        os.remove(self.log_file_name)

        await self.host_registry.release_host(self.cfg["host"])
        self.cfg["host"] = None
