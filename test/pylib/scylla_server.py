import asyncio
import logging
import os
import pathlib
import re
import shutil
import time
import uuid

SCYLLA_CONF_TEMPLATE = """cluster_name: {cluster_name}
developer_mode: true
experimental: true
experimental-features: raft
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

skip_wait_for_gossip_to_settle: {skip_wait_for_gossip_to_settle}
ring_delay_ms: {ring_delay_ms}
"""

SCYLLA_CMDLINE_OPTIONS = [
    '--developer-mode', '1',
    '--collectd', '0',
    '-m', '1G',
    '--overprovisioned',
    '--max-networking-io-control-blocks', '100',
    '--unsafe-bypass-fsync', '1',
    '--kernel-page-cache', '1',
    '--flush-schema-tables-after-modification', 'false',
    '--auto-snapshot', '0',
    '--num-tokens', '16',
    # Significantly increase default timeouts to allow running tests
    # on a very slow setup (but without network losses). Note that these
    # are server-side timeouts: The client should also avoid timing out
    # its own requests - for this reason we increase the CQL driver's
    # client-side timeout in conftest.py.
    '--range-request-timeout-in-ms', '300000',
    '--read-request-timeout-in-ms', '300000',
    '--counter-write-request-timeout-in-ms', '300000',
    '--cas-contention-timeout-in-ms', '300000',
    '--truncate-request-timeout-in-ms', '300000',
    '--write-request-timeout-in-ms', '300000',
    '--request-timeout-in-ms', '300000',
    # Allow testing experimental features. Following issue #9467, we need
    # to add here specific experimental features as they are introduced.
    # Note that Alternator-specific experimental features are listed in
    # test/alternator/run.
    '--experimental-features=udf',
    '--enable-user-defined-functions', '1',
    # Set up authentication in order to allow testing this module
    # and other modules dependent on it: e.g. service levels
    '--authenticator', 'PasswordAuthenticator',
    '--strict-allow-filtering', 'true',
]


class ScyllaServer:
    def __init__(self, exe=None, vardir=None, hosts=None,
                 cmdline_options=None):
        self.exe = os.path.abspath(exe)
        self.vardir = vardir
        self.hosts = hosts
        self.cmdline_options = cmdline_options
        self.cfg = {
            "cluster_name": None,
            "host": None,
            "workdir": None,
            "seeds": None,
            "skip_wait_for_gossip_to_settle": 0,
            "ring_delay_ms": 0,
            "smp": 2,
        }
        self.cmd = None

    async def start(self):
        await self.install()

        logging.info("starting server at host %s...", self.cfg["host"])

        await self.do_start()

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
        self.find_scylla_executable()


        # Scylla assumes all instances of a cluster use the same port,
        # so each instance needs an own IP address.
        self.cfg["cluster_name"] = str(uuid.uuid1())
        self.cfg["host"] = await self.hosts.lease_host()
        self.cfg["workdir"] = os.path.join(self.vardir, self.cfg["host"])
        self.cfg["seeds"] = self.cfg["host"]

        logging.info("installing Scylla server in %s...", self.cfg["workdir"])

        self.log_file_name = os.path.join(self.vardir, self.cfg["host"]+".log")

        # SCYLLA_CONF env variable would be better called SCYLLA_CONF_DIR
        # variable, the configuration file name is assumed to be scylla.yaml
        self.config_file_name = os.path.join(self.cfg["workdir"], "conf/scylla.yaml")

        # Use tmpfs to speed up scylla start up
        self.tmpdir = os.getenv('TMPDIR', '/tmp')
        self.tmpdir = os.path.join(self.tmpdir, 'scylla-test-'+self.cfg["host"])

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

    def find_log_file_pattern(self, fil, pattern):
        pattern_r_e = re.compile(pattern)
        for line in fil.readlines():
            if pattern_r_e.match(line):
                return True
        return False

    async def do_start(self):
        START_TIMEOUT = 300     # seconds

        args = ['-c', str(self.cfg["smp"]), self.exe, "--smp={}".format(self.cfg["smp"])]
        args += SCYLLA_CMDLINE_OPTIONS
        # Add suite-specific command line options
        args += self.cmdline_options
        self.cmd = await asyncio.create_subprocess_exec(
            "taskset",
            *args,
            cwd=self.cfg["workdir"],
            stderr=self.log_file,
            stdout=self.log_file,
            env={"SCYLLA_HOME": self.cfg["workdir"]},
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

                if self.find_log_file_pattern(log_file, ".*Scylla.*initialization completed"):
                    return

                # Sleep 10 milliseconds and retry
                await asyncio.sleep(0.1)

        raise RuntimeError("failed to start server {}, check server log at {}".format(
            self.host, self.log_file_name))

    async def stop(self):
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
            # # 3 seconds is enough for a good database to die gracefully:
            # # send SIGKILL if SIGTERM doesn't reach its target
            # timer := time.after_func(3*time.Second, func()
            #         syscall.Kill(self.cmd.Process.Pid, syscall.SIGKILL)
            # )
            # timer.Stop()
            # self.cmd.Process.Wait()
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("stopped server at host %s", host)
            self.cmd = None

    async def uninstall(self):
        if not self.cfg["host"]:
            return
        logging.info("Uninstalling server at %s", self.cfg["workdir"])

#        shutil.rmtree(self.cfg["workdir"])
#        os.remove(self.log_file_name)

        await self.hosts.release_host(self.cfg["host"])
        self.cfg["host"] = None
