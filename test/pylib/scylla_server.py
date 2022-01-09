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

seed_provider:
    - class_name: org.apache.cassandra.locator.simple_seed_provider
      parameters:
          - seeds: {seeds}

skip_wait_for_gossip_to_settle: {skip_wait_for_gossip_to_settle}
ring_delay_ms: {ring_delay_ms}
"""

class ScyllaServer:

    def __init__(self, exe = None, tmpdir = None, hosts = None):
        self.exe = os.path.abspath(exe)
        self.tmpdir = tmpdir
        self.hosts = hosts
        self.cfg = {
            "cluster_name": None,
            "host": None,
            "workdir": None,
            "seeds": None,
            "skip_wait_for_gossip_to_settle": True,
            "ring_delay_ms": 0,
            "smp": 2,
        }

    async def start(self):
        await self.install()

        logging.info("starting server at host %s...", self.cfg["host"])

        await self.do_start()

        logging.info("started server %s", self.cfg["host"])

    @property
    def is_running(self):
        return self.cmd is not None;


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
        self.cfg["workdir"] = os.path.join(self.tmpdir, self.cfg["host"])
        self.cfg["seeds"] = self.cfg["host"]

        logging.info("installing Scylla server in %s...", self.cfg["workdir"])

        self.log_file_name = os.path.join(self.tmpdir, self.cfg["host"]+".log")

        # SCYLLA_CONF env variable would be better called SCYLLA_CONF_DIR
        # variable, the configuration file name is assumed to be scylla.yaml
        self.config_file_name = os.path.join(self.cfg["workdir"], "scylla.yaml")

        # Cleanup any remains of the previously running server in this path
        shutil.rmtree(self.cfg["workdir"])
        pathlib.Path(self.cfg["workdir"]).mkdir(parents=True, exist_ok=True)
        # Create a configuration file. Unfortunately, Scylla can't start without
        # one. Since we have to create a configuration file, let's avoid
        # command line options.
        with open(self.config_file_name, 'w') as config_file:
            config_file.write(SCYLLA_CONF_TEMPLATE.format(**self.cfg))

        # Do not confuse Scylla binary if we derived this from the parent process
        os.unsetenv("SCYLLA_HOME")

        self.log_file = open(self.log_file_name, "wb")

    def find_log_file_pattern(self, fil, pattern):
        pattern_r_e = re.compile(pattern)
        while True:
            line = fil.readline()
            if not line:
                return False 
            elif pattern_r_e.match(line):
                return True


    async def do_start(self):
        START_TIMEOUT = 300 # seconds

        print("do_start()")

        # "--experimental-features=raft", "--default-log-level=trace"
        args = ['-c', str(self.cfg["smp"]), self.exe, "--smp={}".format(self.cfg["smp"])]
        self.cmd = await asyncio.create_subprocess_exec(
            "taskset",
            *args,
            cwd=self.cfg["workdir"],
            stderr=self.log_file,
            stdout=self.log_file,
            env={"SCYLLA_CONF": self.cfg["workdir"]},
            preexec_fn=os.setsid,
        )

        self.start_time = time.time()

        with open(self.log_file_name, 'r') as log_file:
            while time.time() < self.start_time + START_TIMEOUT:
               print("checking the log file...")
               if self.find_log_file_pattern(log_file, ".*Scylla.*initialization completed"):
                   return
               # Sleep 10 milliseconds and retry
               await asyncio.sleep(0.05)

        raise RuntimeError("failed to start server {}, check server log at {}".format(
            self.host, self.log_file_name))


    async def stop(self):
        logging.info("Stopping server at host %s pid ?", self.cfg["host"])
        if not self.cmd:
            return

        try:
            logging.info("Stopping server %d", self.cmd.pid)
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
#        # 3 seconds is enough for a good database to die gracefully:
#        # send SIGKILL if SIGTERM doesn't reach its target
#        timer := time.after_func(3*time.Second, func()
#                syscall.Kill(self.cmd.Process.Pid, syscall.SIGKILL)
#        )
#        timer.Stop()
#        self.cmd.Process.Wait()
            await self.cmd.wait()
        finally:
            logging.info("Stopped server %d", self.cmd.pid)
            self.cmd = None

    async def uninstall(self):
        if not self.cfg["host"]:
            return
        logging.info("Uninstalling server at %s", self.cfg["workdir"])

#        shutil.rmtree(self.cfg["workdir"])
#        os.remove(self.log_file_name)

        await self.hosts.release_host(self.cfg["host"])
        self.cfg["host"] = None
