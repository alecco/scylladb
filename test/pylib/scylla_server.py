import logging 
import os
import pathlib
import shutil
import uuid
import asyncio

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
        self._is_running = False
        self.cfg = {
            "cluster_name": None,
            "host": None,
            "workdir": None,
            "seeds": None,
            "skip_wait_for_gossip_to_settle": True,
            "ring_delay_ms": 0,
            "smp": 2,
        }
#       log_file_name    string
#       config_file_name string
#       cmd            *exec.cmd
#       log_file        *os.file
#
#
#    def mode_name():
#        return "single"
#
    async def start(self):
        await self.install()

        logging.info("starting server at host %s...", self.cfg["host"])

        await self.do_start()

        logging.info("started server %s", self.cfg["host"])

    @property
    def is_running(self):
        return self._is_running;

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

        pathlib.Path(self.cfg["workdir"]).mkdir(parents=True, exist_ok=True)
        # Create a configuration file. Unfortunately, Scylla can't start without
        # one. Since we have to create a configuration file, let's avoid
        # command line options.
        with open(self.config_file_name, 'w') as config_file:
            config_file.write(SCYLLA_CONF_TEMPLATE.format(**self.cfg))

        # Do not confuse Scylla binary if we derived this from the parent process
        os.unsetenv("SCYLLA_HOME")

        self.log_file = open(self.log_file_name, "wb")

#
#        var log_file *os.File
#        if log_file, err = os.open_file(self.log_file_name,
#                os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644); err != nil
#
#                return err
#
#        # Open another file descriptor to ensure its position is not advanced
#        # by writes
#        if self.log_file, err = os.Open(self.log_file_name); err != nil
#                return err
#
#        cmd := exec.Command(self.exe, fmt.Sprintf("--smp=%d", self.cfg.SMP), "--experimental-features=raft", "--default-log-level=trace")
#        cmd.Dir = self.cfg.Dir
#        cmd.Env = append(cmd.Env, fmt.Sprintf("SCYLLA_CONF=%s", self.cfg.Dir))
#        cmd.Stdout = log_file
#        cmd.Stderr = log_file
#
#        self.cmd = cmd
#
#
#    def find_log_file_pattern(file, pattern):
#        pattern_r_e = regexp.must_compile(pattern)
#        scanner = bufio.new_scanner(file)
#        for scanner.Scan():
#            if pattern_r_e.Match(scanner.Bytes()):
#                return true
#
#        return false
#
#
    async def do_start(self):
#        START_TIMEOUT = 300 * time.Second

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

#        start = time.Now()
#        for _ = range time.Tick(time.Millisecond * 10):
#            if find_log_file_pattern(self.log_file, "Scylla.*initialization completed")
#                    break
#
#            if time.Now().Sub(start) > START_TIMEOUT
#                    return merry.Errorf("failed to start self %s on lane %s, check server log at %s",
#                            self.cfg.URI, lane.id, palette.Path(self.log_file_name))
#

    async def stop(self):
        logging.info("Stopping server at host %s pid ?", self.cfg["host"])
        logging.info("Stopped server at host %s", self.cfg["host"])
        if self.cmd:
            self.cmd.kill()
            await self.cmd.wait()
            self.cmd = None

    async def uninstall(self):
        if not self.cfg["host"]:
            return
        logging.info("Uninstalling server at %s", self.cfg["workdir"])

#        shutil.rmtree(self.cfg["workdir"])
#        os.remove(self.log_file_name)

        await self.hosts.release_host(self.cfg["host"])
        self.cfg["host"] = None

#
#class ScyllaServerUninstallArtefact:
#
#    def __init__(server, lane):
#        self.server = server 
#        self.lane = lane
#
#
#    def remove():
#        os.remove_all(a.server.cfg.Dir)
#        os.Remove(a.server.log_file_name)
#
#
#class ScyllaServerStopArtefact:
#
#    def __init__(server, lane):
#        self.cmd = cmd
#        self.lane = lane
#
#    def remove():
#        logging.info("Stopping server %d", a.cmd.Process.Pid)
#        self.cmd.Process.Kill()
#        # 3 seconds is enough for a good database to die gracefully:
#        # send SIGKILL if SIGTERM doesn't reach its target
#        timer := time.after_func(3*time.Second, func()
#                syscall.Kill(self.cmd.Process.Pid, syscall.SIGKILL)
#        )
#        timer.Stop()
#        self.cmd.Process.Wait()
#        logging.info("Stopped server %d", a.cmd.Process.Pid)
#
#
