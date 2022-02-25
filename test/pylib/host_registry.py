#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import errno
import fcntl
import os
import random
from test.pylib.pool import Pool
from typing import NewType, Awaitable

Host = NewType('Host', str)


class HostRegistry:
    """A Scylla servers needs a unique IP address and working directory
    which we need to manage and share across many running tests. Store
    all shared external resources within this class to make sure
    nothing is leaked by the harness."""

    def __init__(self) -> None:

        # Imagine multiple instances of test.py run concurrently.
        # Each will be trying to start and stop Scylla servers.
        # If different runs share the same Scylla IP pool, Scyllas may
        # fail to bind CQL port, and tests will fail. So let's
        # give each run its own class B network in 127.*.*.* range.
        # Each scylla server will get an IP in this network.
        #
        # Why not simply give each Scylla a unique IP direved from its own
        # pid? The pid changes between restarts, and harness does start and
        # stops Scyllas.
        #
        # To create a subnet let's randomly generate a number and put
        # a file with this name into /tmp. If the file doesn't exist,
        # the subnet is free. If it already exists and is locked,
        # there is another process running with this subnet and we
        # should try again with another subnet. If the file isn't
        # locked, it remains from some previous invocation and can be
        # locked and reused.
        while True:
            # Avoid 127.0.*.* since CCM (a different test framework)
            # assumes it will be available for it to run Scylla
            # instances. 127.255.255.255 is also illegal.
            self.subnet = "127.{}.{}".format(random.randrange(1, 254),
                                             random.randrange(0, 255))
            self.lock_filename = os.path.join(os.getenv('TMPDIR', '/tmp'), "scylla-" + self.subnet)
            self.lock_file = open(self.lock_filename, "w")
            try:
                fcntl.lockf(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as e:
                if e.errno != errno.EACCES and e.errno != errno.EAGAIN:
                    raise
                self.lock_file.close()
                os.unlink(self.lock_filename)

            self.lock_file.close()

        self.subnet += ".{}"
        self.next_host_id = 0

        async def create_host():
            self.next_host_id += 1
            return self.subnet.format(self.next_host_id)

        self.pool = Pool(253, create_host)

    async def lease_host(self) -> Awaitable[Host]:
        return await self.pool.get()

    async def release_host(self, host: Host) -> None:
        return await self.pool.put(host)

    async def cleanup(self) -> None:
        if self.lock_filename:
            os.unlink(self.lock_filename)
            self.lock_filename = ""
