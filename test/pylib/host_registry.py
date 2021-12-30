from test.pylib.pool import Pool


class HostRegistry:
    """A Scylla servers needs a unique IP address and working directory
    which we need to manage and share across many running tests. Store
    all shared external resources within this class to make sure
    nothing is leaked by the harness."""

    def __init__(self):
        self.next_host_id = 1

        async def create_host():
            self.next_host_id += 1
            return "127.0.0.{}".format(self.next_host_id)

        self.pool = Pool(250, create_host)

    async def lease_host(self):
        return await self.pool.get()

    async def release_host(self, host):
        return await self.pool.put(host)

    def host(self):
        return self.pool.instance()
