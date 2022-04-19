import asyncio
from typing import Generic, Callable, Awaitable, TypeVar, AsyncContextManager

T = TypeVar('T')


class Pool(Generic[T]):
    """Asynchronous object pool.
    You need a pool of up to N objects, but objects should be created
    on demand, so that if you use less, you don't create anything upfront.
    If there is no object in the pool and all N objects are in use, you want
    to wait until one of the object is returned to the pool. Expects a
    builder async function to build a new object.

    Usage example:
    async def start_server():
        return Server()
    pool = Pool(4, start_server)
    ...
    async with pool.instance() as server:
        await run_test(test, server)
    """

    def __init__(self, size: int, build: Callable[[], Awaitable[T]]):
        import sys
        print(f"XXX Pool() size {size}", file=sys.stderr)  # XXX
        assert(size >= 0)
        self.pool: asyncio.Queue[T] = asyncio.Queue(size)
        self.build = build
        self.total = 0

    async def get(self) -> T:
        print(f"XXX Pool.get() empty? {self.pool.empty()} total {self.total} pool max {self.pool.maxsize}")  # XXX
        if self.pool.empty() and self.total < self.pool.maxsize:
            # Increment the total first to avoid a race
            # during self.build()
            self.total += 1
            try:
                print(f"XXX Pool.get() self.pool.put")  # XXX
                await self.pool.put(await self.build())
            except:     # noqa: E722
                print(f"XXX Pool.get() total -= 1")  # XXX
                self.total -= 1
                raise

        return await self.pool.get()

    async def put(self, obj: T):
        print("XXX Pool.put()")  # XXX
        await self.pool.put(obj)

    def instance(self) -> AsyncContextManager[T]:
        print(f"XXX Pool.instance() type {type(self.pool)}")  # XXX
        class Instance:
            def __init__(self, pool):
                print("XXX Pool.Instance()")  # XXX
                self.pool = pool

            async def __aenter__(self):
                self.obj = await self.pool.get()
                print(f"XXX Pool.Instance.__aenter__() type {type(self.pool)} ret type {type(self.obj)}")  # XXX
                return self.obj

            async def __aexit__(self, exc_type, exc, obj):
                print("XXX Pool.Instance.__aexit__()")  # XXX
                if self.obj:
                    await self.pool.put(self.obj)
                    self.obj = None

        return Instance(self)
