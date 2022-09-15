import asyncio
from typing import Generic, Callable, Awaitable, TypeVar, AsyncContextManager, Final

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

    obj = await pool.get()
    try:
        ... do stuff with obj ...
    finally:
        await pool.put(obj)

    If the object is considered no longer usable by other users of the pool
    you can 'steal' it, which frees up space in the pool.
    obj2 = await.pool.get()
    try:
        ... do stuff with obj that makes it nonreusable ...
    finally:
        await pool.steal()
    """
    def __init__(self, max_size: int, build: Callable[[], Awaitable[T]]):
        assert(max_size >= 0)
        self.max_size: Final[int] = max_size
        self.build: Final[Callable[[], Awaitable[T]]] = build
        self.cond: Final[asyncio.Condition] = asyncio.Condition()
        self.pool: list[T] = []
        self.total: int = 0 # len(self.pool) + leased objects

    async def get(self) -> T:
        """Borrow an object from the pool."""
        async with self.cond:
            await self.cond.wait_for(lambda: self.pool or self.total < self.max_size)
            if self.pool:
                return self.pool.pop()

            # No object in pool, but total < max_size so we can construct one
            self.total += 1

        try:
            obj = await self.build()
        except:
            async with self.cond:
                self.total -= 1
                self.cond.notify()
            raise
        return obj

    async def steal(self) -> None:
        """Take ownership of a previously borrowed object.
           Frees up space in the pool.
        """
        async with self.cond:
            self.total -= 1
            self.cond.notify()

    async def put(self, obj: T):
        """Return a previously borrowed object to the pool."""
        async with self.cond:
            self.pool.append(obj)
            self.cond.notify()
