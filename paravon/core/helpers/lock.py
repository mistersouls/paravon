import asyncio
from contextlib import asynccontextmanager


class RWLock:
    def __init__(self):
        self._readers = 0
        self._readers_lock = asyncio.Lock()
        self._writer_lock = asyncio.Lock()

    @property
    def rlock(self):
        return self._readers_lock

    @property
    def wlock(self):
        return self._writer_lock

    @asynccontextmanager
    async def read(self):
        await self._acquire_read()
        try:
            yield
        finally:
            await self._release_read()

    @asynccontextmanager
    async def write(self):
        await self._writer_lock.acquire()
        try:
            yield
        finally:
            self._writer_lock.release()

    async def _acquire_read(self):
        async with self._readers_lock:
            self._readers += 1
            if self._readers == 1:
                # first reader blocks writers
                await self._writer_lock.acquire()

    async def _release_read(self):
        async with self._readers_lock:
            self._readers -= 1
            if self._readers == 0:
                # last reader releases writer lock
                self._writer_lock.release()
