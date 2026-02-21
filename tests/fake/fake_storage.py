import asyncio

from paravon.core.ports.storage import Storage, StorageFactory


class FakeStorage(Storage):
    """
    A simple in-memory storage implementation for testing.
    It mimics the async get/put/delete interface of the real Storage.
    """

    def __init__(self):
        self._data = {}
        self.get_calls = 0

    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        self.get_calls += 1
        return self._data.get((namespace, key))

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        self._data[(namespace, key)] = value

    async def delete(self, namespace: bytes, key: bytes) -> None:
        if (namespace, key) in self._data:
            del self._data[(namespace, key)]


class FakeStorageFactory:
    def __init__(self, max_dbs: int = 10) -> None:
        self._max_dbs = max_dbs
        self._backends: dict[str, FakeStorage] = {}
        self._lock = asyncio.Lock()

    @property
    def max_keyspaces(self) -> int:
        return self._max_dbs

    async def get(self, sid: str) -> Storage:
        async with self._lock:
            if sid not in self._backends:
                self._backends[sid] = FakeStorage()
            return self._backends[sid]

    async def close(self) -> None:
        async with self._lock:
            coros = [s.close() for s in self._backends.values()]
            await asyncio.gather(*coros)
            self._backends.clear()
