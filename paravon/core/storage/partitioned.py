import asyncio
from typing import AsyncIterator

from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import Storage, StorageFactory


class PartitionedStorage(Storage):
    def __init__(
        self,
        storage_factory: StorageFactory,
        serializer: Serializer,
    ) -> None:
        self._storage_factory = storage_factory
        self._serializer = serializer
        self._backends: dict[str, Storage] = {}
        self._lock = asyncio.Lock()

    async def get(self, keyspace: bytes, key: bytes) -> bytes | None:
        backend = await self.select_backend(keyspace)
        return await backend.get(keyspace, key)

    async def put(self, keyspace: bytes, key: bytes, value: bytes) -> None:
        backend = await self.select_backend(keyspace)
        return await backend.put(keyspace, key, value)

    async def put_many(self, items: list[tuple[bytes, bytes, bytes]]) -> None:
        if not items:
            return

        keyspaces = {keyspace for keyspace, _, _ in items}
        if len(keyspaces) != 1:
            raise ValueError("put_many requires all items to share the same keyspace")

        keyspace = next(iter(keyspaces))
        backend = await self.select_backend(keyspace)

        await backend.put_many(items)

    async def delete(self, keyspace: bytes, key: bytes) -> None:
        backend = await self.select_backend(keyspace)
        return await backend.delete(keyspace, key)

    async def close(self) -> None:
        await self._storage_factory.close()

    async def iter(
        self,
        keyspace: bytes,
        prefix: bytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
        batch_size: int = 1024,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        backend = await self.select_backend(keyspace)
        async for key, value in backend.iter(
            keyspace=keyspace,
            prefix=prefix,
            start=start,
            limit=limit,
            reverse=reverse,
            batch_size=batch_size,
        ):
            yield key, value

    async def select_backend(self, keyspace: bytes) -> Storage:
        pid = int(keyspace.decode("ascii"), 16)
        env_index = pid // self._storage_factory.max_keyspaces
        return await self._storage_factory.get(str(env_index))
