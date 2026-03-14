import asyncio
from typing import AsyncIterator

from paravon.core.models.version import ValueVersion, HLC
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import StorageFactory, Storage


class PartitionedStorage:
    def __init__(
        self,
        storage_factory: StorageFactory,
        serializer: Serializer,
    ) -> None:
        self._storage_factory = storage_factory
        self._serializer = serializer
        self._backends: dict[str, Storage] = {}
        self._lock = asyncio.Lock()

    async def get(self, keyspace: bytes, key: bytes) -> ValueVersion | None:
        backend = await self._select_backend(keyspace)
        return await backend.get(keyspace, key)

    async def put(self, keyspace: bytes, key: bytes, value: bytes) -> ValueVersion:
        backend = await self._select_backend(keyspace)
        return await backend.put(keyspace, key, value)

    async def delete(self, keyspace: bytes, key: bytes) -> ValueVersion:
        backend = await self._select_backend(keyspace)
        return await backend.delete(keyspace, key)

    async def apply(
        self,
        keyspace: bytes,
        key: bytes,
        version: ValueVersion
    ) -> ValueVersion:
        backend = await self._select_backend(keyspace)
        return await backend.apply(keyspace, key, version)

    async def iter(
        self,
        keyspace: bytes,
        hlc: HLC,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, ValueVersion]]:
        backend = await self._select_backend(keyspace)
        async for key, version in backend.iter(keyspace, hlc, batch_size):
            yield key, version

    async def close(self) -> None:
        await self._storage_factory.close()

    async def _select_backend(self, keyspace: bytes) -> Storage:
        pid = int(keyspace.decode("ascii"), 16)
        env_index = pid // self._storage_factory.max_keyspaces
        return await self._storage_factory.get(str(env_index))
