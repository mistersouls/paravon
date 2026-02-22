import asyncio
from typing import AsyncIterator

from paravon.core.helpers.hlc import HLC
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import Storage, StorageFactory
from paravon.core.storage.codec import KeyCodec


class VersionedStorage:
    DATASPACE = b"data"
    INDEXSPACE = b"index"
    METASPACE = b"meta"

    HLC_KEY = b"hlc"
    HLC_LEN_SIZE = 2
    USER_LEN_SIZE = 2
    SENTINEL = b""
    TOMBSTONE = b""

    def __init__(
        self,
        backend: Storage,
        hlc: HLC,
        serializer: Serializer
    ) -> None:
        self._backend = backend
        self._hlc = hlc
        self._serializer = serializer

    async def get(self, keyspace: bytes, key: bytes) -> bytes | None:
        prefix = KeyCodec.data_prefix(keyspace, key)
        async for data_key, value in self._backend.iter(
            keyspace=self.DATASPACE,
            prefix=prefix,
            reverse=True,
            limit=1,
            batch_size=1
        ):
            if value == self.TOMBSTONE:
                return None
            return value

        return None

    async def put(self, keyspace: bytes, key: bytes, value: bytes) -> None:
        items = self._get_put_items(keyspace, key, value)
        await self._backend.put_many(items)

    async def put_many(self, items: list[tuple[bytes, bytes, bytes]]) -> None:
        ops = []

        for keyspace, user_key, value in items:
            ops.extend(self._get_put_items(keyspace, user_key, value))

        await self._backend.put_many(ops)

    async def delete(self, keyspace: bytes, key: bytes) -> None:
        await self.put(keyspace, key, self.TOMBSTONE)

    async def close(self) -> None:
        await self._backend.close()

    async def iter(
        self,
        keyspace: bytes,
        prefix: bytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
        batch_size: int = 1024,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        index_prefix = keyspace if prefix is None else prefix
        async for index_key, _ in self._backend.iter(
            keyspace=self.INDEXSPACE,
            prefix=index_prefix,
            start=start,
            reverse=reverse,
            batch_size=batch_size,
            limit=limit,
        ):
            parsed = KeyCodec.parse_index_key(keyspace, index_key)
            if parsed is None:
                continue    # truncated or corrupted

            hlc_bytes, user_key = parsed
            data_key = KeyCodec.data_key(keyspace, user_key, hlc_bytes)
            value = await self._backend.get(self.DATASPACE, data_key)
            yield user_key, value

    def _get_put_items(
        self,
        keyspace: bytes,
        key: bytes,
        value: bytes
    ) -> list[tuple[bytes, bytes, bytes]]:
        """Not thread-safe"""
        self._hlc = hlc = self._hlc.tick_local()
        hlc_bytes = hlc.encode()

        data_key = KeyCodec.data_key(keyspace, key, hlc_bytes)
        index_key = KeyCodec.index_key(keyspace, key, hlc_bytes)

        hlc_meta = self._serializer.serialize(hlc.to_dict())

        items = [
            (self.DATASPACE, data_key, value),
            (self.INDEXSPACE, index_key, self.SENTINEL),
            (self.METASPACE, self.HLC_KEY, hlc_meta)
        ]

        return items


class VersionedStorageFactory:
    def __init__(
        self,
        backend_factory: StorageFactory,
        serializer: Serializer,
        node_id: str
    ) -> None:
        self._backend_factory = backend_factory
        self._serializer = serializer
        self._node_id = node_id

        self._versioned: dict[str, Storage] = {}
        self._lock = asyncio.Lock()

    @property
    def max_keyspaces(self) -> int:
        return self._backend_factory.max_keyspaces

    async def get(self, sid: str) -> Storage:
        async with self._lock:
            if sid not in self._versioned:
                backend = await self._backend_factory.get(sid)
                hlc = await self._get_hlc(backend)
                self._versioned[sid] = VersionedStorage(
                    backend=backend,
                    hlc=hlc,
                    serializer=self._serializer
                )
            return self._versioned[sid]

    async def close(self) -> None:
        async with self._lock:
            coros = [b.close() for b in self._versioned.values()]
            await asyncio.gather(*coros, return_exceptions=True)
            self._versioned.clear()

    async def _get_hlc(self, backend: Storage) -> HLC:
        hlc_bytes = await backend.get(
            VersionedStorage.METASPACE,
            VersionedStorage.HLC_KEY
        )
        if hlc_bytes is None:
            return HLC.initial(self._node_id)

        hlc_dict = self._serializer.deserialize(hlc_bytes)
        return HLC.from_dict(hlc_dict)
