import asyncio
from typing import AsyncIterator

from paravon.core.helpers.hlc import HLC, ConflictResolver
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import Storage, StorageFactory
from paravon.core.storage.codec import KeyCodec


class VersionedStorage:
    DATASPACE = b"data"
    INDEXSPACE = b"index"
    METASPACE = b"meta"
    HLC_KEY = b"hlc"

    def __init__(
        self,
        backend: Storage,
        hlc: HLC,
        serializer: Serializer,
        conflict_resolver: ConflictResolver,
    ) -> None:
        self._backend = backend
        self._hlc = hlc
        self._serializer = serializer
        self._conflict_resolver = conflict_resolver

    async def get(self, keyspace: bytes, key: bytes) -> bytes | None:
        kv = await self._get_latest_data_key_val(keyspace, key)
        if kv is not None and kv[1]:
            return kv[1]
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
        await self.put(keyspace, key, KeyCodec.TOMBSTONE)

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

    async def apply_remote(
        self,
        keyspace: bytes,
        index_key: bytes,
        value: bytes
    ) -> HLC | None:
        parsed = KeyCodec.parse_index_key(keyspace, index_key)
        if parsed is None:
            return None

        r_hlc_bytes, user_key = parsed
        r_hlc = HLC.decode(r_hlc_bytes)
        self._hlc = self._hlc.tick_on_receive(r_hlc)

        local_hlc = None
        kv = await self._get_latest_data_key_val(keyspace, user_key)
        if kv is not None:
            local_hlc, _ = KeyCodec.parse_data_key(keyspace, kv[0])

        candidates = [v for v in (local_hlc, r_hlc) if v is not None]
        winner = self._conflict_resolver.resolve(candidates)

        if winner != local_hlc:
            r_data_key = KeyCodec.data_key(keyspace, user_key, r_hlc_bytes)
            items = self._atomic_put_items(r_data_key, index_key, r_hlc, value)
            await self._backend.put_many(items)
            return winner

        return local_hlc

    async def iter_from_hlc(
        self,
        keyspace: bytes,
        hlc: bytes,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, bytes, bytes]]:
        start = KeyCodec.index_prefix(keyspace, hlc)

        async for index_key, _ in self._backend.iter(
            keyspace=self.INDEXSPACE,
            prefix=keyspace,
            start=start,
            batch_size=batch_size
        ):
            parsed = KeyCodec.parse_index_key(keyspace, index_key)
            if parsed is None:
                continue

            c_hlc_bytes, user_key = parsed
            data_key = KeyCodec.data_key(keyspace, user_key, c_hlc_bytes)
            value = await self._backend.get(self.DATASPACE, data_key)

            yield index_key, user_key, value

    def _atomic_put_items(
        self,
        data_key: bytes,
        index_key: bytes,
        hlc: HLC,
        value: bytes
    ) -> list[tuple[bytes, bytes, bytes]]:
        hlc_meta = self._serializer.serialize(hlc.to_dict())
        items = [
            (self.DATASPACE, data_key, value),
            (self.INDEXSPACE, index_key, KeyCodec.SENTINEL),
            (self.METASPACE, VersionedStorage.HLC_KEY, hlc_meta)
        ]
        return items

    async def _get_latest_data_key_val(
        self,
        keyspace: bytes,
        key: bytes
    ) -> tuple[bytes, bytes] | None:
        prefix = KeyCodec.data_prefix(keyspace, key)
        async for data_key, value in self._backend.iter(
            keyspace=self.DATASPACE,
            prefix=prefix,
            reverse=True,
            limit=1,
            batch_size=1
        ):
            return data_key, value

        return None

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
        return self._atomic_put_items(data_key, index_key, hlc, value)


class VersionedStorageFactory:
    def __init__(
        self,
        backend_factory: StorageFactory,
        serializer: Serializer,
        conflict_resolver: ConflictResolver,
        node_id: str
    ) -> None:
        self._backend_factory = backend_factory
        self._serializer = serializer
        self._node_id = node_id
        self._conflict_resolver = conflict_resolver

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
                    serializer=self._serializer,
                    conflict_resolver=self._conflict_resolver
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
