import asyncio
import logging
from typing import AsyncIterator

from paravon.core.models.version import HLC
from paravon.core.models.request import PutData
from paravon.core.models.version import ValueVersion
from paravon.core.ports.conflict import ConflictResolver
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import BackendStorageFactory, BackendStorage, Storage
from paravon.core.storage.codec import KeyCodec


class VersionedStorage:
    DATASPACE = b"data"
    INDEXSPACE = b"index"
    METASPACE = b"meta"
    HLC_KEY = b"hlc"

    def __init__(
        self,
        hlc: HLC,
        backend: BackendStorage,
        serializer: Serializer,
        conflict_resolver: ConflictResolver,
    ) -> None:
        self._hlc = hlc
        self._backend = backend
        self._serializer = serializer
        self._conflict_resolver = conflict_resolver
        self._logger = logging.getLogger("core.storage.versioned")

    async def get(self, keyspace: bytes, key: bytes) -> ValueVersion | None:
        kv = await self._get_latest_data_key_val(keyspace, key)
        if kv is None or kv[1] == KeyCodec.TOMBSTONE:
            return None

        data_key, value = kv
        parsed = KeyCodec.parse_data_key(keyspace, data_key)
        if parsed is None:
            self._logger.error(f"Error parsing internal key: {data_key.hex()}")
            return None

        hlc_bytes, _ = parsed
        return self._build_version(value, HLC.decode(hlc_bytes))

    async def put(self, keyspace: bytes, key: bytes, value: bytes) -> ValueVersion:
        data = self._get_put_items(keyspace, key, value)
        await self._backend.put_many(data.items)
        return self._build_version(value, data.hlc)

    async def delete(self, keyspace: bytes, key: bytes) -> ValueVersion:
       return  await self.put(keyspace, key, KeyCodec.TOMBSTONE)

    async def apply(
        self,
        keyspace: bytes,
        key: bytes,
        version: ValueVersion
    ) -> ValueVersion:
        self._hlc = self._hlc.tick_on_receive(version.hlc)
        local = await self.get(keyspace, key)
        candidates = [v for v in (local, version) if v is not None]
        winner = self._conflict_resolver.resolve(candidates)

        if winner != local:
            hlc = winner.hlc
            hlc_bytes = hlc.encode()
            value = winner.value
            data_key = KeyCodec.data_key(keyspace, key, hlc_bytes)
            index_key = KeyCodec.index_key(keyspace, key, hlc_bytes)
            items = self._atomic_put_items(data_key, index_key, hlc, value)
            await self._backend.put_many(items)
            return winner

        return local

    async def iter(
        self,
        keyspace: bytes,
        hlc: HLC,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, ValueVersion]]:
        start = KeyCodec.index_prefix(keyspace, hlc.encode())

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
            c_hlc = HLC.decode(c_hlc_bytes)
            if c_hlc <= hlc:
                if c_hlc < hlc:
                    self._logger.error(
                        f"HLC ordering violation: index returned "
                        f"c_hlc={c_hlc} < watermark={hlc}"
                    )
                continue

            data_key = KeyCodec.data_key(keyspace, user_key, c_hlc_bytes)
            value = await self._backend.get(self.DATASPACE, data_key)
            version = ValueVersion(
                value=value,
                hlc=c_hlc,
                is_tombstone=value == KeyCodec.TOMBSTONE,
            )
            yield user_key, version

    async def get_last_hlc(self, keyspace: bytes) -> HLC:
        async for index_key, _ in self._backend.iter(
            keyspace=self.INDEXSPACE,
            prefix=keyspace,
            reverse=True,
            limit=1,
            batch_size=1
        ):
            parsed = KeyCodec.parse_index_key(keyspace, index_key)
            if parsed is None:
                continue

            hlc_bytes, _ = parsed
            return HLC.decode(hlc_bytes)

        return HLC.initial(node_id=self._hlc.node_id)

    async def close(self) -> None:
        await self._backend.close()

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

    @staticmethod
    def _build_version(value: bytes | None, hlc: HLC) -> ValueVersion:
        tombstone = value == KeyCodec.TOMBSTONE
        if tombstone:
            return ValueVersion.tombstone(hlc=hlc)

        return ValueVersion(
            value=value,
            hlc=hlc,
            is_tombstone=False,
        )

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
    ) -> PutData:
        """Not thread-safe"""
        self._hlc = hlc = self._hlc.tick_local()
        hlc_bytes = hlc.encode()
        data_key = KeyCodec.data_key(keyspace, key, hlc_bytes)
        index_key = KeyCodec.index_key(keyspace, key, hlc_bytes)
        items = self._atomic_put_items(data_key, index_key, hlc, value)
        return PutData(
            items=items,
            data_key=data_key,
            index_key=index_key,
            hlc=hlc
        )


class VersionedStorageFactory:
    def __init__(
        self,
        backend_factory: BackendStorageFactory,
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

    async def _get_hlc(self, backend: BackendStorage) -> HLC:
        hlc_bytes = await backend.get(
            VersionedStorage.METASPACE,
            VersionedStorage.HLC_KEY
        )
        if hlc_bytes is None:
            return HLC.initial(self._node_id)

        hlc_dict = self._serializer.deserialize(hlc_bytes)
        return HLC.from_dict(hlc_dict)
