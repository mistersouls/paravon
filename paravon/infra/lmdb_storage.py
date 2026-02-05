import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import lmdb

from paravon.core.ports.storage import Storage


class LMDBStorage:
    """
    Concrete implementation of the Storage protocol backed by an LMDB
    environment. Each namespace is mapped to a dedicated LMDB database
    (DBI), allowing the caller to isolate logical datasets while sharing
    the same underlying environment.

    The class exposes an asynchronous interface by delegating all LMDB
    operations to a ThreadPoolExecutor. LMDB is a fully synchronous
    library, and offloading its operations prevents blocking the event
    loop while preserving LMDB's performance characteristics.

    The implementation provides simple get/put/delete semantics without
    any transactional batching or multi-operation atomicity. Each call
    opens a short-lived LMDB transaction. Stronger guarantees may be
    provided by LMDB itself depending on the configuration (sync,
    writemap, durability settings), but callers must not rely on
    anything beyond the documented behavior of the Storage protocol.
    """
    def __init__(
        self,
        path: str,
        map_size: int = 1 << 30,
        max_workers: int = 1,
        max_dbs: int = 256,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = False
    ) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._env = lmdb.open(
            path,
            map_size=map_size,
            max_dbs=max_dbs,
            lock=True,
            writemap=writemap,
            sync=sync,
            readahead=readahead,
        )
        self._dbis: dict[bytes, object] = {}

    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get,
            namespace,
            key,
        )

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_put,
            namespace,
            key,
            value
        )

    async def delete(self, namespace: bytes, key: bytes) -> None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_delete,
            namespace,
            key,
        )

    def _get_dbis(self, namespace: bytes) -> object:
        dbi = self._dbis.get(namespace)
        if dbi is None:
            dbi = self._env.open_db(namespace)
            self._dbis[namespace] = dbi
        return dbi

    def _sync_get(self, namespace: bytes, key: bytes) -> bytes | None:
        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=False) as txn:
            return txn.get(key)

    def _sync_put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=True) as txn:
            txn.put(key, value)

    def _sync_delete(self, namespace: bytes, key: bytes) -> None:
        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=True) as txn:
            txn.delete(key)


class LMDBStorageFactory:
    """
    Factory for creating LMDB-backed Storage instances. Each call to
    `create` produces a new LMDBStorage rooted at a subdirectory of the
    factory's base path. This allows the system to create multiple
    independent logical storage areas, for example one per partition or per
    subsystem.

    The factory encapsulates LMDB configuration parameters such as map
    size, maximum number of databases, and performance-related flags.
    All Storage instances created by the factory share the same
    configuration but operate on separate LMDB environments.
    """
    def __init__(
        self,
        path: Path,
        map_size: int = 1 << 30,
        max_dbs: int = 256,
        max_workers: int = 1,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = False
    ) -> None:
        self._path = path
        self._map_size = map_size
        self._max_dbs = max_dbs
        self._max_workers = max_workers
        self._readahead = readahead
        self._writemap = writemap
        self._sync = sync

    def create(self, sid: str) -> Storage:
        path = self._path / sid
        path.mkdir(exist_ok=True)
        return LMDBStorage(
            path=str(path),
            map_size=self._map_size,
            max_dbs=self._max_dbs,
            max_workers=self._max_workers,
            readahead=self._readahead,
            writemap=self._writemap,
            sync=self._sync
        )

    @property
    def max_namespaces(self) -> int:
        return self._max_dbs
