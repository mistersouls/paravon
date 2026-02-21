import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import AsyncIterator

from paravon.core.helpers.utils import decrement_key, increment_key
from paravon.core.ports.storage import Storage
from paravon.infra.lmdb_storage.backend import LMDBBackend


class LMDBStorage:
    def __init__(
        self,
        path: str,
        map_size: int = 1 << 30,
        max_dbs: int = 8,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = True,
        lock: bool = True,
        max_readers: int = 4,
        max_writers: int = 1,
    ) -> None:
        self._backend = LMDBBackend(
            path=path,
            map_size=map_size,
            max_dbs=max_dbs,
            readahead=readahead,
            writemap=writemap,
            sync=sync,
            lock=lock,
        )
        self._read_pool = ThreadPoolExecutor(max_workers=max_readers)
        self._write_pool = ThreadPoolExecutor(max_workers=max_writers)

    async def get(self, keyspace: bytes, key: bytes) -> bytes | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._read_pool, self._backend.get, keyspace, key
        )

    async def put(self, keyspace: bytes, key: bytes, value: bytes) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._write_pool, self._backend.put, keyspace, key, value
        )

    async def delete(self, keyspace: bytes, key: bytes) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._write_pool, self._backend.delete, keyspace, key
        )

    async def put_many(self, items: list[tuple[bytes, bytes, bytes]]) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._write_pool, self._backend.put_many, items
        )

    async def iter(
        self,
        keyspace: bytes,
        prefix: bytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
        batch_size: int = 1024,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        loop = asyncio.get_running_loop()
        remaining = None
        next_key = start

        if limit is not None and limit >= 0:
            if batch_size > limit:
                batch_size = limit
            remaining = limit

        if batch_size <= 0:
            return

        while True:
            batch: list[tuple[bytes, bytes]] = await loop.run_in_executor(
                self._read_pool,
                self._backend.scan,
                keyspace,
                prefix,
                next_key,
                batch_size,
                reverse
            )

            if not batch:
                break

            for key, value in batch:
                yield key, value

                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            # pagination strict
            if reverse:
                next_key = decrement_key(batch[-1][0])
            else:
                next_key = increment_key(batch[-1][0])

    async def close(self) -> None:
        def shutdown() -> None:
            self._backend.close()
            self._read_pool.shutdown(wait=True)
            self._write_pool.shutdown(wait=True)

        await asyncio.to_thread(shutdown)


class LMDBStorageFactory:
    def __init__(
        self,
        path: Path,
        map_size: int = 1 << 30,
        max_dbs: int = 256,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = True,
        lock: bool = True,
        max_readers: int = 4,
        max_writers: int = 1,
    ) -> None:
        self._path = path
        self._map_size = map_size
        self._max_dbs = max_dbs
        self._readahead = readahead
        self._writemap = writemap
        self._sync = sync
        self._lock = lock
        self._max_readers = max_readers
        self._max_writers = max_writers

        self._backends: dict[str, LMDBStorage] = {}
        self._backends_lock = asyncio.Lock()

    @property
    def max_keyspaces(self) -> int:
        return self._max_dbs

    async def get(self, sid: str) -> Storage:
        async with self._backends_lock:
            if sid not in self._backends:
                path = self._path / sid
                path.mkdir(exist_ok=True)
                backend = LMDBStorage(
                    path=str(path),
                    map_size=self._map_size,
                    max_dbs=self._max_dbs,
                    readahead=self._readahead,
                    writemap=self._writemap,
                    sync=self._sync,
                    lock=self._lock,
                    max_readers=self._max_readers,
                    max_writers=self._max_writers,
                )
                self._backends[sid] = backend

            return self._backends[sid]

    async def close(self) -> None:
        coros = [b.close() for b in self._backends.values()]
        await asyncio.gather(*coros, return_exceptions=True)
        self._backends.clear()
