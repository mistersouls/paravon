import threading

import lmdb


class LMDBBackend:
    def __init__(
        self,
        path: str,
        map_size: int = 1 << 30,
        max_dbs: int = 8,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = True,
        lock: bool = True,
    ) -> None:
        self._env = lmdb.open(
            path,
            map_size=map_size,
            max_dbs=max_dbs,
            lock=lock,
            writemap=writemap,
            sync=sync,
            readahead=readahead,
        )
        self._dbis: dict[bytes, object] = {}
        self._dbis_lock = threading.Lock()

    def get(self, db_name: bytes, key: bytes) -> bytes | None:
        dbi = self._get_dbi(db_name)
        with self._env.begin(db=dbi, write=False) as txn:
            return txn.get(key)

    def put(self, db_name: bytes, key: bytes, value: bytes) -> bool:
        dbi = self._get_dbi(db_name)
        with self._env.begin(db=dbi, write=True) as txn:
            return txn.put(key, value)

    def put_many(self, items: list[tuple[bytes, bytes, bytes]]) -> None:
        dbis: dict[bytes, object] = {}
        for db_name, _, _ in items:
            if db_name not in dbis:
                dbis[db_name] = self._get_dbi(db_name)

        with self._env.begin(write=True) as txn:
            for db_name, key, value in items:
                txn.put(key, value, db=dbis[db_name])

    def delete(self, db_name: bytes, key: bytes) -> bool:
        dbi = self._get_dbi(db_name)
        with self._env.begin(db=dbi, write=True) as txn:
            return txn.delete(key)

    def scan(
        self,
        db_name: bytes,
        prefix: bytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
    ) -> list[tuple[bytes, bytes]]:
        """
        Scan the LMDB keyspace with optional prefix filtering,
        pagination and direction.

        Parameters
        ----------
        db_name : bytes
            Name of the LMDB database (physical DBI).
            This does NOT act as a logical namespace.
            Keys inside the DBI are scanned lexicographically.

        prefix : bytes | None
            Logical namespace filter. Only keys starting with
            this prefix are returned.
            This is a *hard* lexical filter:
                - forward: stop when key no longer starts with prefix
                - reverse: stop when key no longer starts with prefix

            If prefix is provided and start is None:
                - forward: cursor starts at the first key >= prefix
                - reverse: cursor starts at the last key <= prefix + b"\xff"

        start : bytes | None
            Lexicographic starting point for pagination.
            This is NOT a prefix. It is a lower (forward) or upper (reverse) bound.

            Semantics:
                - forward: first key >= start
                - reverse: last key <= start + b"\xff"

            If both prefix and start are provided:
                - start determines the initial cursor position
                - prefix restricts the scan range
                - if start is outside prefix, the scan returns []

        limit : int | None
            Maximum number of items to return.
            If None, scan continues until prefix boundary or end of DBI.

        reverse : bool
            Direction of the scan.
                - False: forward (ascending lexicographic order)
                - True: reverse (descending lexicographic order)

        Behavior summary
        ----------------
        1. If start is provided:
            - start determines the initial cursor position
            - prefix acts as a filter (stop when key no longer matches)

        2. If start is None and prefix is provided:
            - forward: cursor.set_range(prefix)
            - reverse: cursor.set_range(prefix + b"\xff") then cursor.prev()

        3. If neither start nor prefix is provided:
            - forward: cursor.first()
            - reverse: cursor.last()

        4. The scan stops when:
            - limit is reached
            - cursor cannot advance
            - key does not start with prefix (if prefix is provided)
        """
        if limit is not None and limit <= 0:
            return []

        dbi = self._get_dbi(db_name)
        items: list[tuple[bytes, bytes]] = []

        with self._env.begin(write=False) as txn:
            with txn.cursor(db=dbi) as cursor:
                if not self._position_cursor(cursor, prefix, start, reverse):
                    return []

                while True:
                    key = cursor.key()

                    if prefix is not None and not key.startswith(prefix):
                        break

                    items.append((key, cursor.value()))
                    if limit is not None and len(items) >= limit:
                        break

                    if not self._advance_cursor(cursor, reverse):
                        break

        return items

    def close(self) -> None:
        self._dbis.clear()
        self._env.close()

    @staticmethod
    def _advance_cursor(cursor: lmdb.Cursor, reverse: bool) -> bool:
        if reverse:
            return cursor.prev()
        else:
            return cursor.next()

    def _get_dbi(self, name: bytes) -> object:
        with self._dbis_lock:
            dbi = self._dbis.get(name)
            if dbi is None:
                dbi = self._env.open_db(name)
                self._dbis[name] = dbi
            return dbi

    def _position_cursor(
        self,
        cursor: lmdb.Cursor,
        prefix: bytes | None,
        start: bytes | None,
        reverse: bool
    ) -> bool:
        if start is not None:
            return self._cursor_range(cursor, start, reverse)

        if prefix is not None:
            return self._cursor_range(cursor, prefix, reverse)

        if reverse:
            return cursor.last()
        else:
            return cursor.first()

    @staticmethod
    def _cursor_range(cursor: lmdb.Cursor, key: bytes, reverse: bool) -> bool:
        if reverse:
            key_end = key + b"\xff"
            if cursor.set_range(key_end):
                return cursor.prev()
            return cursor.last()
        else:
            return cursor.set_range(key)
