from typing import Protocol, AsyncIterator


class Storage(Protocol):
    """
    Minimal asynchronous interface for a keyspaced key–value backend.
    A Storage implementation exposes a simple byte-oriented KV store
    where multiple logical datasets coexist via keyspaces.

    The interface does not prescribe durability, isolation, or
    transactional semantics. Implementations may provide stronger
    guarantees, but callers must not rely on anything beyond the
    behavior described here.
    """

    async def get(self, keyspace: bytes, key: bytes) -> bytes | None:
        """
        Retrieve the value associated with `key` inside the given
        keyspace. Returns None if the key does not exist.

        Implementations must not raise exceptions for missing keys.
        """

    async def put(self, keyspace: bytes, key: bytes, value: bytes) -> None:
        """
        Store `value` under `key` inside the keyspace. If the key
        already exists, its value is replaced.

        The write must be visible to subsequent calls to `get` within
        the same Storage instance. Durability depends on the backend.
        """

    async def put_many(self, items: list[tuple[bytes, bytes, bytes]]) -> None:
        """
        Store a batch of records efficiently.

        This method is designed for situations where multiple key–value pairs
        need to be written at once. Instead of issuing many individual writes,
        the storage layer groups them into a single operation, reducing overhead
        and ensuring that all updates are applied together.
        """

    async def delete(self, keyspace: bytes, key: bytes) -> None:
        """
        Remove the entry associated with `key` inside the keyspace.
        If the key does not exist, the method must succeed silently.

        After deletion, `get(key)` must return None.
        """

    async def close(self) -> None:
        """
        Release all underlying resources associated with this Storage
        instance (file handles, mmap regions, thread pools, etc.).

        After calling close(), the instance must not be used again.
        """

    def iter(
        self,
        keyspace: bytes,
        prefix: bytes | None = None,
        start: bytes | None = None,
        limit: int | None = None,
        reverse: bool = False,
        batch_size: int = 1024,
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """
        Asynchronously stream key–value pairs from the given keyspace.

        The iteration follows the natural lexicographic order of keys
        (or the reverse order if requested). Instead of holding a long-lived
        transaction, the scan progresses in short batches: each batch is
        fetched inside a worker thread, then yielded asynchronously once the
        transaction is closed. This makes the iteration safe for large
        datasets and prevents blocking the event loop.

        The caller may choose where iteration begins, restrict the scan to a
        prefix or a key range, or stop after a certain number of items. The
        method guarantees that keys are neither skipped nor duplicated across
        batches, even if the underlying store is being modified concurrently.

        This interface is suitable for streaming, pagination, incremental
        indexing, or any workload that benefits from controlled, batched
        traversal of a keyspace.
        """


class StorageFactory(Protocol):
    """
    Factory interface for creating Storage instances. A factory is
    responsible for producing independent Storage objects, typically
    identified by a string-based storage identifier (sid). This allows
    the system to create multiple logical storage areas, for example
    one per partition, per shard, or per subsystem.

    The factory may enforce limits on the number of keyspaces or
    storage instances it can create. Implementations are free to map
    the storage identifier to files, directories, database tables, or
    any other backend-specific structure.
    """

    @property
    def max_keyspaces(self) -> int:
        """
        Returns the maximum number of keyspaces supported by Storage
        instances created by this factory. Implementations that do not
        impose a limit may return a large constant or a sentinel value.

        This property allows callers to anticipate backend constraints
        and adapt their keyspace allocation strategy accordingly.
        """

    async def get(self, sid: str) -> Storage:
        """
        Return a Storage instance associated with the given storage
        identifier `sid`.

        If an instance for this identifier already exists, the factory may
        return the existing one. Otherwise, it must create a new instance.
        Callers must not assume whether instances are cached, pooled, or
        created on demand.

        The returned Storage must behave as a logically isolated key–value
        store, regardless of how the factory manages underlying resources.
        """

    async def close(self) -> None:
        """
        Release all resources associated with this factory and any
        Storage instances it created. After calling close(), no further
        Storage instances may be created.

        The method must not block the event loop.
        """
