from typing import Protocol


class Storage(Protocol):
    """
    Defines the minimal asynchronous interface required for a storage
    backend used by the system. A Storage implementation provides a
    simple namespaced key–value store where both keys and values are
    raw bytes. The namespace mechanism allows multiple logical datasets
    to coexist within the same backend without interfering with each
    other.

    The interface is intentionally minimal. It does not prescribe any
    durability, isolation, or transactional guarantees. Each concrete
    implementation is free to provide stronger semantics, but callers
    must not rely on anything beyond the behavior described here.
    """

    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        """
        Retrieves the value associated with the given key inside the
        specified namespace. If the key does not exist, the method
        returns None.

        The method must not raise an exception for missing keys. It is
        the caller's responsibility to interpret the absence of a value.
        """

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        """
        Stores the given value under the specified key inside the
        namespace. If the key already exists, its value is replaced.

        The method must ensure that the write is visible to subsequent
        calls to `get` within the same Storage instance. The exact
        durability guarantees depend on the implementation.
        """

    async def delete(self, namespace: bytes, key: bytes) -> None:
        """
        Removes the entry associated with the given key inside the
        namespace. If the key does not exist, the method must succeed
        silently.

        After deletion, a call to `get` for the same key must return
        None.
        """


class StorageFactory(Protocol):
    """
    Factory interface for creating Storage instances. A factory is
    responsible for producing independent Storage objects, typically
    identified by a string-based storage identifier (sid). This allows
    the system to create multiple logical storage areas, for example
    one per partition, per shard, or per subsystem.

    The factory may enforce limits on the number of namespaces or
    storage instances it can create. Implementations are free to map
    the storage identifier to files, directories, database tables, or
    any other backend-specific structure.
    """

    def create(self, sid: str) -> Storage:
        """
        Creates and returns a new Storage instance associated with the
        given storage identifier. The returned Storage must be usable
        independently of other Storage instances produced by the same
        factory.

        The factory may choose to reuse existing underlying resources
        (e.g., shared database connections), but each Storage instance
        must behave as a logically isolated key–value store.
        """

    @property
    def max_namespaces(self) -> int:
        """
        Returns the maximum number of namespaces supported by Storage
        instances created by this factory. Implementations that do not
        impose a limit may return a large constant or a sentinel value.

        This property allows callers to anticipate backend constraints
        and adapt their namespace allocation strategy accordingly.
        """
