import pytest

from paravon.core.storage.partitioned import PartitionedStorage


class DummyStorage:
    """Simple in‑memory backend for testing PartitionedStorage."""
    def __init__(self):
        self.data = {}

    async def get(self, keyspace, key):
        return self.data.get((keyspace, key))

    async def put(self, keyspace, key, value):
        self.data[(keyspace, key)] = value

    async def put_many(self, items):
        for keyspace, key, value in items:
            self.data[(keyspace, key)] = value

    async def delete(self, keyspace, key):
        self.data.pop((keyspace, key), None)

    async def iter(self, keyspace, prefix=None, start=None, limit=None, reverse=False, batch_size=1024):
        for (ks, key), value in self.data.items():
            if ks != keyspace:
                continue
            if prefix is not None and not key.startswith(prefix):
                continue
            yield key, value

    async def close(self):
        pass


class DummyFactory:
    """Fake StorageFactory that returns DummyStorage instances."""
    def __init__(self, max_keyspaces):
        self.max_keyspaces = max_keyspaces
        self.envs = {}

    async def get(self, env_index):
        if env_index not in self.envs:
            self.envs[env_index] = DummyStorage()
        return self.envs[env_index]

    async def close(self):
        self.envs.clear()


@pytest.fixture
def storage(serializer):
    factory = DummyFactory(max_keyspaces=4)
    return PartitionedStorage(factory, serializer)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_partition_routing(storage):
    # keyspace "0001" → pid = 1 → env_index = 1 // 4 = 0
    backend1 = await storage._select_backend(b"0001")

    # keyspace "0008" → pid = 8 → env_index = 8 // 4 = 2
    backend2 = await storage._select_backend(b"0008")

    assert backend1 is not backend2


@pytest.mark.ut
@pytest.mark.asyncio
async def test_put_get(storage):
    await storage.put(b"0001", b"a", b"hello")
    val = await storage.get(b"0001", b"a")
    assert val == b"hello"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_delete(storage):
    await storage.put(b"0001", b"a", b"x")
    await storage.delete(b"0001", b"a")
    assert await storage.get(b"0001", b"a") is None


@pytest.mark.ut
@pytest.mark.asyncio
async def test_put_many(storage):
    items = [
        (b"0001", b"a", b"1"),
        (b"0001", b"b", b"2"),
        (b"0001", b"c", b"3"),
    ]
    await storage.put_many(items)

    assert await storage.get(b"0001", b"a") == b"1"
    assert await storage.get(b"0001", b"b") == b"2"
    assert await storage.get(b"0001", b"c") == b"3"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_put_many_rejects_multiple_keyspaces(storage):
    items = [
        (b"0001", b"a", b"1"),
        (b"0002", b"b", b"2"),
    ]
    with pytest.raises(ValueError):
        await storage.put_many(items)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_iter_prefix(storage):
    await storage.put(b"0001", b"user:1", b"1")
    await storage.put(b"0001", b"user:2", b"2")
    await storage.put(b"0001", b"order:1", b"10")

    out = []
    async for k, v in storage.iter(b"0001", prefix=b"user:"):
        out.append((k, v))

    assert out == [(b"user:1", b"1"), (b"user:2", b"2")]


@pytest.mark.ut
@pytest.mark.asyncio
async def test_iter_start(storage):
    await storage.put(b"0001", b"a", b"1")
    await storage.put(b"0001", b"b", b"2")
    await storage.put(b"0001", b"c", b"3")

    # Dummy backend: start is ignored, but test ensures API works
    out = []
    async for k, v in storage.iter(b"0001", start=b"b"):
        out.append((k, v))

    # backend yields in insertion order
    assert (b"a", b"1") in out
    assert (b"b", b"2") in out
    assert (b"c", b"3") in out


@pytest.mark.ut
@pytest.mark.asyncio
async def test_two_keyspaces_two_envs(storage):
    await storage.put(b"0001", b"a", b"1")
    await storage.put(b"0008", b"a", b"2")

    backend1 = await storage._select_backend(b"0001")
    backend2 = await storage._select_backend(b"0008")

    assert backend1 is not backend2
    assert backend1.data[(b"0001", b"a")] == b"1"
    assert backend2.data[(b"0008", b"a")] == b"2"
