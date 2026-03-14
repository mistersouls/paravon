import pytest
from paravon.core.storage.partitioned import PartitionedStorage
from paravon.core.models.version import ValueVersion, HLC


def make_version(value: bytes | None, node="A", tombstone=False):
    hlc = HLC.initial(node)
    if tombstone:
        return ValueVersion.tombstone(hlc=hlc, origin=node)
    return ValueVersion.from_bytes(value=value, hlc=hlc, origin=node)


class DummyStorage:
    """Simple in‑memory backend returning ValueVersion objects."""
    def __init__(self):
        self.data = {}

    async def get(self, keyspace, key):
        return self.data.get((keyspace, key))

    async def put(self, keyspace, key, value):
        version = make_version(value)
        self.data[(keyspace, key)] = version
        return version

    async def delete(self, keyspace, key):
        version = make_version(None, tombstone=True)
        self.data[(keyspace, key)] = version
        return version

    async def apply(self, keyspace, key, version):
        self.data[(keyspace, key)] = version
        return version

    async def iter(self, keyspace, hlc, batch_size=1024):
        for (ks, key), version in self.data.items():
            if ks == keyspace:
                yield key, version

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
    backend1 = await storage._select_backend(b"0001")  # pid=1 → env=0
    backend2 = await storage._select_backend(b"0008")  # pid=8 → env=2
    assert backend1 is not backend2


@pytest.mark.ut
@pytest.mark.asyncio
async def test_put_get(storage):
    await storage.put(b"0001", b"a", b"hello")
    version = await storage.get(b"0001", b"a")
    assert isinstance(version, ValueVersion)
    assert version.value == b"hello"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_delete(storage):
    await storage.put(b"0001", b"a", b"x")
    await storage.delete(b"0001", b"a")
    version = await storage.get(b"0001", b"a")
    assert version.is_tombstone is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_iter(storage):
    await storage.put(b"0001", b"a", b"1")
    await storage.put(b"0001", b"b", b"2")
    await storage.put(b"0001", b"c", b"3")

    out = []
    async for k, v in storage.iter(b"0001", hlc=HLC.initial("A")):
        out.append((k, v.value))

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
    assert backend1.data[(b"0001", b"a")].value == b"1"
    assert backend2.data[(b"0008", b"a")].value == b"2"
