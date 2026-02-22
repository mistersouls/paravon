import asyncio

import pytest

from paravon.core.helpers.hlc import HLC
from paravon.core.storage.codec import KeyCodec
from paravon.core.storage.versioned import VersionedStorage
from paravon.infra.lmdb_storage.aiobackend import LMDBStorage
from paravon.infra.msgpack_serializer import MsgPackSerializer


@pytest.fixture
def vs(tmp_path) -> VersionedStorage:
    backend = LMDBStorage(str(tmp_path), map_size=1 << 20)
    serializer = MsgPackSerializer()
    hlc = HLC.initial(node_id="node-1")
    return VersionedStorage(backend, hlc, serializer)


@pytest.mark.it
@pytest.mark.asyncio
async def test_vs_put_get(vs):
    await vs.put(b"ks", b"a", b"v1")
    val = await vs.get(b"ks", b"a")

    assert val == b"v1"


@pytest.mark.it
@pytest.mark.asyncio
async def test_vs_multiple_versions(vs):
    await vs.put(b"ks", b"a", b"v1")
    await vs.put(b"ks", b"a", b"v2")
    await vs.put(b"ks", b"a", b"v3")

    val = await vs.get(b"ks", b"a")
    assert val == b"v3"


@pytest.mark.it
@pytest.mark.asyncio
async def test_vs_delete(vs):
    await vs.put(b"ks", b"a", b"v1")
    await vs.delete(b"ks", b"a")

    val = await vs.get(b"ks", b"a")
    assert val is None


@pytest.mark.it
@pytest.mark.asyncio
async def test_vs_put_many(vs):
    await vs.put_many([
        (b"ks", b"a", b"1"),
        (b"ks", b"b", b"2"),
        (b"ks", b"c", b"3"),
    ])

    assert await vs.get(b"ks", b"a") == b"1"
    assert await vs.get(b"ks", b"b") == b"2"
    assert await vs.get(b"ks", b"c") == b"3"


@pytest.mark.asyncio
async def test_vs_iter_temporal_order(vs):
    await vs.put(b"ks", b"a", b"1")
    await asyncio.sleep(0.001)
    await vs.put(b"ks", b"b", b"2")
    await asyncio.sleep(0.001)
    await vs.put(b"ks", b"c", b"3")

    out = []
    async for k, v in vs.iter(b"ks"):
        out.append((k, v))

    assert out == [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
    ]


@pytest.mark.asyncio
async def test_vs_iter_reverse(vs):
    await vs.put(b"ks", b"a", b"1")
    await vs.put(b"ks", b"b", b"2")
    await vs.put(b"ks", b"c", b"3")

    out = []
    async for k, v in vs.iter(b"ks", reverse=True):
        out.append((k, v))

    assert out == [
        (b"c", b"3"),
        (b"b", b"2"),
        (b"a", b"1"),
    ]


@pytest.mark.asyncio
async def test_vs_iter_prefix(vs):
    await vs.put(b"ks", b"user:1", b"1")
    await vs.put(b"ks", b"user:2", b"2")
    await vs.put(b"ks", b"order:1", b"10")

    out = []
    # bad usage because prefix and start is for internal usage
    async for k, v in vs.iter(b"ks", prefix=b"user:"):
        out.append((k, v))

    assert out == []


@pytest.mark.asyncio
async def test_vs_multi_keyspace_isolation(vs):
    await vs.put(b"ks1", b"a", b"1")
    await vs.put(b"ks2", b"a", b"2")

    assert await vs.get(b"ks1", b"a") == b"1"
    assert await vs.get(b"ks2", b"a") == b"2"


@pytest.mark.asyncio
async def test_vs_hlc_persistence(tmp_path):
    backend = LMDBStorage(str(tmp_path), map_size=1 << 16)
    serializer = MsgPackSerializer()

    vs1 = VersionedStorage(backend, HLC.initial("node-1"), serializer)
    await vs1.put(b"ks", b"a", b"1")
    await vs1.close()

    # reopen
    backend2 = LMDBStorage(str(tmp_path), map_size=1 << 16)
    vs2 = VersionedStorage(backend2, HLC.initial("node-1"), serializer)

    await vs2.put(b"ks", b"a", b"2")
    val = await vs2.get(b"ks", b"a")

    assert val == b"2"

    await vs2.close()


@pytest.mark.asyncio
async def test_vs_iter_ignores_corrupted_index(vs):
    # normal entries
    await vs.put(b"ks", b"a", b"1")
    await vs.put(b"ks", b"b", b"2")

    # inject corrupted index key
    await vs._backend.put(VersionedStorage.INDEXSPACE, b"ksCORRUPTED", b"")

    out = []
    async for k, v in vs.iter(b"ks"):
        out.append((k, v))

    assert out == [(b"a", b"1"), (b"b", b"2")]


@pytest.mark.asyncio
async def test_vs_iter_since_hlc(vs):
    await vs.put(b"ks", b"a", b"v1")
    await vs.put(b"ks", b"b", b"v2")
    hlc_start = vs._hlc.tick_local().encode()
    await vs.put(b"ks", b"c", b"v3")


    start = KeyCodec.index_prefix(b"ks", hlc_start)

    out = []
    async for k, v in vs.iter(b"ks", start=start):
        out.append((k, v))

    assert out == [(b"c", b"v3")]
