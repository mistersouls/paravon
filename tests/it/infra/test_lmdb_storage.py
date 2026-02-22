import pytest
from paravon.infra.lmdb_storage.aiobackend import LMDBStorage


@pytest.fixture
def storage(tmp_path):
    return LMDBStorage(path=str(tmp_path), map_size=1 << 16)


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_basic_put_get_delete(storage):
    await storage.put(b"ks", b"a", b"1")
    assert await storage.get(b"ks", b"a") == b"1"

    await storage.delete(b"ks", b"a")
    assert await storage.get(b"ks", b"a") is None

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_put_many_and_iter_forward(storage):

    items = [
        (b"ks", b"a", b"1"),
        (b"ks", b"b", b"2"),
        (b"ks", b"c", b"3"),
    ]
    await storage.put_many(items)

    out = []
    async for k, v in storage.iter(b"ks"):
        out.append((k, v))

    assert out == [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_iter_reverse(storage):
    await storage.put(b"ks", b"a", b"1")
    await storage.put(b"ks", b"b", b"2")
    await storage.put(b"ks", b"c", b"3")

    out = []
    async for k, v in storage.iter(b"ks", reverse=True):
        out.append((k, v))

    assert out == [(b"c", b"3"), (b"b", b"2"), (b"a", b"1")]

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_iter_with_prefix(storage):
    await storage.put(b"ks", b"user:1", b"1")
    await storage.put(b"ks", b"user:2", b"2")
    await storage.put(b"ks", b"order:1", b"10")

    out = []
    async for k, v in storage.iter(b"ks", prefix=b"user:"):
        out.append((k, v))

    assert out == [(b"user:1", b"1"), (b"user:2", b"2")]

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_iter_prefix_and_start(storage):
    await storage.put(b"ks", b"user:1", b"1")
    await storage.put(b"ks", b"user:2", b"2")
    await storage.put(b"ks", b"user:3", b"3")

    out = []
    async for k, v in storage.iter(b"ks", prefix=b"user:", start=b"user:2"):
        out.append((k, v))

    assert out == [(b"user:2", b"2"), (b"user:3", b"3")]

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_iter_prefix_reverse_limit(storage):
    await storage.put(b"ks", b"user:1", b"1")
    await storage.put(b"ks", b"user:2", b"2")
    await storage.put(b"ks", b"user:3", b"3")

    out = []
    async for k, v in storage.iter(b"ks", prefix=b"user:", reverse=True, limit=2):
        out.append((k, v))

    assert out == [(b"user:3", b"3"), (b"user:2", b"2")]

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_iter_pagination_forward(storage):
    items = [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ]
    for k, v in items:
        await storage.put(b"ks", k, v)

    out = []
    async for k, v in storage.iter(b"ks", batch_size=2):
        out.append((k, v))

    assert out == items

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_iter_pagination_reverse(storage):
    items = [
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ]
    for k, v in items:
        await storage.put(b"ks", k, v)

    out = []
    async for k, v in storage.iter(b"ks", batch_size=2, reverse=True):
        out.append((k, v))

    assert out == list(reversed(items))

    await storage.close()


@pytest.mark.it
@pytest.mark.asyncio
async def test_storage_close(storage):
    await storage.put(b"ks", b"a", b"1")
    await storage.close()

    with pytest.raises(Exception):
        await storage.put(b"ks", b"b", b"2")
