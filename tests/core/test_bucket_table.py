import pytest
from paravon.core.models.membership import NodePhase
from tests.utils import make_member


@pytest.mark.ut
def test_bucket_for_is_deterministic(table):
    assert table.bucket_for("abc") == table.bucket_for("abc")
    assert table.bucket_for("abc") != table.bucket_for("xyz")


@pytest.mark.ut
@pytest.mark.asyncio
async def test_add_or_update_inserts_membership(table, meta_manager):
    m = make_member("node-1")
    await table.add_or_update(m)

    bucket_id = table.bucket_for("node-1")
    bucket = table.buckets[bucket_id]

    assert "node-1" in bucket.memberships
    assert table._views["node-1"] == bucket_id
    meta_manager.bump_incarnation.assert_awaited()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_add_or_update_updates_existing(table):
    m1 = make_member("node-1", epoch=1)
    m2 = make_member("node-1", epoch=2)

    await table.add_or_update(m1)
    await table.add_or_update(m2)

    bucket_id = table.bucket_for("node-1")
    assert table.buckets[bucket_id].memberships["node-1"].epoch == 2


@pytest.mark.ut
@pytest.mark.asyncio
async def test_remove_deletes_membership(table, meta_manager):
    m = make_member("node-1")
    await table.add_or_update(m)

    await table.remove("node-1")

    bucket_id = table.bucket_for("node-1")
    assert "node-1" not in table.buckets[bucket_id].memberships
    assert "node-1" not in table._views
    meta_manager.bump_incarnation.assert_awaited()


@pytest.mark.ut
def test_checksums_cached(table):
    c1 = table.get_checksums()
    c2 = table.get_checksums()
    assert c1 is c2  # same object due to caching


@pytest.mark.ut
def test_checksums_invalidate_on_change(table):
    c1 = table.get_checksums()
    table._mark_dirty()
    c2 = table.get_checksums()
    assert c1 is not c2


@pytest.mark.ut
@pytest.mark.asyncio
async def test_peek_random_member_returns_member(table):
    m = make_member("node-1")
    await table.add_or_update(m)

    picked = table.peek_random_member()
    assert picked.node_id == "node-1"


@pytest.mark.ut
def test_peek_random_member_empty(table):
    assert table.peek_random_member() is None


@pytest.mark.ut
@pytest.mark.asyncio
async def test_merge_bucket_inserts_new_members(table):
    m = make_member("node-1", epoch=1)
    bucket_id = table.bucket_for("node-1")

    await table.merge_bucket(bucket_id, [m])

    assert "node-1" in table.buckets[bucket_id].memberships
    assert table._views["node-1"] == bucket_id


@pytest.mark.ut
@pytest.mark.asyncio
async def test_merge_bucket_updates_epoch(table):
    m1 = make_member("node-1", epoch=1)
    m2 = make_member("node-1", epoch=5)

    bucket_id = table.bucket_for("node-1")

    await table.merge_bucket(bucket_id, [m1])
    await table.merge_bucket(bucket_id, [m2])

    assert table.buckets[bucket_id].memberships["node-1"].epoch == 5


@pytest.mark.ut
@pytest.mark.asyncio
async def test_merge_bucket_ignores_expired(table, meta_manager):
    meta_manager.get_membership.return_value.incarnation = 10

    expired = make_member("node-1", epoch=1, incarnation=1)
    bucket_id = table.bucket_for("node-1")

    await table.merge_bucket(bucket_id, [expired])

    assert "node-1" not in table.buckets[bucket_id].memberships


@pytest.mark.ut
@pytest.mark.asyncio
async def test_merge_bucket_purges_removed(table, meta_manager):
    # Local membership in remove phase
    m_local = make_member("node-1", epoch=1)
    m_local.phase = NodePhase.idle

    bucket_id = table.bucket_for("node-1")
    table.buckets[bucket_id].memberships["node-1"] = m_local
    table._views["node-1"] = bucket_id

    # Remote does NOT include node-1
    meta_manager.get_membership.return_value.incarnation = 10  # force TTL expiry

    await table.merge_bucket(bucket_id, [])

    assert "node-1" not in table.buckets[bucket_id].memberships
    assert "node-1" not in table._views


@pytest.mark.ut
@pytest.mark.asyncio
async def test_merge_bucket_syncs_incarnation(table, meta_manager):
    remote = make_member("node-1", epoch=1, incarnation=50)
    bucket_id = table.bucket_for("node-1")

    await table.merge_bucket(bucket_id, [remote])

    meta_manager.set_incarnation.assert_awaited_with(50)
