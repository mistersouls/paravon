from unittest.mock import AsyncMock

import pytest

from paravon.core.models.membership import MembershipDiff, MembershipChange, Membership
from paravon.core.service.topology import TopologyManager
from paravon.core.space.ring import Ring
from tests.utils import make_member


@pytest.fixture
def topology(meta_manager, serializer):
    return TopologyManager(meta_manager, serializer)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_add_membership_updates_table_and_ring(topology, meta_manager):
    m = make_member("node-1", tokens=[10, 20, 30])

    await topology.add_membership(m)

    bucket_id = topology._table.bucket_for("node-1")
    assert "node-1" in topology._table.buckets[bucket_id].memberships

    ring = await topology.get_ring()
    assert ring.find_successor(10).node_id == "node-1"
    assert ring.find_successor(20).node_id == "node-1"

    meta_manager.set_incarnation.assert_awaited()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_apply_bucket_updates_ring(topology):
    before = make_member("node-1", tokens=[10])
    after = make_member("node-1", tokens=[20])

    diff = MembershipDiff(
        added=[],
        removed=[],
        updated=[MembershipChange(before=before, after=after)],
        bucket_id="0"
    )

    topology._table.merge_bucket = AsyncMock(return_value=diff)

    await topology.apply_bucket("0", [])

    ring = await topology.get_ring()

    assert len(ring) == 1
    assert ring[0].node_id == "node-1"
    assert ring[0].token == 20


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_bucket_memberships(topology):
    m = make_member("node-1")
    await topology.add_membership(m)

    bucket_id = topology._table.bucket_for("node-1")
    result = await topology.get_bucket_memberships(bucket_id)

    assert "node-1" in result
    assert isinstance(result["node-1"], Membership)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_checksums(topology):
    m = make_member("node-1")
    await topology.add_membership(m)

    checksums = await topology.get_checksums()

    assert isinstance(checksums, dict)
    assert len(checksums) == topology._TOTAL_BUCKETS


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_ring_returns_ring(topology):
    ring = await topology.get_ring()
    assert isinstance(ring, Ring)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_pick_random_membership(topology):
    m1 = make_member("node-1")
    m2 = make_member("node-2")

    await topology.add_membership(m1)
    await topology.add_membership(m2)

    picked = await topology.pick_random_membership()

    assert picked.node_id in {"node-1", "node-2"}


@pytest.mark.ut
@pytest.mark.asyncio
async def test_pick_random_membership_empty(topology):
    assert await topology.pick_random_membership() is None


@pytest.mark.ut
@pytest.mark.asyncio
async def test_remove_membership(topology, meta_manager):
    m = make_member("node-1", tokens=[10])
    await topology.add_membership(m)

    await topology.drain_membership(m)

    bucket_id = topology._table.bucket_for("node-1")
    assert "node-1" in topology._table.buckets[bucket_id].memberships

    ring = await topology.get_ring()

    assert len(ring) == 0

    meta_manager.set_incarnation.assert_awaited()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_restore_rebuilds_topology(topology):
    m1 = make_member("node-1", tokens=[10])
    m2 = make_member("node-2", tokens=[20])

    await topology.restore([m1, m2])

    assert "node-1" in topology._table._views
    assert "node-2" in topology._table._views

    ring = await topology.get_ring()

    assert len(ring) == 2
    tokens = sorted((v.token, v.node_id) for v in ring._vnodes)
    assert tokens == [(10, "node-1"), (20, "node-2")]

    assert ring.find_successor(10).node_id == "node-2"
    assert ring.find_successor(20).node_id == "node-1"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_restore_skips_local_node(topology, meta_manager):
    local = await meta_manager.get_membership()

    m1 = make_member(local.node_id, tokens=[10])
    m2 = make_member("node-2", tokens=[20])

    await topology.restore([m1, m2])

    assert local.node_id not in topology._table._views
    assert "node-2" in topology._table._views

    ring = await topology.get_ring()
    assert ring.find_successor(20).node_id == "node-2"
    assert ring.find_successor(10).node_id != local.node_id

