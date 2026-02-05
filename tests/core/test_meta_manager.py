import ssl

import pytest
from paravon.core.models.config import PeerConfig
from paravon.core.models.meta import Membership, NodePhase
from paravon.core.ports.storage import Storage
from paravon.core.routing.app import RoutedApplication
from paravon.core.service.meta import NodeMetaManager
from tests.fake.fake_storage import FakeStorage


@pytest.fixture
def storage():
    return FakeStorage()


@pytest.fixture
def peer_config():
    return PeerConfig(
        node_id="node-1",
        host="127.0.0.1",
        port=0,
        app=RoutedApplication(),
        ssl_ctx=ssl.create_default_context(),
        backlog=120,
        seeds=set()
    )


@pytest.fixture
def manager(peer_config, storage):
    return NodeMetaManager(peer_config, storage)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_initializes_from_config(manager, storage):
    membership = await manager.get_membership()

    assert membership.node_id == "node-1"
    assert membership.phase == NodePhase.idle

    # node_id and phase must be persisted
    assert await storage.get(b"system", b"node_id") == b"node-1"
    assert await storage.get(b"system", b"phase") == b"idle"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_reads_from_storage(peer_config, storage):
    await storage.put(b"system", b"node_id", b"node-1")
    await storage.put(b"system", b"phase", b"ready")

    manager = NodeMetaManager(peer_config, storage)
    membership = await manager.get_membership()

    assert membership.node_id == "node-1"
    assert membership.phase == NodePhase.ready


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_raises_on_node_id_mismatch(peer_config, storage):
    await storage.put(b"system", b"node_id", b"other-node")

    manager = NodeMetaManager(peer_config, storage)

    with pytest.raises(RuntimeError) as exc:
        await manager.get_membership()

    msg = str(exc.value)
    assert "cannot change identity" in msg
    assert "other-node" in msg
    assert "node-1" in msg


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_uses_cache(manager, storage):
    m1 = await manager.get_membership()
    initial_calls = storage.get_calls

    m2 = await manager.get_membership()
    later_calls = storage.get_calls

    assert m1 is m2
    assert later_calls == initial_calls  # no extra storage access


@pytest.mark.ut
@pytest.mark.asyncio
async def test_set_phase_initializes_membership_if_needed(manager, storage):
    await manager.set_phase(NodePhase.ready)

    membership = await manager.get_membership()
    assert membership.phase == NodePhase.ready

    stored = await storage.get(b"system", b"phase")
    assert stored == b"ready"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_set_phase_updates_cached_membership(manager, storage):
    await manager.get_membership()
    initial_calls = storage.get_calls

    await manager.set_phase(NodePhase.ready)

    assert manager._membership.phase == NodePhase.ready
    assert storage.get_calls == initial_calls
