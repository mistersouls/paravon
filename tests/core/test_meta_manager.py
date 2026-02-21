import ssl
from typing import cast

import pytest
from paravon.core.models.config import PeerConfig
from paravon.core.models.membership import NodePhase, NodeSize
from paravon.core.routing.app import RoutedApplication
from paravon.core.service.meta import NodeMetaManager
from tests.fake.fake_storage import FakeStorageFactory, FakeStorage


@pytest.fixture
def storage_factory():
    return FakeStorageFactory()


@pytest.fixture
def peer_config():
    return PeerConfig(
        node_id="node-1",
        node_size=NodeSize.M,
        host="127.0.0.1",
        port=0,
        app=RoutedApplication(),
        ssl_ctx=ssl.create_default_context(),
        client_ssl_ctx=ssl.create_default_context(),
        backlog=120,
        seeds=set(),
        peer_listener="127.0.0.1:0",
    )


@pytest.fixture
def manager(peer_config, storage_factory, serializer):
    return NodeMetaManager(peer_config, storage_factory, serializer)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_initializes_from_config(manager, storage_factory):
    membership = await manager.get_membership()

    assert membership.node_id == "node-1"
    assert membership.phase == NodePhase.idle

    # node_id and phase must be persisted
    storage = await storage_factory.get(NodeMetaManager.SYS_SID)
    keyspace = NodeMetaManager.SYS_KEYSPACE
    assert await storage.get(keyspace, b"node_id") == b'"node-1"'
    assert await storage.get(keyspace, b"phase") is None


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_reads_from_storage(
    peer_config, storage_factory, serializer
):
    storage = await storage_factory.get(NodeMetaManager.SYS_SID)
    await storage.put(
        NodeMetaManager.SYS_KEYSPACE,
        b"node_id",
        serializer.serialize("node-1")
    )
    await storage.put(
        NodeMetaManager.SYS_KEYSPACE,
        b"phase",
        serializer.serialize("ready")
    )

    manager = NodeMetaManager(peer_config, storage_factory, serializer)
    membership = await manager.get_membership()

    assert membership.node_id == "node-1"
    assert membership.phase == NodePhase.ready


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_raises_on_node_id_mismatch(
        peer_config, storage_factory, serializer
):
    storage = await storage_factory.get(NodeMetaManager.SYS_SID)
    await storage.put(NodeMetaManager.SYS_KEYSPACE, b"node_id", b'"other-node"')

    manager = NodeMetaManager(peer_config, storage_factory, serializer)

    with pytest.raises(RuntimeError) as exc:
        await manager.get_membership()

    msg = str(exc.value)
    assert "cannot change identity" in msg
    assert "other-node" in msg
    assert "node-1" in msg


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_membership_uses_cache(manager, storage_factory):
    storage = cast(
        FakeStorage,
        await storage_factory.get(NodeMetaManager.SYS_SID)
    )
    m1 = await manager.get_membership()
    initial_calls = storage.get_calls

    m2 = await manager.get_membership()
    later_calls = storage.get_calls

    assert m1 is m2
    assert later_calls == initial_calls  # no extra storage access


@pytest.mark.ut
@pytest.mark.asyncio
async def test_set_phase_initializes_membership_if_needed(
        manager, storage_factory
):
    storage = await storage_factory.get(NodeMetaManager.SYS_SID)
    await manager.set_phase(NodePhase.ready)

    membership = await manager.get_membership()
    assert membership.phase == NodePhase.ready

    stored = await storage.get(NodeMetaManager.SYS_KEYSPACE, b"phase")
    assert stored == b'"ready"'


@pytest.mark.ut
@pytest.mark.asyncio
async def test_set_phase_updates_cached_membership(manager, storage_factory):
    storage = cast(
        FakeStorage,
        await storage_factory.get(NodeMetaManager.SYS_SID)
    )
    await manager.get_membership()
    initial_calls = storage.get_calls

    await manager.set_phase(NodePhase.ready)

    assert manager._membership.phase == NodePhase.ready
    assert storage.get_calls == initial_calls
