import asyncio
import ssl
import pytest
from unittest.mock import AsyncMock, MagicMock

from paravon.core.models.membership import NodeSize
from paravon.core.routing.app import RoutedApplication
from paravon.core.service.coordinator import Coordinator
from paravon.core.models.request import (
    GetRequest, PutRequest, DeleteRequest, ReplicaSet, RequestContext
)
from paravon.core.models.config import PeerConfig
from paravon.core.space.partition import PartitionPlacement, LogicalPartition
from paravon.core.space.vnode import VNode


@pytest.fixture
def mocks():
    peer_clients = MagicMock()
    peer_clients.get = AsyncMock()

    return {
        "meta": AsyncMock(),
        "probe": MagicMock(),
        "topology": AsyncMock(),
        "pool": peer_clients,
        "spawner": MagicMock(),
        "storage": AsyncMock(),
    }


def make_coordinator(mocks):
    cfg = PeerConfig(
        node_id="A",
        node_size=NodeSize.M,
        host="127.0.0.1",
        port=0,
        app=RoutedApplication(),
        ssl_ctx=ssl.create_default_context(),
        client_ssl_ctx=ssl.create_default_context(),
        backlog=120,
        seeds=set(),
        peer_listener="127.0.0.1:0",
        partition_shift=16,
        replication_factor=3
    )

    mocks["spawner"].spawn.side_effect = lambda coro: asyncio.create_task(coro)

    return Coordinator(
        meta_manager=mocks["meta"],
        probe_manager=mocks["probe"],
        topology_manager=mocks["topology"],
        peer_clients=mocks["pool"],
        peer_config=cfg,
        spawner=mocks["spawner"],
        storage=mocks["storage"],
    )


def make_placement(node_id="A"):
    vnode = VNode(node_id=node_id, token=123)
    partition = LogicalPartition(pid=1, start=100, end=123)
    return PartitionPlacement(partition=partition, vnode=vnode)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_select_replicas_basic(mocks):
    coordinator = make_coordinator(mocks)
    ring = MagicMock()
    ring.iter_from.return_value = [
        VNode("A", 1),
        VNode("B", 2),
        VNode("C", 3),
        VNode("D", 4),
    ]
    mocks["topology"].get_ring.return_value = ring
    mocks["meta"].get_membership.return_value = MagicMock(node_id="A")
    mocks["probe"].is_alive.return_value = True

    replicas = await coordinator.select_replicas(VNode("A", 1), 3)

    assert replicas.local == "A"
    assert set(replicas.remotes) == {"B", "C"}


@pytest.mark.ut
@pytest.mark.asyncio
async def test_select_replicas_skips_dead_nodes(mocks):
    coordinator = make_coordinator(mocks)

    ring = MagicMock()
    ring.iter_from.return_value = [
        VNode("A", 1),
        VNode("B", 2),  # dead
        VNode("C", 3),
        VNode("D", 4),
    ]

    mocks["topology"].get_ring.return_value = ring
    mocks["meta"].get_membership.return_value = MagicMock(node_id="A")

    def alive(node_id):
        return node_id != "B"

    mocks["probe"].is_alive.side_effect = alive

    replicas = await coordinator.select_replicas(VNode("A", 1), 3)

    assert replicas.local == "A"
    assert set(replicas.remotes) == {"C", "D"}


@pytest.mark.ut
@pytest.mark.asyncio
async def test_coordinate_get_local_and_remote_success(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = GetRequest(key=b"k", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=("B", "C")
    ))

    mocks["storage"].get.return_value = b"v"

    async def fake_send(replica, msg):
        await asyncio.sleep(0)
        coordinator._handle_ok({
            "value": b"v",
            "source": replica,
            **msg.data
        })

    mocks["pool"].send.side_effect = fake_send

    message = await coordinator.get(request, placement)

    assert message.type == "ok"
    assert message.data["value"] == b"v"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_forward_get_to_primary(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = GetRequest(key=b"k", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local=None, remotes=("A", "B")
    ))

    async def fake_send(replica, msg):
        coordinator._handle_ok({
            "value": b"v",
            "request_id": msg.data["request_id"],
            "source": replica,
        })

    mocks["pool"].send.side_effect = fake_send

    message = await coordinator.get(request, placement)

    assert message.type == "ok", repr(message)
    assert message.data["value"] == b"v"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_coordinate_put_success(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = PutRequest(key=b"k", value=b"v", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=("B",)
    ))

    mocks["storage"].put.return_value = None

    async def fake_send(replica, msg):
        coordinator._handle_ok({
            "request_id": msg.data["request_id"],
            "source": replica
        })

    mocks["pool"].send.side_effect = fake_send

    message = await coordinator.put(request, placement)

    assert message.type == "ok"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_coordinate_delete_success(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = DeleteRequest(key=b"k", quorum=1, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=()
    ))

    mocks["storage"].delete.return_value = None

    message = await coordinator.delete(request, placement)

    assert message.type == "ok"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_forward_put(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = PutRequest(key=b"k", value=b"v", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local=None, remotes=("A", "B")
    ))

    async def fake_send(replica, msg):
        coordinator._handle_ok({
            "request_id": msg.data["request_id"],
            "source": replica
        })

    mocks["pool"].send.side_effect = fake_send

    message = await coordinator.put(request, placement)

    assert message.type == "ok", repr(message)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_forward_delete(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = DeleteRequest(key=b"k", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local=None, remotes=("A", "B")
    ))

    async def fake_send(replica, msg):
        coordinator._handle_ok({
            "request_id": msg.data["request_id"],
            "source": replica
        })

    mocks["pool"].send.side_effect = fake_send

    message = await coordinator.delete(request, placement)

    assert message.type == "ok", repr(message)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_quorum_failure_due_to_remote_errors(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = GetRequest(key=b"k", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=("B", "C")
    ))

    mocks["storage"].get.side_effect = Exception("local error")

    async def fake_send(replica, msg):
        coordinator._handle_ko({"request_id": msg.data["request_id"], "source": replica})

    mocks["pool"].send.side_effect = fake_send

    message = await coordinator.get(request, placement)

    assert message.type == "ko"
    assert "Quorum" in message.data["message"]


@pytest.mark.ut
@pytest.mark.asyncio
async def test_quorum_exact_threshold(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = GetRequest(key=b"k", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=("B",)
    ))

    mocks["storage"].get.return_value = b"v"

    async def fake_send(replica, msg):
        coordinator._handle_ok({
            "value": b"v",
            "request_id": msg.data["request_id"],
            "source": replica
        })

    mocks["pool"].send.side_effect = fake_send

    message = await coordinator.get(request, placement)

    assert message.type == "ok"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_timeout_waiting_for_quorum(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = GetRequest(key=b"k", quorum=2, timeout=0.01, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=("B",)
    ))

    mocks["storage"].get.return_value = b"v"

    async def never_reply(*_):
        await asyncio.sleep(1)

    mocks["pool"].send.side_effect = never_reply

    message = await coordinator.get(request, placement)

    assert message.type == "ko"
    assert "timeout" in message.data["message"].lower()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_remote_send_exception_marks_suspect(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = PutRequest(key=b"k", value=b"v", quorum=2, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=("B",)
    ))

    mocks["storage"].put.return_value = None

    async def failing_send(*_):
        raise RuntimeError("network error")

    mocks["pool"].send.side_effect = failing_send

    message = await coordinator.put(request, placement)

    mocks["probe"].mark_suspect.assert_called_once_with("B")
    assert message.type == "ko"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_local_write_exception(mocks):
    coordinator = make_coordinator(mocks)
    placement = make_placement("A")
    request = PutRequest(key=b"k", value=b"v", quorum=1, timeout=1, request_id="1")

    coordinator.select_replicas = AsyncMock(return_value=ReplicaSet(
        local="A", remotes=()
    ))

    mocks["storage"].put.side_effect = Exception("disk error")

    message = await coordinator.put(request, placement)

    assert message.type == "ko"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_handle_ko_does_not_mark_local_suspect(mocks):
    coordinator = make_coordinator(mocks)

    ctx = RequestContext(
        request_id="1",
        replicas=ReplicaSet(local="A", remotes=("B",)),
        quorum=1,
        future=asyncio.get_running_loop().create_future(),
        timeout=1,
    )
    coordinator._ctx["1"] = ctx

    coordinator._handle_ko({
        "request_id": "1",
        "source": "A"
    })

    mocks["probe"].mark_suspect.assert_not_called()
