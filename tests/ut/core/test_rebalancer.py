import asyncio
from unittest.mock import AsyncMock

import pytest

from paravon.core.cluster.rebalance import Rebalancer
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer
from paravon.core.space.partition import Partitioner, LogicalPartition
from paravon.core.space.ring import Ring
from paravon.core.space.vnode import VNode
from tests.fake.fake_clients import FakeClientConnectionPool
from tests.fake.fake_storage_service import FakeStorageService
from tests.fake.fake_transport import JsonSerializer


def make_rebalancer(
    partitioner: Partitioner | None = None,
    spawner: TaskSpawner | None = None,
    serializer: Serializer | None = None,
    peer_clients: FakeClientConnectionPool | None = None,
    replication_factor: int = 3,
):
    partitioner = partitioner or Partitioner(partition_shift=2)

    return Rebalancer(
        partitioner=partitioner,
        peer_clients=peer_clients or AsyncMock(new=FakeClientConnectionPool),
        storage_service=AsyncMock(new=FakeStorageService),
        spawner=spawner or TaskSpawner(loop=asyncio.get_event_loop()),
        serializer=serializer or JsonSerializer(),
        replication_factor=replication_factor,
        node_id="node-1"
    )


@pytest.mark.ut
@pytest.mark.asyncio
async def test_rebalance_join():
    partitioner = Partitioner(partition_shift=2)

    end_0 = 1 << 126    # pid_0.end
    end_1 = 2 << 126    # pid_1.end
    end_2 = 3 << 126    # pid_2.end
    end_3 = 4 << 126    # pid_3.end

    old_ring = Ring([
        VNode("node-A", end_0), # pid_0: B, C, D
        VNode("node-B", end_1), # pid_1: C, D, A
        VNode("node-C", end_2), # pid_2: D, A, B
        VNode("node-D", end_3), # pid_3: A, B, C
    ])

    new_ring = Ring([
        VNode("node-A", end_0), # pid_0: B, C, 1
        VNode("node-B", end_1), # pid_1: C, 1, D
        VNode("node-C", end_2), # pid_2: 1, D, A
        VNode("node-1", end_2 + 1),
        VNode("node-D", end_3), # pid_3: A, B, C
    ])

    rebalancer = make_rebalancer(partitioner=partitioner)

    plan = await rebalancer.plan(old_ring, new_ring)

    p0 = LogicalPartition(pid=0, start=0, end=end_0)
    p1 = LogicalPartition(pid=1, start=end_0, end=end_1)
    p2 = LogicalPartition(pid=2, start=end_1, end=end_2)
    expected = {
        ("node-B", "node-C", "node-D"): [p0],
        ("node-C", "node-D", "node-A"): [p1],
        ("node-D", "node-A", "node-B"): [p2]
    }

    assert plan == expected
    await rebalancer.apply(plan)

    await rebalancer.handle(
        Message(
            type="partition/fetch",
            data={
                "source": "node-C",
                "keyspace": p0.pid_bytes
            }
        )
    )
    await rebalancer.handle(
        Message(
            type="partition/fetch",
            data={
                "source": "node-A",
                "keyspace": p1.pid_bytes
            }
        )
    )
    await rebalancer.handle(
        Message(
            type="partition/fetch",
            data={
                "source": "node-D",
                "keyspace": p2.pid_bytes
            }
        )
    )
    actual = await rebalancer.wait()
    assert set(actual[0]) == {0, 1, 2}
    assert actual[1] == []


@pytest.mark.ut
@pytest.mark.asyncio
async def test_rebalance_join_group():
    partitioner = Partitioner(partition_shift=3)

    end_0 = 1 << 125
    end_1 = 2 << 125
    end_2 = 3 << 125
    end_3 = 4 << 125
    end_4 = 5 << 125
    end_5 = 6 << 125
    end_6 = 7 << 125
    end_7 = 8 << 125

    # pid_0: A, C, B
    # pid_1: B, C, A
    # pid_2: B, C, A
    # pid_3: B, C, A
    # pid_4: A, D, C
    # pid_5: D, A, C
    # pid_6: D, A, C
    # pid_7: A, C, B
    old_ring = Ring([
        VNode("node-A", end_0 - 1),
        VNode("node-A", end_0 + 1),
        VNode("node-C", end_0 + 2),

        VNode("node-B", end_3 + 1),

        VNode("node-C", end_4),
        VNode("node-A", end_5),

        VNode("node-D", end_6 + 1),
        VNode("node-A", end_6 + 2),
    ])

    # pid_0: A, C, 1
    # pid_1: 1, B, C
    # pid_2: 1, B, C
    # pid_3: B, C, A
    # pid_4: A, D, C
    # pid_5: D, A, C
    # pid_6: D, A, C
    # pid_7: A, C, 1
    new_ring = Ring([
        VNode("node-A", end_0 - 1),
        VNode("node-A", end_0 + 1),
        VNode("node-C", end_0 + 2),

        VNode("node-1", end_2 - 1),
        VNode("node-1", end_3 - 1),

        VNode("node-B", end_3 + 1),

        VNode("node-C", end_4),
        VNode("node-A", end_5),

        VNode("node-D", end_6 + 1),
        VNode("node-A", end_6 + 2),
    ])

    rebalancer = make_rebalancer(partitioner=partitioner)
    plan = await rebalancer.plan(old_ring, new_ring)

    p0 = LogicalPartition(pid=0, start=0, end=end_0)
    p1 = LogicalPartition(pid=1, start=end_0, end=end_1)
    p2 = LogicalPartition(pid=2, start=end_1, end=end_2)
    p7 = LogicalPartition(pid=7, start=end_6, end=end_7)
    expected = {
        ("node-A", "node-C", "node-B"): [p0, p7],
        ("node-B", "node-C", "node-A"): [p1, p2]
    }

    await rebalancer.apply(plan)

    await rebalancer.handle(
        Message(
            type="partition/fetch",
            data={
                "source": "node-C",
                "keyspace": p0.pid_bytes
            }
        )
    )
    await rebalancer.handle(
        Message(
            type="partition/fetch",
            data={
                "source": "node-B",
                "keyspace": p7.pid_bytes
            }
        )
    )
    await rebalancer.handle(
        Message(
            type="partition/fetch",
            data={
                "source": "node-A",
                "keyspace": p1.pid_bytes
            }
        )
    )
    await rebalancer.handle(
        Message(
            type="partition/fetch",
            data={
                "source": "node-C",
                "keyspace": p2.pid_bytes
            }
        )
    )

    assert plan == expected
    actual = await rebalancer.wait()
    assert set(actual[0]) == {0, 7, 1, 2}
    assert actual[1] == []


@pytest.mark.ut
@pytest.mark.asyncio
async def test_plan_drain():
    partitioner = Partitioner(partition_shift=2)

    end_0 = 1 << 126
    end_1 = 2 << 126
    end_2 = 3 << 126
    end_3 = 4 << 126

    old_ring = Ring([
        VNode("node-A", end_0), # pid_0: B, C, X
        VNode("node-B", end_1), # pid_1: C, X, 1
        VNode("node-C", end_2), # pid_2: X, 1, A
        VNode("node-X", end_2 + 1),
        VNode("node-1", end_3), # pid_3: A, B, C
    ])

    new_ring = Ring([
        VNode("node-A", end_0), # pid_0: B, C, 1
        VNode("node-B", end_1), # pid_1: C, 1, A
        VNode("node-C", end_2), # pid_2: 1, A, B
        VNode("node-1", end_3)  # pid_3: A, B, C
    ])

    p0 = LogicalPartition(pid=0, start=0, end=end_0)
    expected = {
        ("node-B", "node-C", "node-X"): [p0]
    }

    rebalancer = make_rebalancer(partitioner=partitioner)

    plan = await rebalancer.plan(old_ring, new_ring)

    assert plan == expected
