import pytest

from paravon.core.space.hashspace import HashSpace
from paravon.core.space.partition import Partitioner, LogicalPartition, PartitionPlacement
from paravon.core.space.ring import Ring
from paravon.core.space.vnode import VNode


@pytest.mark.ut
def test_total_partitions():
    p = Partitioner(partition_shift=4)
    assert p.total_partitions == 16


@pytest.mark.ut
def test_step_size():
    p = Partitioner(partition_shift=8)
    assert p.step == (1 << 128) >> 8


@pytest.mark.ut
def test_pid_for_hash_simple():
    p = Partitioner(partition_shift=4)  # 16 partitions
    h = 0xF000_0000_0000_0000_0000_0000_0000_0000
    pid = p.pid_for_hash(h)
    assert 0 <= pid < p.total_partitions


@pytest.mark.ut
def test_segment_boundaries_are_correct():
    p = Partitioner(4)
    pid = 3
    start, end = p.segment_for_pid(pid)
    assert start == pid * p.step
    assert end == (pid + 1) * p.step


@pytest.mark.ut
def test_partition_for_hash_returns_correct_partition():
    p = Partitioner(4)
    h = 123456789
    part = p.partition_for_hash(h)
    assert isinstance(part, LogicalPartition)
    assert part.pid == p.pid_for_hash(h)
    assert part.contains(h)


@pytest.mark.ut
def test_find_partition_by_key(monkeypatch):
    p = Partitioner(4)

    # Force HashSpace.hash to return a known value
    monkeypatch.setattr(HashSpace, "hash", lambda key: 42)

    part = p.find_partition_by_key(b"hello")
    assert part.pid == p.pid_for_hash(42)
    assert part.contains(42)


@pytest.mark.ut
def test_find_placement_by_key(monkeypatch):
    p = Partitioner(4)

    # Fake hash
    monkeypatch.setattr(HashSpace, "hash", lambda key: 100)

    # Build a simple ring with 3 vnodes
    vnodes = [
        VNode(node_id="A", token=50),
        VNode(node_id="B", token=150),
        VNode(node_id="C", token=250),
    ]
    ring = Ring(vnodes)

    placement = p.find_placement_by_key(b"k", ring)

    assert isinstance(placement, PartitionPlacement)
    assert placement.partition.contains(100)
    assert placement.vnode.node_id == "A"  # successor of partition.end


@pytest.mark.ut
def test_logical_partition_contains():
    part = LogicalPartition(pid=2, start=100, end=200)
    assert part.contains(150)
    assert not part.contains(100)
    assert part.contains(200)


@pytest.mark.ut
def test_pid_bytes_encoding():
    part = LogicalPartition(pid=15, start=0, end=10)
    assert part.pid_bytes == b"f"


@pytest.mark.ut
def test_partition_placement_keyspace():
    part = LogicalPartition(pid=7, start=0, end=10)
    vnode = VNode(node_id="X", token=123)
    placement = PartitionPlacement(part, vnode)

    assert placement.keyspace == b"7"
    assert placement.vnode.node_id == "X"
