import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from paravon.core.cluster.probe import ProbeManager
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.message import Message


@pytest.fixture
def peer_clients():
    pool = AsyncMock()
    pool.subscribe = MagicMock()

    # client mock
    client = AsyncMock()
    client.send = AsyncMock()

    # always return the same mock client
    pool.get = AsyncMock(return_value=client)

    return pool


@pytest.fixture
def probe_manager(peer_clients, meta_manager):
    return ProbeManager(
        peer_clients=peer_clients,
        meta_manager=meta_manager,
        spawner=TaskSpawner(),
        cycle_count=2,
        ping_interval=0.1,
    )


@pytest.fixture(autouse=True)
def fast_sleep():
    with patch("asyncio.sleep", new=AsyncMock()) as _:
        yield


@pytest.mark.ut
@pytest.mark.asyncio
async def test_is_alive_when_fd_absent(probe_manager):
    # peer never seen -> alive
    assert probe_manager.is_alive("node-1") is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_mark_suspect_starts_probe(probe_manager):
    await probe_manager.mark_suspect("node-1")

    assert "node-1" in probe_manager._fds

    assert "node-1" in probe_manager._tasks

    task = probe_manager._tasks["node-1"]
    assert not probe_manager._tasks["node-1"].done()
    task.cancel()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_probe_receives_heartbeats_and_recovers(peer_clients, probe_manager):
    await probe_manager.mark_suspect("node-1")
    assert "node-1" in probe_manager._fds

    for _ in range(probe_manager.required_samples):
        msg = Message("ping/put", {"source": "node-1"})
        await probe_manager.handle(msg)

    assert probe_manager.is_alive("node-1") is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_probe_runs_until_fd_alive(peer_clients, probe_manager):
    await probe_manager.mark_suspect("node-1")
    task = probe_manager._tasks["node-1"]
    assert "node-1" in probe_manager._fds

    # simulate enough heartbeats to satisfy required_samples
    for _ in range(probe_manager.required_samples):
        msg = Message("ping/get", {"source": "node-1"})
        await probe_manager.handle(msg)

    # allow probe loop to iterate
    await asyncio.wait_for(task, timeout=0.01)

    assert task.done() is True
    assert probe_manager.is_alive("node-1") is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_probe_does_not_recover_with_insufficient_samples(probe_manager):
    await probe_manager.mark_suspect("node-1")
    fd = probe_manager._fds["node-1"]

    # send fewer samples than required
    for _ in range(probe_manager.required_samples - 1):
        msg = Message("ping/delete", {"source": "node-1"})
        await probe_manager.handle(msg)

    assert probe_manager.is_alive("node-1") is False
    probe_manager._tasks["node-1"].cancel()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_handle_records_heartbeat(probe_manager):
    await probe_manager.mark_suspect("node-1")
    fd = probe_manager._fds["node-1"]

    assert len(fd._arrival_times) == 0

    msg = Message("ping/put", {"source": "node-1"})
    await probe_manager.handle(msg)

    assert len(fd._arrival_times) == 1
    probe_manager._tasks["node-1"].cancel()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_run_stops_all_tasks(probe_manager):
    stop_event = asyncio.Event()

    await probe_manager.mark_suspect("node-1")
    task = probe_manager._tasks["node-1"]

    # stop manager
    stop_event.set()
    await probe_manager.run(stop_event)

    assert task.cancelled() is True
    assert probe_manager._stopped is True
