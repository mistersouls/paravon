import asyncio
import pytest

from paravon.core.transport.flow import FlowControl


@pytest.mark.ut
@pytest.mark.asyncio
async def test_initial_state():
    fc = FlowControl()
    assert fc.write_paused is False
    assert fc._writable.is_set() is True

    await fc.drain()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_pause_writing():
    fc = FlowControl()

    fc.pause_writing()
    assert fc.write_paused is True
    assert fc._writable.is_set() is False


@pytest.mark.ut
@pytest.mark.asyncio
async def test_resume_writing():
    fc = FlowControl()

    fc.pause_writing()
    assert fc.write_paused is True

    fc.resume_writing()
    assert fc.write_paused is False
    assert fc._writable.is_set() is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_drain_blocks_until_resume():
    fc = FlowControl()
    fc.pause_writing()

    async def waiter():
        await fc.drain()
        return "done"

    task = asyncio.create_task(waiter())

    await asyncio.sleep(0)  # yield control loop
    assert not task.done()

    fc.resume_writing()

    result = await task
    assert result == "done"


@pytest.mark.asyncio
async def test_multiple_drains_unblocked_on_resume():
    fc = FlowControl()
    fc.pause_writing()

    results = []

    async def waiter(i):
        await fc.drain()
        results.append(i)

    tasks = [asyncio.create_task(waiter(i)) for i in range(3)]

    await asyncio.sleep(0)  # yield control loop

    assert results == []

    fc.resume_writing()

    await asyncio.gather(*tasks)

    assert results == [0, 1, 2]
