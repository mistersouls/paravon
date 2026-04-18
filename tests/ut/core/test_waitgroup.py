import asyncio
import pytest

from paravon.core.helpers.waitgroup import WaitGroup


@pytest.mark.ut
@pytest.mark.asyncio
async def test_single_add_single_done():
    wg = WaitGroup()
    await wg.add(1)
    assert not wg.is_done()

    await wg.done()
    assert wg.is_done()

    await wg.wait()  # should return immediately


@pytest.mark.ut
@pytest.mark.asyncio
async def test_multiple_add_multiple_done():
    wg = WaitGroup()
    await wg.add(3)
    assert not wg.is_done()

    await wg.done()
    assert not wg.is_done()

    await wg.done()
    assert not wg.is_done()

    await wg.done()
    assert wg.is_done()

    await wg.wait()  # should return immediately


@pytest.mark.ut
@pytest.mark.asyncio
async def test_multiple_waiters_simultaneous():
    wg = WaitGroup()
    await wg.add(1)

    async def waiter():
        await wg.wait()
        return True

    t1 = asyncio.create_task(waiter())
    t2 = asyncio.create_task(waiter())
    t3 = asyncio.create_task(waiter())

    await asyncio.sleep(0)
    assert not t1.done()
    assert not t2.done()
    assert not t3.done()

    await wg.done()

    assert await t1
    assert await t2
    assert await t3


@pytest.mark.ut
@pytest.mark.asyncio
async def test_wait_after_done_returns_immediately():
    wg = WaitGroup()
    await wg.add(1)
    await wg.done()

    # wait should not block
    await wg.wait()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_multiple_waiters_delayed():
    wg = WaitGroup()
    await wg.add(2)

    async def waiter():
        await wg.wait()
        return True

    t1 = asyncio.create_task(waiter())

    await asyncio.sleep(0)
    assert not t1.done()

    await wg.done()
    await asyncio.sleep(0)
    assert not t1.done()

    t2 = asyncio.create_task(waiter())  # late waiter
    await asyncio.sleep(0)
    assert not t2.done()

    await wg.done()

    assert await t1
    assert await t2


@pytest.mark.ut
@pytest.mark.asyncio
async def test_too_many_done_raises():
    wg = WaitGroup()
    await wg.add(1)
    await wg.done()

    with pytest.raises(RuntimeError):
        await wg.done()  # too many done()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_multiple_cycles_with_waiters():
    wg = WaitGroup()

    await wg.add(1)

    async def waiter1():
        await wg.wait()
        return "cycle1"

    w1 = asyncio.create_task(waiter1())

    await asyncio.sleep(0)
    assert not w1.done()

    await wg.done()

    assert await w1 == "cycle1"
    assert wg.is_done()

    await wg.add(2)

    async def waiter2():
        await wg.wait()
        return "cycle2"

    async def waiter3():
        await wg.wait()
        return "cycle2"

    w2 = asyncio.create_task(waiter2())
    w3 = asyncio.create_task(waiter3())

    await asyncio.sleep(0)
    assert not w2.done()
    assert not w3.done()

    await wg.done()
    await asyncio.sleep(0)
    assert not w2.done()
    assert not w3.done()

    await wg.done()

    assert await w2 == "cycle2"
    assert await w3 == "cycle2"
    assert wg.is_done()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_partial_results_recorded():
    wg = WaitGroup[str]()

    await wg.add(3)

    # Only 2 tasks record results
    await wg.done("a", True)
    await wg.done("b", False)
    await wg.done(None)   # no result recorded

    success, failed = await wg.wait()

    assert set(success) == {"a"}
    assert set(failed) == {"b"}
    # One task had no recorded result → OK


@pytest.mark.ut
@pytest.mark.asyncio
async def test_no_results_recorded():
    wg = WaitGroup[str]()

    await wg.add(3)

    await wg.done(None)
    await wg.done(None)
    await wg.done(None)

    success, failed = await wg.wait()

    assert success == []
    assert failed == []


@pytest.mark.ut
@pytest.mark.asyncio
async def test_partial_results_across_cycles():
    wg = WaitGroup[str]()

    # Cycle 1
    await wg.add(2)
    await wg.done("x", True)
    await wg.done(None)
    s1, f1 = await wg.wait()
    assert s1 == ["x"]
    assert f1 == []

    # Cycle 2
    await wg.add(3)
    await wg.done("y", False)
    await wg.done(None)
    await wg.done("z", True)
    s2, f2 = await wg.wait()
    assert set(s2) == {"z"}
    assert set(f2) == {"y"}
