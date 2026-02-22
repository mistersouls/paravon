import asyncio
import pytest

from paravon.core.helpers.sub import Subscription


@pytest.mark.ut
@pytest.mark.asyncio
async def test_single_subscriber_receives_messages():
    sub = Subscription(asyncio.get_event_loop())

    async def consumer():
        async for v in sub:
            return v
        return None

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    sub.publish("hello")
    await asyncio.sleep(0)

    assert await task == "hello"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_multicast_two_subscribers_receive_same_messages():
    sub = Subscription(asyncio.get_event_loop())

    async def consumer():
        async for v in sub:
            return v
        return None

    c1 = asyncio.create_task(consumer())
    c2 = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    sub.publish("msg")
    await asyncio.sleep(0)

    assert await c1 == "msg"
    assert await c2 == "msg"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_subscribers_independent_progression():
    sub = Subscription(asyncio.get_event_loop())

    async def consumer():
        results = []
        async for v in sub:
            results.append(v)
            if len(results) == 2:
                return results
        return None

    c1 = asyncio.create_task(consumer())
    c2 = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    sub.publish(1)
    await asyncio.sleep(0)
    sub.publish(2)
    await asyncio.sleep(0)

    assert await c1 == [1, 2]
    assert await c2 == [1, 2]


@pytest.mark.ut
@pytest.mark.asyncio
async def test_no_buffer_message_lost_if_not_subscribed():
    sub = Subscription(asyncio.get_event_loop())

    sub.publish("lost")

    async def consumer():
        async for v in sub:
            return v
        return None

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    sub.publish("next")
    await asyncio.sleep(0)

    assert await task == "next"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_close_causes_termination():
    sub = Subscription(asyncio.get_event_loop())

    async def consumer():
        values = []
        async for v in sub:
            values.append(v)
        return values

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    sub.publish(1)
    await asyncio.sleep(0)

    sub.close()
    await asyncio.sleep(0)

    assert await task == [1]


@pytest.mark.ut
@pytest.mark.asyncio
async def test_multiple_subscribers_stop_on_close():
    sub = Subscription(asyncio.get_event_loop())

    async def consumer():
        async for _ in sub:
            pass

        return True

    c1 = asyncio.create_task(consumer())
    c2 = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    sub.close()
    await asyncio.sleep(0)

    assert await c1 is True
    assert await c2 is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_chain_of_waiters_is_respected():
    sub = Subscription(asyncio.get_event_loop())

    async def consumer():
        results = []
        async for v in sub:
            results.append(v)
            if len(results) == 3:
                return results
        return None

    task = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    sub.publish("a")
    await asyncio.sleep(0)
    sub.publish("b")
    await asyncio.sleep(0)
    sub.publish("c")
    await asyncio.sleep(0)

    assert await task == ["a", "b", "c"]


@pytest.mark.ut
@pytest.mark.asyncio
async def test_concurrent_subscribers_receive_all_messages():
    sub = Subscription(asyncio.get_event_loop())

    async def consumer():
        out = []
        async for v in sub:
            out.append(v)
            if len(out) == 3:
                return out
        return None

    c1 = asyncio.create_task(consumer())
    c2 = asyncio.create_task(consumer())
    await asyncio.sleep(0)

    for x in ["x", "y", "z"]:
        sub.publish(x)
        await asyncio.sleep(0)

    assert await c1 == ["x", "y", "z"]
    assert await c2 == ["x", "y", "z"]
