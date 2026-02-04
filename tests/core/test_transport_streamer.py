import asyncio
import pytest
from unittest.mock import Mock, AsyncMock

from paravon.core.transport.stream import Streamer
from paravon.core.models.message import Message
from paravon.core.transport.flow import FlowControl


@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_writes_to_transport(transport, serializer):
    queue = asyncio.Queue()

    flow = Mock(spec=FlowControl)
    flow.write_paused = False
    flow.drain = AsyncMock(return_value=None)

    streamer = Streamer(transport, flow, serializer, queue)

    msg = Message(type="ping", data={"a": 1})
    await streamer.send(msg)

    assert transport.buffer.startswith(b"X-")
    assert not transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_waits_for_flow_control(transport, serializer):
    queue = asyncio.Queue()

    flow = Mock(spec=FlowControl)
    flow.write_paused = True

    async def unblock():
        flow.write_paused = False

    flow.drain = AsyncMock(side_effect=unblock)

    streamer = Streamer(transport, flow, serializer, queue)

    msg = Message(type="ping", data={})
    await streamer.send(msg)

    flow.drain.assert_awaited_once()
    assert transport.buffer.startswith(b"X-")


@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_closes_transport_on_serialize_error(transport):
    queue = asyncio.Queue()

    class BadSerializer:
        @staticmethod
        def serialize(obj):
            raise ValueError("boom")

        @staticmethod
        def deserialize(obj):
            raise ValueError("boom")

    serializer = BadSerializer()

    flow = Mock(spec=FlowControl)
    flow.write_paused = False
    flow.drain = AsyncMock(return_value=None)

    streamer = Streamer(transport, flow, serializer, queue)

    msg = Message(type="x", data={})
    await streamer.send(msg)

    assert transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_receive_returns_next_message(transport, serializer):
    queue = asyncio.Queue()

    flow = Mock(spec=FlowControl)
    flow.write_paused = False
    flow.drain = AsyncMock(return_value=None)

    streamer = Streamer(transport, flow, serializer, queue)

    msg = Message(type="pong", data={"x": 2})
    await queue.put(msg)

    received = await streamer.receive()
    assert received == msg


@pytest.mark.ut
@pytest.mark.asyncio
async def test_run_app_closes_transport_on_normal_exit(transport, serializer):
    async def app(receive, send):
        return

    queue = asyncio.Queue()

    flow = Mock(spec=FlowControl)
    flow.write_paused = False
    flow.drain = AsyncMock(return_value=None)

    streamer = Streamer(transport, flow, serializer, queue)

    await streamer.run_app(app)

    assert transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_run_app_closes_transport_on_exception(transport, serializer):
    async def app(receive, send):
        raise RuntimeError("boom")

    queue = asyncio.Queue()

    flow = Mock(spec=FlowControl)
    flow.write_paused = False
    flow.drain = AsyncMock(return_value=None)

    streamer = Streamer(transport, flow, serializer, queue)

    await streamer.run_app(app)

    assert transport.is_closing()
