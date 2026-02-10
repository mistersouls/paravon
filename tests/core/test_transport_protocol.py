import asyncio
import ssl
import struct
import pytest
from unittest.mock import Mock, AsyncMock

from paravon.core.transport.protocol import Protocol
from paravon.core.models.config import ServerConfig
from paravon.core.models.state import ServerState
from paravon.core.models.message import Message
from paravon.core.transport.flow import FlowControl


@pytest.fixture
def server_state():
    return ServerState()


@pytest.fixture
def config():
    return ServerConfig(
        app=AsyncMock(),
        host="1.1.1.1",
        port=1234,
        backlog=100,
        ssl_ctx=ssl.create_default_context(),
        max_buffer_size=1024,
    )


@pytest.mark.ut
@pytest.mark.asyncio
async def test_connection_made_initializes_everything(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    assert proto._transport is transport
    assert isinstance(proto._flow, FlowControl)
    assert proto in server_state.connections
    assert len(server_state.tasks) == 1
    assert proto._streamer is not None


@pytest.mark.ut
@pytest.mark.asyncio
async def test_connection_lost_removes_connection_and_closes_transport(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    proto.connection_lost(exc=None)

    assert proto not in server_state.connections
    assert transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_data_received_single_complete_frame(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    # Replace queue with a mock to inspect calls
    proto._streamer.queue = Mock()
    proto._streamer.queue.put_nowait = Mock()

    payload = serializer.serialize({"type": "greeting", "data": {"message": "hello"}})
    frame = struct.pack("!I", len(payload)) + payload

    proto.data_received(frame)

    proto._streamer.queue.put_nowait.assert_called_once()
    msg = proto._streamer.queue.put_nowait.call_args[0][0]
    assert isinstance(msg, Message)
    assert msg.type == "greeting"


@pytest.mark.ut
@pytest.mark.asyncio
async def test_data_received_fragmented_frame(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    proto._streamer.queue = Mock()
    proto._streamer.queue.put_nowait = Mock()

    payload = b"hello"
    frame = struct.pack("!I", len(payload)) + payload

    proto.data_received(frame[:2])
    proto.data_received(frame[2:])

    proto._streamer.queue.put_nowait.assert_called_once()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_data_received_multiple_frames(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    proto._streamer.queue = Mock()
    proto._streamer.queue.put_nowait = Mock()

    p1 = b"a"
    p2 = b"bbb"

    frame = (
        struct.pack("!I", len(p1)) + p1 +
        struct.pack("!I", len(p2)) + p2
    )

    proto.data_received(frame)

    assert proto._streamer.queue.put_nowait.call_count == 2


@pytest.mark.ut
@pytest.mark.asyncio
async def test_data_received_message_too_large_closes_connection(config, server_state, serializer, transport):
    config.max_buffer_size = 10

    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    payload = b"x" * 20
    frame = struct.pack("!I", len(payload)) + payload

    proto.data_received(frame)

    assert transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_data_received_buffer_overflow(config, server_state, serializer, transport):
    config.max_buffer_size = 5

    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    proto.data_received(b"123456")  # overflow

    assert transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_data_received_invalid_frame_is_ko(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    proto._streamer.queue = Mock()
    proto._streamer.queue.put_nowait = Mock()
    mock_put_nowait = proto._streamer.queue.put_nowait

    payload = b"hello"
    frame = struct.pack("!I", len(payload)) + payload

    proto.data_received(frame)

    mock_put_nowait.assert_called_once()
    msg = mock_put_nowait.call_args.args[0]
    assert msg == Message(type="ko", data={"message": "Invalid frame format"})


@pytest.mark.ut
@pytest.mark.asyncio
async def test_pause_resume_writing(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    proto._flow = Mock()

    proto.pause_writing()
    proto._flow.pause_writing.assert_called_once()

    proto.resume_writing()
    proto._flow.resume_writing.assert_called_once()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_shutdown_closes_transport(config, server_state, serializer, transport):
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    proto.shutdown()

    assert transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_transport_rejects_frame_too_large(monkeypatch, serializer, transport):
    # Arrange
    config = Mock(max_buffer_size=10)
    server_state = Mock()

    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    # Frame length = 9999 > max_buffer_size
    big_frame_header = struct.pack("!I", 9999)

    # Act
    proto.data_received(big_frame_header)
    await asyncio.sleep(0)

    assert transport.is_closing()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_transport_waits_for_full_frame(monkeypatch, serializer, transport):
    config = Mock(max_buffer_size=1024)
    server_state = Mock()

    serializer.deserialize = Mock()
    proto = Protocol(config, server_state, serializer)
    proto.connection_made(transport)

    header = struct.pack("!I", 5)
    proto.data_received(header)

    proto.data_received(b"he")

    assert proto._buffer == b"he"
    assert proto._expected_length == 5
    serializer.deserialize.assert_not_called()
