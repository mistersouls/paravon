import asyncio
import struct
import pytest

from paravon.core.transport.server import MessageServer
from paravon.core.models.config import ServerConfig
from paravon.core.models.message import Message
from tests.conftest import serializer


@pytest.mark.it
@pytest.mark.asyncio
async def test_one_client(tmp_path, serializer, mtls_contexts):
    received_messages: list[Message] = []
    server_ctx, client_ctx = mtls_contexts

    async def app(receive, send):
        # Read a single message then exit
        msg = await receive()
        received_messages.append(msg)


    config = ServerConfig(
        app=app,
        host="127.0.0.1",
        port=0,
        backlog=10,
        ssl_ctx=server_ctx,
        max_buffer_size=1024,
        timeout_graceful_shutdown=1.0,
    )

    loop = asyncio.get_event_loop()
    server = MessageServer(config=config, serializer=serializer, loop=loop)
    await server.start()

    sockets = server._server.sockets  # type: ignore[union-attr]
    assert sockets
    port = sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection(config.host, port, ssl=client_ctx)

    payload = serializer.serialize({"type": "greeting", "data": {"message": "hello"}})
    frame = struct.pack("!I", len(payload)) + payload
    writer.write(frame)
    await writer.drain()

    writer.close()
    await writer.wait_closed()

    await server.shutdown()

    assert len(received_messages) == 1
    msg = received_messages[0]
    assert isinstance(msg, Message)
    assert msg.type == "greeting"
    assert msg.data == {"message": "hello"}


@pytest.mark.it
@pytest.mark.asyncio
async def test_multiple_clients(serializer, mtls_contexts):
    server_ctx, client_ctx = mtls_contexts
    received = []

    async def app(receive, send):
        msg = await receive()
        received.append(msg)

    config = ServerConfig(
        app=app,
        host="127.0.0.1",
        port=0,
        backlog=10,
        ssl_ctx=server_ctx,
        max_buffer_size=1024,
        timeout_graceful_shutdown=1.0,
    )

    loop = asyncio.get_event_loop()
    server = MessageServer(config, serializer, loop)
    await server.start()

    port = server._server.sockets[0].getsockname()[1]

    clients = []
    for letter in ["a", "b", "c"]:
        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx)
        payload = serializer.serialize({"type": "alphabet", "data": {"letter": letter}})
        frame = struct.pack("!I", len(payload)) + payload
        writer.write(frame)
        await writer.drain()
        clients.append(writer)

    for w in clients:
        w.close()
        await w.wait_closed()

    await server.shutdown()

    assert len(received) == 3
    assert sorted([m.data["letter"] for m in received]) == ["a", "b", "c"]


@pytest.mark.it
@pytest.mark.asyncio
async def test_shutdown_with_open_connection(serializer, mtls_contexts):
    server_ctx, client_ctx = mtls_contexts

    async def app(receive, send):
        await receive()  # ignore

    config = ServerConfig(
        app=app,
        host="127.0.0.1",
        port=0,
        backlog=10,
        ssl_ctx=server_ctx,
        max_buffer_size=1024,
        timeout_graceful_shutdown=1.0,
    )

    loop = asyncio.get_event_loop()
    server = MessageServer(config, serializer, loop)
    await server.start()

    port = server._server.sockets[0].getsockname()[1]

    await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx)

    await server.shutdown()

    assert len(server.state.connections) == 0


@pytest.mark.it
@pytest.mark.asyncio
async def test_shutdown_timeout_cancels_tasks(serializer, mtls_contexts):
    server_ctx, client_ctx = mtls_contexts
    task_started = asyncio.Event()

    async def app(receive, send):
        async def long_task():
            task_started.set()
            await asyncio.sleep(999)

        # On crée une task longue et on l’enregistre dans server.state.tasks
        t = asyncio.create_task(long_task())
        server.state.tasks.add(t)

        # On consomme le message pour débloquer Streamer
        await receive()

    config = ServerConfig(
        app=app,
        host="127.0.0.1",
        port=0,
        backlog=10,
        ssl_ctx=server_ctx,
        max_buffer_size=1024,
        timeout_graceful_shutdown=0.2,
    )

    loop = asyncio.get_event_loop()
    server = MessageServer(config, serializer, loop)
    await server.start()

    port = server._server.sockets[0].getsockname()[1]

    reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx)
    frame = struct.pack("!I", 1) + b"x"
    writer.write(frame)
    await writer.drain()

    await task_started.wait()

    await server.shutdown()
    await asyncio.sleep(0)
    assert len(server.state.tasks) == 1
    assert all(t.cancelled() for t in server.state.tasks)


@pytest.mark.it
@pytest.mark.asyncio
async def test_protocol_error_closes_connection(monkeypatch, serializer, mtls_contexts):
    server_ctx, client_ctx = mtls_contexts

    async def app(receive, send):
        pass

    config = ServerConfig(
        app=app,
        host="127.0.0.1",
        port=0,
        backlog=10,
        ssl_ctx=server_ctx,
        max_buffer_size=1024,
        timeout_graceful_shutdown=1.0,
    )

    loop = asyncio.get_event_loop()
    server = MessageServer(config, serializer, loop)

    await server.start()
    port = server._server.sockets[0].getsockname()[1]

    def boom(self, data):
        raise RuntimeError("boom")

    monkeypatch.setattr("paravon.core.transport.protocol.Protocol.data_received", boom)

    reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=client_ctx)

    writer.write(struct.pack("!I", 1) + b"x")
    await writer.drain()

    await asyncio.sleep(0.1)

    await server.shutdown()

    assert len(server.state.connections) == 0


@pytest.mark.it
@pytest.mark.asyncio
async def test_shutdown_without_start(serializer, mtls_contexts):
    server_ctx, _ = mtls_contexts

    async def app(receive, send):
        pass

    config = ServerConfig(
        app=app,
        host="127.0.0.1",
        port=0,
        backlog=10,
        ssl_ctx=server_ctx,
        max_buffer_size=1024,
        timeout_graceful_shutdown=1.0,
    )

    server = MessageServer(config, serializer)

    await server.shutdown()
