import asyncio
import struct
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from paravon.core.connections.client import ClientConnection
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.sub import Subscription
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.throttling.backoff import ExponentialBackoff
from paravon.core.models.message import Message


@pytest.fixture
def backoff():
    backoff = ExponentialBackoff()
    backoff.next_delay = MagicMock(return_value=0)
    return backoff


@pytest.mark.ut
@pytest.mark.asyncio
async def test_close_idempotent(para_config, backoff, serializer):
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )
    await client.close()
    await client.close()
    assert client._stopped is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_connect_success_starts_recv_loop(para_config, serializer, backoff):
    reader = asyncio.StreamReader()
    writer = MagicMock()
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )

    with patch("asyncio.open_connection", return_value=(reader, writer)):
        await client.connect()

    assert client.connected is True
    assert client._receive_task is not None


@pytest.mark.ut
@pytest.mark.asyncio
async def test_connect_retries_then_stops(para_config, serializer, backoff):
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )

    with patch("asyncio.open_connection", side_effect=Exception("fail")):
        await client.connect()

    assert client.connected is False



@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_connects_implicitly(para_config, serializer, backoff):
    reader = asyncio.StreamReader()
    writer = MagicMock()
    writer.drain = MagicMock(side_effect=lambda: asyncio.sleep(0))
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )

    with patch("asyncio.open_connection", return_value=(reader, writer)):
        await client.send(Message(type="t", data={"num": 1}))

    writer.write.assert_called_once()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_raises_if_connect_fails(para_config, serializer, backoff):
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )

    with patch("asyncio.open_connection", side_effect=Exception("fail")):
        with pytest.raises(RuntimeError):
            await client.send(Message(type="t", data={"num": 1}))


@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_connection_reset(para_config, serializer, backoff):
    reader = asyncio.StreamReader()
    writer = MagicMock()
    writer.drain = MagicMock(side_effect=ConnectionResetError("reset"))
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )

    with patch("asyncio.open_connection", return_value=(reader, writer)):
        with pytest.raises(ConnectionResetError):
            await client.send(Message(type="t", data={"num": 1}))

    assert client.connected is False


@pytest.mark.ut
@pytest.mark.asyncio
async def test_recv_publishes_message(para_config, serializer, backoff):
    reader = asyncio.StreamReader()
    writer = MagicMock()
    subscription = Subscription(asyncio.get_event_loop())

    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=subscription,
        backoff=backoff,
        max_retries=3,
    )

    msg = {"type": "t", "data": {"num": 123}}
    payload = serializer.serialize(msg)
    frame = struct.pack("!I", len(payload)) + payload

    async def feed():
        reader.feed_data(frame)
        await asyncio.sleep(0)
        reader.feed_eof()

    async def consume():
        async for message in subscription:
            return message
        return None

    with patch("asyncio.open_connection", return_value=(reader, writer)):
        await client.connect()

        consumer_task = asyncio.create_task(consume())
        asyncio.create_task(feed())
        await asyncio.sleep(0)
        received = await consumer_task

    assert isinstance(received, Message)
    assert received.type == "t"
    assert received.data == {"num": 123}



@pytest.mark.ut
@pytest.mark.asyncio
async def test_recv_incomplete_read(para_config, serializer, backoff):
    reader = asyncio.StreamReader()
    writer = MagicMock()
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )

    async def feed():
        reader.feed_data(b"\x00\x00\x00\x05")  # header only
        await asyncio.sleep(0)
        reader.feed_eof()

    with patch("asyncio.open_connection", return_value=(reader, writer)):
        await client.connect()
        asyncio.create_task(feed())
        await asyncio.sleep(0.05)

    assert client.connected is False


@pytest.mark.ut
@pytest.mark.asyncio
async def test_recv_deserialize_none_stops(para_config, backoff, serializer):
    serializer.deserialize = MagicMock(return_value=None)

    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )
    reader = asyncio.StreamReader()
    writer = MagicMock()

    payload = serializer.serialize({"type": "t", "data": {"num": "ABC"}})
    frame = struct.pack("!I", len(payload)) + payload

    async def feed():
        reader.feed_data(frame)
        await asyncio.sleep(0)
        reader.feed_eof()

    with patch("asyncio.open_connection", return_value=(reader, writer)):
        await client.connect()
        asyncio.create_task(feed())
        await asyncio.sleep(0.05)

    assert client.connected is False


@pytest.mark.ut
@pytest.mark.asyncio
async def test_close_stops_recv_task(para_config, serializer, backoff):
    reader = asyncio.StreamReader()
    writer = MagicMock()
    client = ClientConnection(
        address="127.0.0.1:0",
        ssl_context=para_config.get_client_ssl_ctx(),
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        subscription=Subscription(asyncio.get_event_loop()),
        backoff=backoff,
        max_retries=3,
    )

    with patch("asyncio.open_connection", return_value=(reader, writer)):
        await client.connect()
        await client.close()

    assert client._receive_task is None
    assert client.connected is False


@pytest.mark.ut
@pytest.mark.asyncio
async def test_register_new_node(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    await pool.register("n1", "127.0.0.1:9000")
    assert pool.has("n1") is True


@pytest.mark.ut
@pytest.mark.asyncio
async def test_register_same_address_no_close(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    await pool.register("n1", "127.0.0.1:9000")

    fake_conn = AsyncMock()
    pool._connections["n1"] = fake_conn

    await pool.register("n1", "127.0.0.1:9000")
    fake_conn.close.assert_not_called()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_register_changes_address_closes_connection(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    await pool.register("n1", "127.0.0.1:9000")

    fake_conn = AsyncMock()
    pool._connections["n1"] = fake_conn

    await pool.register("n1", "127.0.0.1:9001")
    fake_conn.close.assert_called_once()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_creates_connection(para_config, serializer):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    await pool.register("n1", "127.0.0.1:9000")

    with patch("paravon.core.connections.pool.ClientConnection") as CC:
        instance = MagicMock()
        CC.return_value = instance

        conn = await pool.get("n1")

    assert conn is instance
    CC.assert_called_once()


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_raises_if_not_registered(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    with pytest.raises(KeyError):
        await pool.get("unknown")


@pytest.mark.ut
@pytest.mark.asyncio
async def test_get_raises_if_stopped(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    pool._stopped = True
    with pytest.raises(RuntimeError):
        await pool.get("n1")


@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_calls_connection_send(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    await pool.register("n1", "127.0.0.1:9000")

    fake_conn = AsyncMock()
    pool._connections["n1"] = fake_conn

    msg = Message(type="t", data={"x": 1})
    await pool.send("n1", msg)

    fake_conn.send.assert_called_once_with(msg)


@pytest.mark.ut
@pytest.mark.asyncio
async def test_send_raises_if_stopped(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    pool._stopped = True
    with pytest.raises(RuntimeError):
        await pool.send("n1", Message(type="t", data={}))


@pytest.mark.ut
@pytest.mark.asyncio
async def test_dispatch_calls_handlers(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    called = asyncio.Event()

    class FakeHandler:
        @staticmethod
        async def handle(_):
            called.set()

    pool.subscribe("t", FakeHandler())

    async def produce():
        await asyncio.sleep(0)
        pool._subscription.publish(Message(type="t", data={"a": 1}))
        await asyncio.sleep(0)
        pool._subscription.publish(None)

    stop_event = asyncio.Event()
    consumer = asyncio.create_task(pool.dispatch_forever(stop_event))
    asyncio.create_task(produce())

    await called.wait()
    await consumer


@pytest.mark.ut
@pytest.mark.asyncio
async def test_dispatch_no_handler(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )

    async def produce():
        await asyncio.sleep(0)
        pool._subscription.publish(Message(type="x", data={}))
        await asyncio.sleep(0)
        pool._subscription.publish(None)

    stop_event = asyncio.Event()
    consumer = asyncio.create_task(pool.dispatch_forever(stop_event))
    asyncio.create_task(produce())

    await consumer


@pytest.mark.ut
@pytest.mark.asyncio
async def test_dispatch_stops_on_stop_event(para_config, serializer):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    stop_event = asyncio.Event()

    async def produce():
        await asyncio.sleep(0)
        stop_event.set()
        pool._subscription.publish(Message(type="t", data={}))
        await asyncio.sleep(0)
        pool._subscription.publish(None)

    consumer = asyncio.create_task(pool.dispatch_forever(stop_event))
    asyncio.create_task(produce())

    await consumer


@pytest.mark.ut
@pytest.mark.asyncio
async def test_close_stops_and_closes_connections(para_config, serializer):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    fake1 = AsyncMock()
    fake2 = AsyncMock()

    pool._connections["n1"] = fake1
    pool._connections["n2"] = fake2

    await pool.close()

    assert pool._stopped is True
    fake1.close.assert_called_once()
    fake2.close.assert_called_once()
    assert pool._connections == {}
    assert pool._addresses == {}


@pytest.mark.ut
@pytest.mark.asyncio
async def test_close_idempotent(serializer, para_config):
    pool = ClientConnectionPool(
        serializer=serializer,
        spawner=TaskSpawner(asyncio.get_event_loop()),
        ssl_context=para_config.get_client_ssl_ctx(),
        backoff_factory=lambda: ExponentialBackoff(),
        max_retries=3,
        loop=asyncio.get_event_loop(),
    )
    await pool.close()
    await pool.close()
    assert pool._stopped is True
