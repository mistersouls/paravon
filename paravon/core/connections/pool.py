import asyncio
import logging
import ssl
from collections import defaultdict
from typing import Callable

from paravon.core.connections.client import ClientConnection
from paravon.core.connections.handler import MessageHandler
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.helpers.sub import Subscription
from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer
from paravon.core.throttling.backoff import ExponentialBackoff


class ClientConnectionPool:
    """
    Manages persistent TCP connections keyed by node_id and dispatches all
    incoming messages to registered handlers.

    The pool maintains a single global Subscription that aggregates messages
    from all ClientConnection instances. Services can register handlers for
    specific message types via `subscribe(msg_type, handler)`.

    The dispatch loop (`run_dispatch_loop`) consumes the global subscription
    and forwards each message to the appropriate handlers. Handlers are
    executed asynchronously and independently to avoid blocking the dispatch
    pipeline.

    The `stopped` flag prevents creation of new connections or dispatch after
    shutdown. Closing the pool terminates all active connections, closes the
    subscription, and stops the dispatch loop.
    """

    def __init__(
        self,
        serializer: Serializer,
        spawner: TaskSpawner,
        ssl_context: ssl.SSLContext,
        backoff_factory: Callable[[], ExponentialBackoff] = lambda: ExponentialBackoff(),
        max_retries: int = 10,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        if loop is None:
            loop = asyncio.get_event_loop()

        self._serializer = serializer
        self._spawner = spawner
        self._ssl_context = ssl_context
        self._backoff_factory = backoff_factory
        self._max_retries = max_retries
        self._subscription = Subscription(loop)

        self._addresses: dict[str, str] = {}
        self._connections: dict[str, ClientConnection] = {}
        self._handlers: dict[str, list[MessageHandler]] = defaultdict(list)

        self._stopped = False
        self._lock = asyncio.Lock()

        self._logger = logging.getLogger("core.connections.pool")

    async def close(self) -> None:
        """
        Shut down the pool and close all active connections.

        After shutdown, no new connections can be created and no new messages
        will be dispatched. The global subscription is closed, causing the
        dispatch loop to exit naturally.
        """
        if self._stopped:
            return

        self._stopped = True

        # Stop the dispatch loop
        self._subscription.publish(None)

        # Close all active connections
        tasks = [conn.close() for conn in self._connections.values()]
        await asyncio.gather(*tasks, return_exceptions=True)

        self._connections.clear()
        self._addresses.clear()

    async def get(self, node_id: str) -> ClientConnection:
        """
        Retrieve or create the persistent connection for the given node_id.

        The node_id must have been registered beforehand. Creation is blocked
        once the pool is stopped.
        """
        if self._stopped:
            raise RuntimeError("Connection pool is shut down")

        async with self._lock:
            if node_id not in self._addresses:
                raise KeyError(f"Node {node_id} is not registered")

            conn = self._connections.get(node_id)
            if conn is None:
                address = self._addresses[node_id]
                conn = ClientConnection(
                    address=address,
                    ssl_context=self._ssl_context,
                    serializer=self._serializer,
                    spawner=self._spawner,
                    backoff=self._backoff_factory(),
                    max_retries=self._max_retries,
                    subscription=self._subscription,
                )
                self._connections[node_id] = conn

            return conn

    def has(self, node_id: str) -> bool:
        """
        Return True if the pool knows about the given node_id.

        This checks only whether the node_id has been registered, not whether
        a connection is currently active.
        """
        return node_id in self._addresses

    async def dispatch_forever(self, stop_event: asyncio.Event) -> None:
        """
        Consume the global message stream and dispatch each message to the
        handlers registered for its type.

        The loop stops when:
        - the subscription yields None (pool shutdown), or
        - the stop_event is set.
        """
        if self._stopped:
            raise RuntimeError("Connection pool is shut down")

        async for msg in self._subscription:
            if msg is None or stop_event.is_set():
                break

            handlers = self._handlers.get(msg.type, [])
            tasks = [handler.handle(msg) for handler in handlers]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            else:
                # yield event loop
                self._logger.warning(f"No handler registered for {msg.type}: {msg}")
                await asyncio.sleep(0)

    async def register(self, node_id: str, address: str) -> None:
        """
        Register or update the network address for a node_id.

        If the address changes, the existing connection is closed and removed.
        The next call to `get()` or `send()` will create a new connection.
        """
        async with self._lock:
            old = self._addresses.get(node_id)

            if old is None:
                self._addresses[node_id] = address
                return

            if old == address:
                return

            self._addresses[node_id] = address
            conn = self._connections.pop(node_id, None)
            if conn is not None:
                await conn.close()

    async def send(self, node_id: str, message: Message) -> None:
        """
        Send a message to the node identified by node_id.

        The connection is created lazily if needed. Sending is blocked once
        the pool is stopped.
        """
        if self._stopped:
            raise RuntimeError("Connection pool is shut down")

        conn = await self.get(node_id)
        await conn.send(message)

    def subscribe(self, msg_type: str, handler: MessageHandler) -> None:
        """
        Register a handler for a specific message type.

        The handler will be invoked asynchronously for every incoming message
        of that type. Multiple handlers may be registered for the same type.
        """
        self._handlers[msg_type].append(handler)
