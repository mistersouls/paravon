import ssl
from dataclasses import dataclass

from paravon.core.models.membership import NodeSize
from paravon.core.transport.application import Application


@dataclass
class ServerConfig:
    """
    Static configuration for a Paravon MessageServer.

    This structure defines all parameters required to start a server:
    networking, TLS, resource limits, and graceful shutdown behavior.
    """
    app: Application
    """
    The user-defined application coroutine with the signature:
        async def app(receive, send)
    It receives decoded messages and may send responses.
    """

    host: str
    """
    IP address or hostname on which the server listens.
    """

    port: int
    """
    TCP port to bind. If set to 0, the OS selects an available port.
    """

    backlog: int
    """
    Maximum number of pending TCP connections waiting for accept().
    """

    ssl_ctx: ssl.SSLContext
    """
    TLS context used to secure incoming connections.
    Paravon uses mutual TLS: the server requires a valid client certificate.
    """

    limit_concurrency: int = 1024
    """
    Maximum number of concurrent active connections allowed.
    """

    max_buffer_size: int = 4 * 1024 * 1024  # 4MB
    """
    Maximum size of the per-connection receive buffer.
    Protects against malformed frames or memory exhaustion attacks.
    """

    max_message_size: int = 1 * 1024 * 1024  # 1MB
    """
    Maximum size of a decoded message payload.
    """

    timeout_graceful_shutdown: float = 5.0
    """
    Maximum time (in seconds) allowed for graceful shutdown:
    - active connections must close
    - background tasks registered in ServerState.tasks must complete
    After this timeout, remaining tasks are cancelled.
    """


@dataclass(kw_only=True)
class PeerConfig(ServerConfig):
    node_id: str
    node_size: NodeSize
    seeds: set[str]
