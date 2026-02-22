import socket
import ssl
import struct
from typing import Any

from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer


class ParavonClient:
    """
    Synchronous TCP client for Paravon.
    Uses MsgPack serialization and a length-prefixed frame:

        [4-byte big-endian length][payload]

    This client is minimal and blocking. It is intended for CLI usage,
    debugging, and simple scripts.
    """
    def __init__(self, host: str, port: int, ssl_ctx: ssl.SSLContext, serializer: Serializer):
        self._host = host
        self._port = port
        self._ssl_ctx = ssl_ctx
        self._serializer = serializer
        self._sock: socket.socket | None = None

    def connect(self) -> None:
        if self._sock is not None:
            return

        raw_sock = socket.create_connection((self._host, self._port))
        self._sock = self._ssl_ctx.wrap_socket(raw_sock, server_hostname=self._host)

    def close(self) -> None:
        if self._sock:
            try:
                self._sock.close()
            finally:
                self._sock = None

    def send(self, message: Message) -> None:
        if not self._sock:
            self.connect()

        payload = self._serializer.serialize(message.to_dict())
        frame = struct.pack("!I", len(payload)) + payload

        self._sock.sendall(frame)

    def recv(self) -> Message:
        if not self._sock:
            self.connect()

        # Read exactly 4 bytes
        header = self._recv_exact(4)
        length = struct.unpack("!I", header)[0]

        # Read payload
        payload = self._recv_exact(length)
        raw = self._serializer.deserialize(payload)

        try:
            return Message(**raw)
        except Exception as ex:
            raise ValueError(f"Invalid message format: {raw}") from ex

    def _recv_exact(self, n: int) -> bytes:
        """Blocking read of exactly n bytes."""
        buf = b""
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("Connection closed by peer")
            buf += chunk
        return buf

    def request(self, message: Message) -> Message:
        self.send(message)
        return self.recv()
