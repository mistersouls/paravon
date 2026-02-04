import asyncio


class FakeTransport(asyncio.Transport):
    """
    A minimal in-memory implementation of asyncio.Transport
    intended for tests.

    It records written data into an internal buffer and tracks whether the
    transport has been closed. It does not perform any real I/O.
    """

    def __init__(self) -> None:
        super().__init__()
        self._buffer = bytearray()
        self._closed = False
        self._peername = ("127.0.0.1", 9999)

    def get_extra_info(self, name, default=None):
        if name == "peername":
            return self._peername
        return default

    def write(self, data: bytes) -> None:
        if self._closed:
            raise RuntimeError("Cannot write to closed transport")
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data must be bytes-like")
        self._buffer.extend(data)

    def close(self) -> None:
        self._closed = True

    def is_closing(self) -> bool:
        return self._closed

    # Optional helpers for tests
    @property
    def buffer(self) -> bytes:
        return bytes(self._buffer)
