import asyncio
import json
import base64
from typing import Any

from paravon.core.models.membership import Membership


class FakeJSONEncoder(json.JSONEncoder):
    """
    JSON encoder that supports:
    - bytes (encoded as base64)
    - objects exposing to_dict()
    """

    def default(self, obj):
        if isinstance(obj, bytes):
            return {"__bytes__": base64.b64encode(obj).decode()}
        return super().default(obj)


class JsonSerializer:
    @staticmethod
    def hook_factory(classes: dict[str, type]):
        def hook(obj):
            if "__bytes__" in obj:
                return base64.b64decode(obj["__bytes__"])

            return obj

        return hook

    @staticmethod
    def serialize(obj) -> bytes:
        return json.dumps(obj, cls=FakeJSONEncoder).encode()

    @classmethod
    def deserialize(cls, data) -> Any:
        return json.loads(
            data.decode(),
            object_hook=cls.hook_factory({"Membership": Membership}),
        )


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
