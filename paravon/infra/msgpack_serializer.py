import msgpack
from typing import Any

from paravon.core.ports.serializer import Serializer


class MsgPackSerializer(Serializer):
    """
    MsgPack-based implementation of the Serializer interface.

    - deterministic binary encoding
    - compact
    - fast
    - widely used in distributed systems
    """
    def serialize(self, message: Any) -> bytes:
        return msgpack.packb(message, use_bin_type=True)

    def deserialize(self, data: bytes) -> Any:
        return msgpack.unpackb(data, raw=False)
