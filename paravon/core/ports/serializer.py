from typing import Protocol, Any


class Serializer(Protocol):
    """
    Defines the interface for encoding/decoding messages exchanged
    over the TCP transport.

    Implementations must be:
    - deterministic
    - pure (no side effects)
    - safe against malformed input
    """

    def serialize(self, message: Any) -> bytes:
        """Encode a Python object into bytes suitable for network transport."""

    def deserialize(self, data: bytes) -> Any:
        """Decode bytes received from the network into a Python object."""
