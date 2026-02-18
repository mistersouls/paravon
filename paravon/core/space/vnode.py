from collections.abc import Iterable
from typing import Self


class VNode(tuple):
    """
    VNode represents a virtual node on the 128-bit hash ring.

    The token defines the vnode's position on the ring and is immutable.
    The node_id identifies the physical node currently responsible for this
    vnode and may change during rebalancing or scaling operations.
    """

    __slots__ = ()

    def __new__(cls, node_id: str, token: int) -> Self:
        return super().__new__(cls, (node_id, token))

    @property
    def node_id(self) -> str:
        """The identifier of the physical node currently owning this vnode."""
        return self[0]

    @property
    def token(self) -> int:
        """The immutable 128-bit token of this vnode."""
        return self[1]

    @classmethod
    def vnodes_for(cls, node_id: str, tokens: Iterable[int]) -> list[VNode]:
        return [cls(node_id, token) for token in tokens]

    def repr_token(self) -> str:
        int_token = str(self.token)
        if len(int_token) >= 12:
            int_token = f"{int_token[:6]}...{int_token[-6:]}"
        hex_token = f"{self.token:032x}"[:12]
        return f"Token(hash={int_token}, hex={hex_token})"

    def __repr__(self) -> str:
        return f"VNode(node_id={self.node_id}, token={self.repr_token()})"
