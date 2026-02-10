from typing import Any

from paravon.core.models.config import PeerConfig
from paravon.core.models.membership import NodePhase, Membership, NodeSize
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import Storage


class NodeMetaManager:
    """
    Manages the persisted metadata associated with a node.
    The metadata is stored in a dedicated system namespace inside
    the provided Storage instance.

    The manager loads the membership lazily and caches it. If a persisted
    node identifier exists, it must match the configured PeerConfig. A
    mismatch may indicate that the node is attempting to start with the
    configuration of another node. Because a node's identity is immutable
    once initialized, this situation is treated as a fatal error and the
    manager raises a RuntimeError.
    """
    _SYS_NAMESPACE = b"system"

    def __init__(self, peer_config: PeerConfig, system_storage: Storage, serializer: Serializer) -> None:
        self._peer_config = peer_config
        self._system_storage = system_storage
        self._serializer = serializer
        self._membership: Membership | None = None
        self._incarnation: int | None = None

    async def bump_epoch(self) -> int:
        """
        Increment and persist the local membership epoch.

        The epoch is a per‑node, monotonically increasing counter that tracks
        updates to this node's membership record (phase changes, token updates,
        capacity changes, etc.). Gossip uses this value to determine which
        version of a node's membership is newer.

        If the membership has not been initialized yet, this method creates
        the initial epoch and triggers membership initialization.
        """
        if self._membership is None:
            epoch = 1
            await self._put("epoch", epoch)
            await self._init_membership()
        else:
            epoch = self._membership.epoch + 1
            await self._put("epoch", epoch)
            self._membership.epoch = epoch

        return epoch

    async def bump_incarnation(self) -> int:
        """
        Increment and persist the global ring incarnation number.

        The incarnation acts as a fencing token for the entire ring. Any gossip
        state carrying an older incarnation is ignored, ensuring that outdated
        membership information cannot be reintroduced after global operations
        such as node removal, ring resets, or large‑scale rebalancing.
        """
        incarnation = await self.get_incarnation()
        incarnation += 1
        await self._put("incarnation", incarnation)
        return incarnation

    async def get_incarnation(self) -> int:
        """
        Return the persisted ring incarnation, loading it from storage if needed.

        The incarnation is cached in memory after the first read. If no value
        exists in storage, the method returns 0, representing the initial
        incarnation of the ring.
        """
        if self._incarnation is None:
            self._incarnation = await self._get("incarnation", 0)
        return self._incarnation

    async def get_membership(self) -> Membership:
        """
        Loads the membership information from storage if it has not been
        loaded yet. The method reads the memberships meta from
        the system namespace.

        If a persisted node identifier exists and differs from the
        configured PeerConfig, the method raises a RuntimeError. This
        protects the invariant that a node cannot change identity once
        initialized and prevents accidental startup with another node's
        configuration.
        """
        if self._membership is None:
            self._membership = await self._init_membership()
        return self._membership

    async def set_incarnation(self, inc: int) -> None:
        await self._put("incarnation", inc)
        if self._membership:
            self._membership.incarnation = inc
        else:
            await self._init_membership()

    async def set_phase(self, phase: NodePhase) -> None:
        """
        Persists the provided phase under the 'phase' key and updates the
        in‑memory membership object. If the membership has not been loaded
        yet, the method triggers its initialization.
        """
        await self._put("phase", phase.name)

        if self._membership:
            self._membership.phase = phase
        else:
            await self._init_membership()

    async def set_tokens(self, tokens: list[int]) -> None:
        """
        Persists the provided tokens under the 'tokens' key and updates the
        in‑memory membership object. If the membership has not been loaded
        yet, the method triggers its initialization.
        """
        await self._put("tokens", Membership.tokens_bytes(tokens))

        if self._membership:
            self._membership.tokens = tokens
        else:
            await self._init_membership()

    async def _init_membership(self) -> Membership:
        epoch = await self._get("epoch", 0)
        incarnation = await self._get("incarnation", 0)
        stored_node_id = await self._get("node_id")
        node_id = await self._validate_node_id(stored_node_id)
        size_name = await self._get("size")
        size = await self._validate_size(size_name)
        phase = NodePhase(await self._get("phase", "idle"))
        tokens = Membership.tokens_from(await self._get("tokens", []))
        peer_address = self._get_peer_address()

        return Membership(
            epoch=epoch,
            incarnation=incarnation,
            node_id=node_id,
            size=size,
            phase=phase,
            tokens=tokens,
            peer_address=peer_address
        )

    async def _get(self, key: str, default: Any = None) -> Any:
        value = await self._system_storage.get(self._SYS_NAMESPACE, key.encode())
        if value is not None:
            return self._serializer.deserialize(value)
        return default

    async def _put(self, key: str, value: Any) -> None:
        await self._system_storage.put(
            self._SYS_NAMESPACE,
            key.encode(),
            self._serializer.serialize(value)
        )

    async def _validate_node_id(self, node_id: str | None) -> str:
        if node_id is None:
            node_id = self._peer_config.node_id
            await self._put("node_id", node_id)

        if node_id != self._peer_config.node_id:
            raise RuntimeError(
                f"Persisted node_id '{node_id}' does not match configured node_id "
                f"'{self._peer_config.node_id}'. A node cannot change identity once "
                f"initialized, and this may indicate the node is starting with the "
                f"configuration of another node."
            )

        return node_id

    async def _validate_size(self, size_name: str | None) -> NodeSize:
        size = self._peer_config.node_size
        if size_name is not None:
            stored_size = NodeSize[size_name]
            if stored_size != size:
                raise RuntimeError(
                    f"Persisted node.size '{stored_size.name}' does not match "
                    f"configured node.size '{size.name}'. Changing a node's "
                    f"capacity class after initialization is not supported."
                )
        else:
            await self._put("size", size.name)

        return size

    def _get_peer_address(self) -> str:
        listener = self._peer_config.peer_listener
        if listener is None:
            host = self._peer_config.host
            port = self._peer_config.port
            listener = f"{host}:{port}"

        return listener
