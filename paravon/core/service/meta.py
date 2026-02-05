from paravon.core.models.config import PeerConfig
from paravon.core.models.meta import NodePhase, Membership
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

    def __init__(self, peer_config: PeerConfig, system_storage: Storage) -> None:
        self._peer_config = peer_config
        self._system_storage = system_storage
        self._membership: Membership | None = None

    async def get_membership(self) -> Membership:
        """
        Loads the membership information from storage if it has not been
        loaded yet. The method reads both the node identifier and the
        phase from the system namespace.

        If a persisted node identifier exists and differs from the
        configured PeerConfig, the method raises a RuntimeError. This
        protects the invariant that a node cannot change identity once
        initialized and prevents accidental startup with another node's
        configuration.

        The method returns a Membership instance containing the resolved
        node identifier and phase.
        """
        if self._membership is not None:
            return self._membership

        b_node_id = await self._system_storage.get(self._SYS_NAMESPACE, b"node_id")
        b_phase = await self._system_storage.get(self._SYS_NAMESPACE, b"phase")

        node_id = b_node_id.decode() if b_node_id else None
        phase = NodePhase(b_phase.decode()) if b_phase else NodePhase.idle

        if node_id and self._peer_config.node_id != node_id:
            raise RuntimeError(
                f"Persisted node_id '{node_id}' does not match configured node_id "
                f"'{self._peer_config.node_id}'. A node cannot change identity once "
                f"initialized, and this indicates the node is starting with the "
                f"configuration of another node."
            )

        node_id = node_id or self._peer_config.node_id
        await self._system_storage.put(
            self._SYS_NAMESPACE, b"node_id", node_id.encode()
        )
        await self._system_storage.put(
            self._SYS_NAMESPACE, b"phase", phase.value.encode()
        )
        self._membership = Membership(
            node_id=node_id,
            phase=phase
        )
        return self._membership

    async def set_phase(self, phase: NodePhase) -> None:
        """
        Persists the provided phase under the 'phase' key and updates the
        in‑memory membership object. If the membership has not been loaded
        yet, the method triggers its initialization.
        """
        await self._system_storage.put(
            self._SYS_NAMESPACE, b"phase", phase.value.encode()
        )

        if self._membership:
            self._membership.phase = phase
        else:
            await self.get_membership()
