import asyncio
import logging

from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.gossip.table import BucketTable
from paravon.core.models.membership import Membership
from paravon.core.models.message import Message
from paravon.core.models.state import PeerState
from paravon.core.ports.serializer import Serializer
from paravon.core.service.meta import NodeMetaManager
from paravon.core.throttling.ratelimiter import RateLimiter


class Gossiper:
    def __init__(
        self,
        peer_state: PeerState,
        serializer: Serializer,
        meta_manager: NodeMetaManager,
        peer_clients: ClientConnectionPool
    ) -> None:
        self._peer_state = peer_state
        self._serializer = serializer
        self._meta_manager = meta_manager
        self._peer_clients = peer_clients
        self._inflight = asyncio.Semaphore(32)
        self._lock = asyncio.Lock()
        self._table = BucketTable(
            total_buckets=128,
            serializer=serializer,
            meta_manager=self._meta_manager,
            delta=5
        )
        self._logger = logging.getLogger("core.gossip.gossiper")

    async def run(
        self,
        stop_event: asyncio.Event,
        rate_limiter: RateLimiter
    ) -> None:
        membership = await self._meta_manager.get_membership()
        self._peer_clients.subscribe("gossip/buckets", self)
        await self._table.add_or_update(membership)
        await self.gossip_loop(stop_event, rate_limiter)

    @staticmethod
    async def handle(message: Message) -> None:
        # stub
        print(message)

    async def gossip_loop(
        self,
        stop_event: asyncio.Event,
        rate_limiter: RateLimiter
    ) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=rate_limiter.delay
                )
            except asyncio.TimeoutError:
                pass

            peer = await self.pick_random_peer()
            if peer is None:
                continue

            if self._inflight.locked():
                rate_limiter.on_error()
            else:
               self._peer_state.spawner.spawn(self._attempt_gossip(peer, rate_limiter))

    async def pick_random_peer(self) -> Membership | None:
        peer = self._table.peek_random_member()
        membership = await self._meta_manager.get_membership()

        if peer is None:
            return None
        if peer.node_id == membership.node_id:
            return None

        return peer

    async def send_checksums(self, peer: Membership) -> None:
        async with self._inflight:
            self._logger.debug(f"Gossiping peer {peer.node_id}")
            source = await self._meta_manager.get_membership()
            checksums = self._table.get_checksums()
            data = {
                "source": source.to_dict(),
                "checksums": checksums
            }
            message = Message(type="gossip/checksums", data=data)
            await self._send_message(peer, message)
            self._logger.debug(f"Sent gossip checksums to {peer.node_id}")

    async def _attempt_gossip(self, peer, rate_limiter) -> None:
        try:
            await self.send_checksums(peer)
            rate_limiter.on_success()
        except Exception as ex:
            self._logger.error(f"Failed to gossip {peer.node_id}: {ex}")
            rate_limiter.on_error()

    async def _send_message(self, peer: Membership, message: Message) -> None:
        node_id = peer.node_id
        address = peer.peer_address
        if not self._peer_clients.has(node_id):
            await self._peer_clients.register(node_id, address)
        await self._peer_clients.send(node_id, message)
