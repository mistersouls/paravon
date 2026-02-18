import asyncio
import logging

from paravon.core.connections.client import ClientConnection
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.membership import Membership
from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.topology import TopologyManager
from paravon.core.throttling.ratelimiter import RateLimiter


class Gossiper:
    def __init__(
        self,
        spawner: TaskSpawner,
        serializer: Serializer,
        topology_manager: TopologyManager,
        meta_manager: NodeMetaManager,
        peer_clients: ClientConnectionPool
    ) -> None:
        self._spawner = spawner
        self._serializer = serializer
        self._topology = topology_manager
        self._meta_manager = meta_manager
        self._peer_clients = peer_clients
        self._inflight = asyncio.Semaphore(32)
        self._logger = logging.getLogger("core.gossip.gossiper")

    async def run(
        self,
        stop_event: asyncio.Event,
        rate_limiter: RateLimiter
    ) -> None:
        self._peer_clients.subscribe("gossip/checksums", self)
        self._peer_clients.subscribe("gossip/bucket", self)
        await self.gossip_loop(stop_event, rate_limiter)

    async def apply_bucket(self, data: dict) -> dict[str, Membership]:
        bucket_id = data["bucket_id"]
        r_memberships = [
            Membership.from_dict(m)
            for m in data["memberships"].values()
        ]
        l_memberships = await self._topology.get_bucket_memberships(bucket_id)
        await self._topology.apply_bucket(bucket_id, r_memberships)
        return l_memberships

    async def apply_checksums(self, data: dict) -> dict[str, int]:
        local = await self._topology.get_checksums()
        remote_checksums = data["checksums"]
        peer = Membership.from_dict(data["source"])
        client = await self._get_client(peer)

        for bucket_id, remote_crc in remote_checksums.items():
            local_crc = local.get(bucket_id)
            if local_crc != remote_crc:
                if remote_crc == 0:
                    self._logger.debug(
                        f"Remote checksum for bucket {bucket_id} "
                        f"is empty from {peer.node_id}"
                    )
                    await self._topology.apply_bucket(bucket_id, [])
                else:
                    self._logger.debug(
                        f"Checksum for bucket {bucket_id} is different "
                        f"from {peer.node_id}, schedule requesting memberships"
                    )
                    self._spawner.spawn(self.request_bucket(client, bucket_id))

        return local

    async def handle(self, message: Message) -> None:
        match message.type:
            case "gossip/checksums":
                await self.apply_checksums(message.data)
            case "gossip/bucket":
                await self.apply_bucket(message.data)

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
               self._spawner.spawn(self._attempt_gossip(peer, rate_limiter))

    async def pick_random_peer(self) -> Membership | None:
        peer = await self._topology.pick_random_membership()
        membership = await self._meta_manager.get_membership()

        if peer is None:
            return None
        if peer.node_id == membership.node_id:
            return None

        return peer

    async def send_checksums(self, client: ClientConnection) -> None:
        async with self._inflight:
            source = await self._meta_manager.get_membership()
            checksums = await self._topology.get_checksums()
            data = {
                "source": source.to_dict(),
                "checksums": checksums
            }
            message = Message(type="gossip/checksums", data=data)
            await client.send(message)

    async def request_bucket(self, client: ClientConnection, bucket_id: str) -> None:
        membership = await self._meta_manager.get_membership()
        l_memberships = await self._topology.get_bucket_memberships(bucket_id)
        message = Message(
            type="gossip/bucket",
            data={
                "bucket_id": bucket_id,
                "source": membership.to_dict(),
                "memberships": {
                    m.node_id: m.to_dict()
                    for m in l_memberships.values()
                }
            }
        )
        await client.send(message)

    async def _attempt_gossip(
        self,
        peer: Membership,
        rate_limiter: RateLimiter
    ) -> None:
        try:
            client = await self._get_client(peer)
            self._logger.debug(f"Gossiping peer {peer.node_id}")
            await self.send_checksums(client)
            self._logger.debug(f"Sent gossip checksums to {peer.node_id}")
            rate_limiter.on_success()
        except Exception as ex:
            self._logger.error(f"Failed to gossip {peer.node_id}: {ex}")
            rate_limiter.on_error()

    async def _get_client(self, peer: Membership) -> ClientConnection:
        await self._peer_clients.register(peer.node_id, peer.peer_address)
        client = await self._peer_clients.get(peer.node_id)
        return client
