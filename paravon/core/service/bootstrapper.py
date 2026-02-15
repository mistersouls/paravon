import asyncio
import logging
from types import TracebackType

from paravon.core.connections.client import ClientConnection
from paravon.core.gossip.gossiper import Gossiper
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.helpers.sub import Subscription
from paravon.core.models.config import PeerConfig
from paravon.core.models.membership import Membership, NodeSize, NodePhase, View
from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer
from paravon.core.space.hashspace import HashSpace


class SeedBootstrapper:
    def __init__(
        self,
        peer_config: PeerConfig,
        membership: Membership,
        gossiper: Gossiper,
        spawner: TaskSpawner,
        serializer: Serializer,
        loop: asyncio.AbstractEventLoop | None = None,
        max_epoch_delta: int = 3
    ) -> None:
        self._seeds = peer_config.seeds
        self._peer_address = peer_config.peer_listener
        if not self._seeds:
            raise RuntimeError("SeedBootstrapper requires seeds but empty.")

        self._membership = membership
        self._gossiper = gossiper
        self._spawner = spawner
        self._peer_config = peer_config
        self._serializer = serializer
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop

        self._max_epoch_delta = max_epoch_delta
        self._subscription = Subscription(self._loop)
        self._seed_clients = {}
        self._logger = logging.getLogger("core.gossip.bootstrapper")

    async def __aenter__(self) -> list[Membership]:
        self._seed_clients = {
            seed: ClientConnection(
                address=seed,
                ssl_context=self._peer_config.client_ssl_ctx,
                subscription=self._subscription,
                serializer=self._serializer,
                spawner=self._spawner
            )
            for seed in self._seeds
            if seed != self._peer_address
        }
        return await self._bootstrap()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None
    ) -> None:
        tasks = [client.close() for client in self._seed_clients.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._seed_clients.clear()

    async def close(self) -> None:
        tasks = [client.close() for client in self._seed_clients.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._seed_clients.clear()
        self._subscription.publish(None)

    async def _bootstrap(self) -> list[Membership]:
        # stub
        fetch_views = self._spawner.spawn(self._fetch_views())
        await self._gossip_seeds()

        views = await fetch_views
        self._logger.info(views)
        s2 = Membership(
            incarnation=0,
            epoch=0,
            node_id="node-1",
            size=NodeSize.L,
            phase=NodePhase.ready,
            tokens=list(HashSpace.generate_tokens("node-1", NodeSize.L.value)),
            peer_address="127.0.0.1:6001"
        )
        return [s2]

    async def _fetch_views(self) -> dict[str, View]:
        views: dict[str, View] = {}
        quorum = (len(self._seeds) // 2) + 1

        async for msg in self._subscription:
            if msg is None:
                break

            if msg.type != "gossip/checksums":
                continue


            checksums = msg.data["checksums"]
            remote = Membership.from_dict(msg.data["source"])
            views[remote.node_id] = View(
                incarnation=remote.incarnation,
                checksums=checksums,
                address=remote.peer_address,
                peer=remote.node_id
            )
            if len(views) >= quorum:
                break

        return views

    async def _gossip_seeds(self) -> None:
        coros = [
            self._gossiper.send_checksums(client)
            for client in self._seed_clients.values()
        ]
        if not coros:
            raise RuntimeError(
                "Please call bootstrap in `async with` "
                "context with non empty seeds."
            )

        result = await asyncio.gather(*coros, return_exceptions=True)
        for done in result:
            if isinstance(done, Exception):
                self._logger.error(
                f"Error while gossiping seed: {str(done)}", exc_info=done
                )

    async def _send_message(self, address: str, message: Message) -> None:
        client = self._seed_clients[address]
        await client.send(message)
