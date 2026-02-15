import asyncio
import logging
from collections import deque

from paravon.core.connections.client import ClientConnection
from paravon.core.gossip.gossiper import Gossiper
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.helpers.sub import Subscription
from paravon.core.models.config import PeerConfig
from paravon.core.models.membership import Membership, View
from paravon.core.ports.serializer import Serializer
from paravon.core.throttling.backoff import ExponentialBackoff


class SeedBootstrapper:
    """
    Bootstrap sequence with infinite CrashLoopBackoff:
        1. Retry sending sync/checksums until quorum view is reached.
        2. Retry requesting membership buckets until all are received.
        3. Return full membership list.

    Guarantees:
        - never fails permanently
        - exponential backoff up to 60s
        - timeouts on all phases
        - no infinite tight loops
    """

    VIEW_TIMEOUT = 5
    MEMBERSHIP_TIMEOUT = 10
    MAX_BUCKET_RETRIES = 5

    def __init__(
        self,
        peer_config: PeerConfig,
        spawner: TaskSpawner,
        serializer: Serializer,
        membership: Membership,
        gossiper: Gossiper,
        loop: asyncio.AbstractEventLoop,
        max_inc_delta: int = 3
    ) -> None:
        self._peer_config = peer_config
        self._spawner = spawner
        self._serializer = serializer
        self._loop = loop
        self._max_inc_delta = max_inc_delta
        self._membership = membership
        self._gossiper = gossiper

        self._quorum = (len(self._peer_config.seeds) // 2) + 1
        self._subscription = Subscription(loop)
        self._seed_clients = {}

        self._logger = logging.getLogger("core.service.bootstrapper")

    async def __aenter__(self) -> list[Membership]:
        self._seed_clients = {
            seed: ClientConnection(
                address=seed,
                ssl_context=self._peer_config.client_ssl_ctx,
                subscription=self._subscription,
                spawner=self._spawner,
                serializer=self._serializer
            )
            for seed in self._peer_config.seeds
            if seed != self._membership.peer_address
        }

        if not self._seed_clients:
            raise RuntimeError("SeedBootstrapper requires at least one seed.")

        return await self._bootstrap()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._subscription.publish(None)
        await asyncio.gather(*(c.close() for c in self._seed_clients.values()))

    async def _bootstrap(self) -> list[Membership]:
        view = await self._bootstrap_view()
        return await self._bootstrap_memberships(view)

    async def _bootstrap_view(self) -> View:
        backoff = ExponentialBackoff(initial=0.5, maximum=60.0, jitter=1.0)
        views: dict[str, View] = {}

        attempt = 0
        while True:
            attempt += 1
            self._logger.debug(f"Attempt #{attempt}")

            start = self._loop.time()
            fetch_views = self._spawner.spawn(self._fetch_views())

            try:
                await asyncio.wait_for(self._try_once(), timeout=self.VIEW_TIMEOUT)
            except asyncio.TimeoutError:
                self._logger.warning("Sending sync checksums timed out")

            elapsed = self.VIEW_TIMEOUT - (self._loop.time() - start)
            if elapsed < 0:
                continue

            try:
                new_views = await asyncio.wait_for(fetch_views, timeout=elapsed)
                views.update(new_views)
                majority = self._majority_view(views)
                if majority:
                    self._logger.info("Majority view reached")
                    return View(
                        incarnation=majority.incarnation,
                        address=majority.address,
                        peer=majority.peer,
                        checksums={
                            b: c
                            for b, c in majority.checksums.items()
                            if c > 0
                        }
                    )
            except asyncio.TimeoutError:
                pass

            delay = backoff.next_delay()
            self._logger.debug(f"No majority yet, retrying in {delay:.2f}s")
            await asyncio.sleep(delay)

        # should never happen
        raise RuntimeError("Expected to return view when breaking loop")

    async def _fetch_views(self) -> dict[str, View]:
        views: dict[str, View] = {}

        async for msg in self._subscription:
            if msg.type != "gossip/checksums":
                continue

            checksums = msg.data["checksums"]
            peer = Membership.from_dict(msg.data["source"])

            views[peer.node_id] = View(
                incarnation=peer.incarnation,
                checksums=checksums,
                address=peer.peer_address,
                peer=peer.node_id
            )

            if len(views) >= self._quorum:
                break

        return views

    async def _try_once(self) -> None:
        coros = [
            self._gossiper.send_checksums(client)
            for client in self._seed_clients.values()
        ]
        result = await asyncio.gather(*coros, return_exceptions=True)
        for done in result:
            if isinstance(done, Exception):
                self._logger.error(f"Error occurred: {done}")

    def _majority_view(self, views: dict[str, View]) -> View | None:
        if len(views) < self._quorum:
            return None

        dominant = max(views.values(), key=lambda v: v.incarnation)

        coherent = [
            v for v in views.values()
            if abs(v.incarnation - dominant.incarnation) <= self._max_inc_delta
        ]

        return dominant if len(coherent) >= self._quorum else None

    async def _bootstrap_memberships(self, view: View) -> list[Membership]:
        backoff = ExponentialBackoff(initial=0.5, maximum=60.0, jitter=1.0)

        while True:
            fetch_memberships = self._spawner.spawn(self._fetch_memberships(view))
            await self._trigger_sync_memberships(view)

            try:
                return await asyncio.wait_for(
                    fetch_memberships, timeout=self.MEMBERSHIP_TIMEOUT
                )
            except asyncio.TimeoutError:
                delay = backoff.next_delay()
                self._logger.warning(
                    f"Membership fetch timed out, retrying in {delay:.2f}s"
                )
                await asyncio.sleep(delay)

    async def _trigger_sync_memberships(self, view: View) -> None:
        checksums = deque(view.checksums.keys())
        retries = {bid: 0 for bid in checksums}
        backoff = ExponentialBackoff(initial=0.5, maximum=60.0, jitter=1.0)
        client = self._seed_clients[view.address]

        while checksums:
            bucket_id = checksums.popleft()

            try:
                await self._gossiper.request_bucket(client, bucket_id)
            except Exception as ex:
                retries[bucket_id] += 1
                if retries[bucket_id] >= self.MAX_BUCKET_RETRIES:
                    self._logger.error(
                        f"[bootstrap] Giving up bucket {bucket_id}, "
                        f"restarting full membership phase"
                    )
                    break

                delay = backoff.next_delay()
                self._logger.warning(
                    f"Retry bucket {bucket_id} in {delay:.2f}s: {ex}"
                )
                await asyncio.sleep(delay)
                checksums.append(bucket_id)

    async def _fetch_memberships(self, view: View) -> list[Membership]:
        memberships: list[Membership] = []
        buckets = view.checksums

        async for msg in self._subscription:
            if msg.type != "gossip/bucket":
                continue

            remote_memberships = msg.data["memberships"]
            remote_bucket_id = msg.data["bucket_id"]

            if remote_bucket_id in buckets:
                del buckets[remote_bucket_id]
                memberships.extend(
                    Membership.from_dict(m)
                    for m in remote_memberships.values()
                )

            if not buckets:
                break

        return memberships
