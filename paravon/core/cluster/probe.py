import asyncio
import logging

from paravon.core.cluster.phi import FailureDetector
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.message import Message
from paravon.core.service.meta import NodeMetaManager


class ProbeManager:
    def __init__(
        self,
        peer_clients: ClientConnectionPool,
        meta_manager: NodeMetaManager,
        spawner: TaskSpawner,
        cycle_count: int = 2,
        ping_interval: float = 1.0,
        threshold: float = 2.0,
        window_size: int = 50,
        min_interval: float = 1e-3,
        max_interval: float = 60.0,
        phi_cap: float = 50.0,
    ) -> None:
        self._peer_clients = peer_clients
        self._meta_manager = meta_manager
        self._spawner = spawner
        self._cycle_count = cycle_count
        self._ping_interval = ping_interval

        self._fd_params = {
            "threshold": threshold,
            "window_size": window_size,
            "min_interval": min_interval,
            "max_interval": max_interval,
            "phi_cap": phi_cap,
        }

        self._fds: dict[str, FailureDetector] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._stopped = False

        self._logger = logging.getLogger("core.cluster.probe")

    @property
    def required_samples(self) -> int:
        return self._cycle_count * 3

    async def run(self, stop_event: asyncio.Event) -> None:
        self._peer_clients.subscribe("ping/put", self)
        self._peer_clients.subscribe("ping/get", self)
        self._peer_clients.subscribe("ping/delete", self)

        self._logger.info("ProbeManager started")
        await stop_event.wait()

        self._logger.info("ProbeManager stopping, cancelling probe tasks")
        for task in self._tasks.values():
            task.cancel()

        if tasks := self._tasks.values():
            await asyncio.gather(*tasks, return_exceptions=True)

        self._stopped = True

    async def handle(self, message: Message) -> None:
        if message.type not in ("ping/put", "ping/get", "ping/delete"):
            return

        peer = message.data["source"]
        fd = self._fds.get(peer)
        if fd is None:
            return

        fd.record_heartbeat()
        self._logger.debug(f"heartbeat received from {peer} ({message.type})")

    def is_alive(self, peer: str) -> bool:
        fd = self._fds.get(peer)
        if fd is None:
            return True

        phi = fd.compute_phi()
        alive = fd.is_alive(min_samples=self.required_samples)

        self._logger.debug(
            f"is_alive({peer}) -> phi={phi:.3f}, "
            f"samples={len(fd._arrival_times)}, "
            f"required={self.required_samples}, alive={alive}"
        )

        return alive

    async def mark_suspect(self, peer: str) -> None:
        if self._stopped:
            return

        lock = self._locks.setdefault(peer, asyncio.Lock())
        async with lock:
            self._logger.warning(f"peer {peer} marked suspect, starting probe")

            fd = self._fds.setdefault(peer, FailureDetector(**self._fd_params))
            fd.reset()

            if task := self._tasks.get(peer):
                task.cancel()

            task = self._spawner.spawn(self._probe(peer, fd))
            self._tasks[peer] = task

    async def _probe(self, peer: str, fd: FailureDetector) -> None:
        self._logger.info(f"probe started for peer {peer}")

        while True:
            await self._run_cycle(peer)

            if fd.is_alive(min_samples=self.required_samples):
                self._logger.info(
                    f"peer {peer} recovered "
                    f"(samples={len(fd._arrival_times)}, phi={fd.compute_phi():.3f})"
                )
                break

        self._logger.info(f"probe finished for peer {peer}")

    async def _run_cycle(self, peer: str) -> None:
        self._logger.debug(f"running probe cycle for {peer}")

        membership = await self._meta_manager.get_membership()

        for msg_type in ("ping/put", "ping/get", "ping/delete"):
            message = Message(msg_type, {"source": membership.node_id})
            await self._ping(peer, message)

    async def _ping(self, peer: str, message: Message) -> None:
        while True:
            client = await self._peer_clients.get(peer)
            try:
                await client.send(message)
                self._logger.debug(f"ping {message.type} to {peer} succeeded")
                break
            except Exception as ex:
                self._logger.error(f"ping {message.type} to {peer} failed: {ex}")
            finally:
                await asyncio.sleep(self._ping_interval)
