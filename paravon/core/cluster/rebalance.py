import asyncio
import logging
import random
import struct
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Generator

from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.helpers.waitgroup import WaitGroup
from paravon.core.models.message import Message
from paravon.core.models.version import ValueVersion, HLC
from paravon.core.ports.serializer import Serializer
from paravon.core.service.storage import StorageService
from paravon.core.space.partition import PlacementStrategy, Partitioner, LogicalPartition
from paravon.core.space.ring import Ring
from paravon.core.throttling.backoff import ExponentialBackoff


@dataclass(slots=True)
class PartitionInfo:
    sources: tuple[str, ...]
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    done: asyncio.Event = field(default_factory=asyncio.Event)


class Rebalancer:
    def __init__(
        self,
        partitioner: Partitioner,
        peer_clients: ClientConnectionPool,
        storage_service: StorageService,
        spawner: TaskSpawner,
        serializer: Serializer,
        replication_factor: int,
        node_id: str,
        *,
        chunk_timeout: float = 5.0,
        max_retries: int = 5,
        concurrency: int = 2,
    ) -> None:
        self._partitioner = partitioner
        self._peer_clients = peer_clients
        self._storage = storage_service
        self._spawner = spawner
        self._serializer = serializer
        self._replication_factor = replication_factor
        self._node_id = node_id
        self._strategy = PlacementStrategy(replication_factor)
        self._chunk_timeout = chunk_timeout
        self._max_retries = max_retries

        self._wg = WaitGroup()
        self._partition_semaphore = asyncio.Semaphore(concurrency)
        self._partitions: dict[int, PartitionInfo] = {}
        self._logger = logging.getLogger("core.space.rebalance")

        self._peer_clients.subscribe("partition/fetch", self)

    async def apply(
        self,
        plan: dict[tuple[str, ...], list[LogicalPartition]]
    ) -> None:
        new_pids = {
            partition.pid
            for partitions in plan.values()
            for partition in partitions
        }
        old_pids = set(self._partitions.keys())

        for pid in old_pids - new_pids:
            info = self._partitions.get(pid)
            if info:
                info.cancelled.set()

        for sources, partitions in plan.items():
            for partition in partitions:
                if partition.pid in self._partitions:
                    # already in pipe
                    continue

                info = PartitionInfo(sources=sources)
                self._partitions[partition.pid] = info
                await self._wg.add(1)
                self._spawner.spawn(self._run_partition(partition, info))

    async def wait(self) -> tuple[list[int], list[int]]:
        return await self._wg.wait()

    async def handle(self, message: Message) -> None:
        if message.type != "partition/fetch":
            return

        data = message.data
        keyspace = LogicalPartition.pid_for(data["keyspace"])
        raw = message.data.get("chunk")
        raw_len = len(raw or b"")
        self._logger.info(
            f"Received rebalance/response for {keyspace} with {raw_len} bytes"
        )

        info = self._partitions.get(keyspace)
        if not info or info.cancelled.is_set():
            self._logger.warning(f"Ignoring message for inactive partition {keyspace}")
            return

        source = data["source"]
        if source not in info.sources:
            self._logger.warning(
                f"Received from {source} but expect "
                f"one of {info.sources}, ignoring"
            )
            return

        await info.queue.put(message.data)

    async def plan(self, old_ring: Ring, new_ring: Ring) -> dict:
        partitions = defaultdict(list)
        yield_every = 250
        total = self._partitioner.total_partitions

        for idx, pid in enumerate(range(total)):
            end = self._partitioner.end_for_pid(pid)
            old_replicas = self._strategy.preference_list(end, old_ring)
            new_replicas = self._strategy.preference_list(end, new_ring)

            if self._node_id in new_replicas and self._node_id not in old_replicas:
                old_sources = tuple(old_replicas)
                if old_sources:
                    partition = LogicalPartition(
                        pid=pid,
                        start=self._partitioner.start_for_pid(pid),
                        end=end
                    )
                    partitions[old_sources].append(partition)

            if idx % yield_every == 0:
                await asyncio.sleep(0)

        return partitions

    async def _apply_chunk(self, keyspace: bytes, chunk: bytes) -> None:
        for key, value in self._parse_chunk(chunk):
            raw_version = self._serializer.deserialize(value)
            version = ValueVersion.from_dict(raw_version)
            await self._storage.apply({
                "keyspace": keyspace,
                "key": key,
                "version": version.to_dict()
            })

    async def _run_partition(
        self,
        partition: LogicalPartition,
        info: PartitionInfo
    ) -> None:
        async with self._partition_semaphore:
            await self._run_partition_inner(partition, info)

    async def _run_partition_inner(
        self,
        partition: LogicalPartition,
        info: PartitionInfo
    ) -> None:
        backoff = ExponentialBackoff()
        retries = 0
        last_hlc = await self._storage.last_hlc_for({
            "keyspace": partition.pid_bytes
        })

        try:
            while not info.cancelled.is_set():
                try:
                    await self._send_request(partition, info.sources, last_hlc)
                    data = await asyncio.wait_for(
                        info.queue.get(),
                        timeout=self._chunk_timeout
                    )
                except Exception as ex:
                    retries += 1

                    if retries > self._max_retries:
                        self._logger.error(
                            f"[pid={partition.pid}] Max retries exceeded "
                            f"({self._max_retries}), giving up"
                        )
                        break

                    delay = backoff.next_delay()
                    self._logger.warning(
                        f"[pid={partition.pid}] Failed to rebalance: "
                        f"{str(ex) or ex.__class__}, "
                        f"retry={retries}, sleeping {delay:.2f}s"
                    )

                    await asyncio.sleep(delay)
                    continue

                backoff.reset()
                retries = 0

                raw = data.get("chunk")
                if raw is None:
                    self._logger.info(f"[pid={partition.pid}] Rebalance DONE")
                    break

                await self._apply_chunk(partition.pid_bytes, raw)
                last_hlc = HLC.from_dict(data["hlc"])
        finally:
            info.done.set()
            await self._wg.done(partition.pid)

    async def _send_request(
        self,
        partition: LogicalPartition,
        sources: tuple[str, ...],
        hlc: HLC
    ) -> None:
        message = Message(
            type="partition/fetch",
            data={"keyspace": partition.pid_bytes, "hlc": hlc.to_dict()}
        )
        if not sources:
            self._logger.warning(
                f"No sources for pid={partition.pid}, skipping request"
            )
            return

        source = self._pick_source(sources)
        self._logger.debug(
            f"[pid={partition.pid}] Sending request to {source} with HLC={hlc}"
        )
        client = await self._peer_clients.get(source)
        await client.send(message)

    @staticmethod
    def _parse_chunk(raw: bytes) -> Generator[tuple[bytes, bytes], None]:
        # [4 bytes klen][klen bytes key][4 bytes vlen][vlen bytes value]...
        pos = 0
        end = len(raw)

        while pos + 4 <= end:
            klen = struct.unpack_from(">I", raw, pos)[0]
            pos += 4
            if pos + klen > end:
                break

            key = raw[pos:pos + klen]
            pos += klen

            if pos + 4 > end:
                break

            vlen = struct.unpack_from(">I", raw, pos)[0]
            pos += 4
            if pos + vlen > end:
                break

            value = raw[pos:pos + vlen]
            pos += vlen

            yield key, value

    @staticmethod
    def _pick_source(sources: tuple[str, ...]) -> str:
        return random.choice(sources)
