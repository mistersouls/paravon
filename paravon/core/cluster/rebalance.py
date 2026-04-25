import asyncio
import logging
import random
import struct
from dataclasses import dataclass, field
from typing import Generator

from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.helpers.waitgroup import WaitGroup
from paravon.core.models.message import Message
from paravon.core.models.version import HLC, ValueVersion
from paravon.core.ports.serializer import Serializer
from paravon.core.service.storage import StorageService
from paravon.core.space.partition import Partitioner, LogicalPartition, PlacementStrategy
from paravon.core.space.ring import Ring
from paravon.core.throttling.backoff import ExponentialBackoff


@dataclass()
class RebalancePlanItem:
    partition: LogicalPartition
    source: str
    last_hlc: HLC
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)


@dataclass
class RebalancePlan:
    incoming: dict[tuple[str, int], RebalancePlanItem]
    outgoing: dict[tuple[str, int], RebalancePlanItem]


RebalanceResult = tuple[list[tuple[str, int]], list[tuple[str, int]]]


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
        self._strategy = PlacementStrategy(replication_factor)
        self._peer_clients = peer_clients
        self._storage_service = storage_service
        self._spawner = spawner
        self._serializer = serializer
        self._node_id = node_id
        self._max_retries = max_retries
        self._chunk_timeout = chunk_timeout

        self._incoming = {}
        self._outgoing = {}

        self._outgoing_wg = WaitGroup()
        self._incoming_wg = WaitGroup()

        self._incoming_pending: dict[tuple[str, int], HLC] = {}
        self._outgoing_sem = asyncio.Semaphore(concurrency)

        self._logger = logging.getLogger("core.cluster.rebalance")

        self._peer_clients.subscribe("rebalance/fetch", self)
        self._peer_clients.subscribe("rebalance/done", self)

    async def handle(self, message: Message) -> None:
        if message.type != "rebalance/fetch":
            return

        data = message.data
        keyspace = LogicalPartition.pid_for(data["keyspace"])
        source = data["source"]
        raw = message.data.get("chunk")
        raw_len = len(raw or b"")
        self._logger.info(
            f"Received rebalance/response for {keyspace} with {raw_len} bytes"
        )

        key = (source, keyspace)
        item = self._outgoing.get(key)
        if not item or item.cancelled.is_set():
            self._logger.warning(f"Ignoring message for inactive or unknown item {key}")
            return

        await item.queue.put(message.data)

    async def plan(self, current: Ring, target: Ring) -> RebalancePlan:
        yield_every = 250
        total = self._partitioner.total_partitions
        outgoing = {}
        incoming = {}

        for idx, pid in enumerate(range(total)):
            end = self._partitioner.end_for_pid(pid)
            partition = LogicalPartition(
                pid=pid,
                start=self._partitioner.start_for_pid(pid),
                end=end
            )

            curr_replicas = self._strategy.preference_list(end, current)
            targ_replicas = self._strategy.preference_list(end, target)
            curr_set = set(curr_replicas)
            targ_set = set(targ_replicas)
            leavers = [n for n in curr_replicas if n in list(curr_set - targ_set)]
            joiners = [n for n in targ_replicas if n in list(targ_set - curr_set)]

            if self._node_id in joiners:
                last_hlc = await self._storage_service.last_hlc_for({
                    "keyspace": partition.pid_bytes
                })
                if len(joiners) == len(leavers):
                    source = leavers[joiners.index(self._node_id)]
                else:
                    source = random.choice(curr_replicas)

                item = RebalancePlanItem(
                    partition=partition,
                    source=source,
                    last_hlc=last_hlc
                )
                outgoing[(source, pid)] = item

            if self._node_id in leavers:
                last_hlc = await self._storage_service.last_hlc_for({
                    "keyspace": partition.pid_bytes
                })
                if len(leavers) != len(joiners):
                    self._logger.debug(f"Not enough replicas, ignoring pid={pid}")
                    continue

                source = joiners[leavers.index(self._node_id)]
                item = RebalancePlanItem(
                    partition=partition,
                    source=source,
                    last_hlc=last_hlc
                )
                incoming[(source, pid)] = item

            if idx and idx % yield_every == 0:
                self._logger.debug(
                    "Planning rebalance: processed %d/%d partitions", idx, total
                )
                await asyncio.sleep(0)

        return RebalancePlan(
            incoming=incoming,
            outgoing=outgoing
        )

    async def apply(self, plan: RebalancePlan) -> None:
        new_incoming = set(plan.incoming)
        new_outgoing = set(plan.outgoing)
        old_incoming = set(self._incoming)
        old_outgoing = set(self._outgoing)

        for old in old_outgoing - new_outgoing:
            item: RebalancePlanItem = self._outgoing.pop(old)
            if item:
                item.cancelled.set()

        for old in old_incoming - new_incoming:
            item: RebalancePlanItem = self._incoming.pop(old, None)
            if item:
                item.cancelled.set()

            self._incoming_pending.pop(old, None)

        for key, item in plan.outgoing.items():
            if key in self._outgoing:
                # already in pipe
                continue

            self._outgoing[key] = item
            await self._outgoing_wg.add(1)
            self._spawner.spawn(self._fetch_partition_for(item))

        for key, item in plan.incoming.items():
            if key in self._incoming:
                # already in pipe
                continue

            self._incoming[key] = item
            await self._incoming_wg.add(1)

            hlc = self._incoming_pending.get(key, None)
            if hlc is not None and hlc >= item.last_hlc:
                self._spawner.spawn(self.mark_incoming_done(key[0], key[1], hlc))

    async def wait_incoming(self) -> RebalanceResult:
        return await self._incoming_wg.wait()

    async def wait_outgoing(self) -> RebalanceResult:
        return await self._outgoing_wg.wait()

    async def mark_incoming_done(self, source: str, pid: int, hlc: HLC) -> None:
        key = (source, pid)
        item = self._incoming.pop(key, None)
        if item:
            self._incoming_pending.pop(key, None)
            await self._incoming_wg.done(key)
        else:
            self._incoming_pending[key] = hlc

    async def _apply_chunk(self, keyspace: bytes, chunk: bytes) -> None:
        for key, value in self._parse_chunk(chunk):
            raw_version = self._serializer.deserialize(value)
            version = ValueVersion.from_dict(raw_version)
            await self._storage_service.apply({
                "keyspace": keyspace,
                "key": key,
                "version": version.to_dict()
            })

    async def _fetch_partition_for(self, item: RebalancePlanItem) -> None:
        async with self._outgoing_sem:
            await self._fetch_partition_inner(item)

    async def _fetch_partition_inner(self, item: RebalancePlanItem) -> None:
        backoff = ExponentialBackoff()
        retries = 0

        pid = item.partition.pid
        source = item.source
        key = (source, pid)

        while not item.cancelled.is_set():
            try:
                await self._send_fetch_request(item)
                data = await asyncio.wait_for(
                    item.queue.get(),
                    timeout=self._chunk_timeout
                )
                if data is None:
                    await self._outgoing_wg.done(key, False)
                    break
            except Exception as ex:
                retries += 1

                if retries > self._max_retries:
                    self._logger.error(
                        f"[pid={item.partition.pid}] Max retries exceeded "
                        f"({self._max_retries}), giving up"
                    )
                    await self._outgoing_wg.done(key, False)
                    break

                delay = backoff.next_delay()
                self._logger.warning(
                    f"[pid={item.partition.pid}] Failed to fetch: "
                    f"{str(ex) or ex.__class__}, "
                    f"retry={retries}, sleeping {delay:.2f}s"
                )

                await asyncio.sleep(delay)
                continue

            backoff.reset()
            retries = 0

            raw = data.get("chunk")
            if raw is None:
                self._logger.info(f"[pid={item.partition.pid}] Rebalance DONE")
                await self._send_done_request(item)
                await self._outgoing_wg.done(key, True)
                break

            await self._apply_chunk(item.partition.pid_bytes, raw)
            item.last_hlc = HLC.from_dict(data["hlc"])

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

    async def _send_fetch_request(self, item: RebalancePlanItem) -> None:
        partition = item.partition
        hlc = item.last_hlc
        source = item.source

        message = Message(
            type="rebalance/fetch",
            data={
                "keyspace": partition.pid_bytes,
                "hlc": hlc.to_dict(),
                "source": self._node_id
            }
        )
        self._logger.debug(
            f"[pid={partition.pid}] Sending fetch request to {source} with HLC={hlc}"
        )
        client = await self._peer_clients.get(source)
        await client.send(message)

    async def _send_done_request(self, item: RebalancePlanItem) -> None:
        partition = item.partition
        hlc = item.last_hlc
        source = item.source

        message = Message(
            type="rebalance/done",
            data={
                "keyspace": partition.pid_bytes,
                "hlc": hlc.to_dict(),
                "source": self._node_id
            }
        )

        backoff = ExponentialBackoff()
        retries = 0

        while True:
            try:
                client = await self._peer_clients.get(source)
                await client.send(message)
                self._logger.debug(
                    f"[pid={partition.pid}] DONE sent to {source} with HLC={hlc}"
                )
                return
            except Exception as ex:
                retries += 1

                if retries > self._max_retries:
                    self._logger.error(
                        f"[pid={partition.pid}] Failed to send DONE after "
                        f"{self._max_retries} retries: {ex}"
                    )
                    return

                delay = backoff.next_delay()
                self._logger.warning(
                    f"[pid={partition.pid}] Failed to send DONE: {ex}, "
                    f"retry={retries}, sleeping {delay:.2f}s"
                )
                await asyncio.sleep(delay)
