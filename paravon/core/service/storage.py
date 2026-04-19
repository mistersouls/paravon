import asyncio
import logging
import struct
import uuid
from typing import Any

from paravon.core.cluster.probe import ProbeManager
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.models.version import HLC
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.models.request import GetRequest, PutRequest, DeleteRequest
from paravon.core.models.version import ValueVersion
from paravon.core.ports.conflict import ConflictResolver
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import BackendStorageFactory
from paravon.core.service.coordinator import Coordinator
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.topology import TopologyManager
from paravon.core.space.partition import PartitionPlacement, Partitioner
from paravon.core.storage.partitioned import PartitionedStorage
from paravon.core.storage.versioned import VersionedStorageFactory


class StorageService:
    MAX_CHUNK = 1024 * 600 # todo(souls): move in config

    def __init__(
        self,
        peer_config: PeerConfig,
        backend_factory: BackendStorageFactory,
        serializer: Serializer,
        topology: TopologyManager,
        conflict_resolver: ConflictResolver,
        meta_manager: NodeMetaManager,
        probe_manager: ProbeManager,
        peer_clients: ClientConnectionPool,
        spawner: TaskSpawner,
        partitioner: Partitioner,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._serializer = serializer
        self._topology = topology
        self._meta_manager = meta_manager
        self._versioned_factory = VersionedStorageFactory(
            node_id=peer_config.node_id,
            backend_factory=backend_factory,
            serializer=self._serializer,
            conflict_resolver=conflict_resolver,
        )
        self._storage = PartitionedStorage(
            storage_factory=self._versioned_factory,
            serializer=self._serializer,
        )
        self._partitioner = partitioner
        self._coordinator = Coordinator(
            meta_manager=meta_manager,
            probe_manager=probe_manager,
            topology_manager=topology,
            peer_clients=peer_clients,
            peer_config=peer_config,
            spawner=spawner,
            storage=self._storage,
            loop=loop
        )

        self._logger = logging.getLogger("core.service.storage")

    async def get(self, data: dict) -> Message:
        key = data["key"]
        placement = await self._find_key_placement(key)
        # GET default in peer_config
        request = GetRequest(
            request_id=data.get("request_id", str(uuid.uuid4())),
            key=key,
            quorum=data.get("quorum", 2),
            timeout=data.get("timeout", 0.05),
        )
        return await self._coordinator.get(request, placement)

    async def put(self, data: dict) -> Message:
        key = data["key"]
        value = data["value"]
        placement = await self._find_key_placement(key)
        # PUT default in peer_config
        request = PutRequest(
            request_id=data.get("request_id", str(uuid.uuid4())),
            key=key,
            value=value,
            quorum=data.get("quorum", 2),
            timeout=data.get("timeout", 0.05),
        )
        return await self._coordinator.put(request, placement)

    async def delete(self, data: dict) -> Message:
        key = data["key"]
        placement = await self._find_key_placement(key)
        # DELETE default in peer_config
        request = DeleteRequest(
            request_id=data.get("request_id", str(uuid.uuid4())),
            key=key,
            quorum=data.get("quorum", 2),
            timeout=data.get("timeout", 0.05),
        )
        return await self._coordinator.delete(request, placement)

    async def read(self, data: dict) -> Message:
        key = data["key"]
        request_id = data.get("request_id", str(uuid.uuid4()))
        placement = await self._find_key_placement(key)
        version = await self._storage.get(placement.keyspace, key)
        membership = await self._meta_manager.get_membership()
        return Message(
            type="read",
            data={
                "version": version.to_dict() if version else None,
                "request_id": request_id,
                "source": membership.node_id
            },
        )

    async def apply(self, data: dict) -> Message:
        keyspace = data["keyspace"]
        key = data["key"]
        request_id = data.get("request_id", str(uuid.uuid4()))
        l_version = ValueVersion.from_dict(data["version"])
        version = await self._storage.apply(keyspace, key, l_version)
        membership = await self._meta_manager.get_membership()
        return Message(
            type="apply",
            data={
                "version": version.to_dict(),
                "request_id": request_id,
                "source": membership.node_id
            }
        )

    async def last_hlc_for(self, data: dict) -> HLC:
        return await self._storage.get_last_hlc(data["keyspace"])

    async def fetch_data(
        self,
        data: dict
    ) -> Message:
        buf = bytearray()
        keyspace = data["keyspace"]
        hlc = HLC.from_dict(data["hlc"])
        batch_size = data.get("batch_size", 1024)
        last_hlc = hlc
        count = 0

        async for key, version in self._storage.iter(keyspace, hlc, batch_size=batch_size):
            # [4 bytes klen][klen bytes key][4 bytes vlen][vlen bytes value]...
            raw_version = self._serializer.serialize(version.to_dict())
            kv = struct.pack(">I", len(key)) + key
            kv += struct.pack(">I", len(raw_version)) + raw_version
            if len(buf) + len(kv) <= self.MAX_CHUNK:
                buf += kv
                last_hlc = version.hlc
                count += 1
            else:
                break

        membership = await self._meta_manager.get_membership()
        data: dict[str, Any] = {
            "keyspace": keyspace,
            "hlc": last_hlc.to_dict(),
            "count": count,
            "request_id": data.get("request_id", str(uuid.uuid4())),
            "source": membership.node_id
        }

        if buf:
            data["chunk"] = bytes(buf)

        return Message(type="rebalance/fetch", data=data)

    async def _find_key_placement(self, key: bytes) -> PartitionPlacement:
        ring = await self._topology.get_ring()
        placement = self._partitioner.find_placement_by_key(key, ring)
        return placement
