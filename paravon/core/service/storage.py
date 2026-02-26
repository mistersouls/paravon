import asyncio
import logging
import uuid
from typing import AsyncIterator

from paravon.core.cluster.probe import ProbeManager
from paravon.core.connections.pool import ClientConnectionPool
from paravon.core.helpers.hlc import HLC, ConflictResolver
from paravon.core.helpers.spawn import TaskSpawner
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.models.request import GetRequest, PutRequest, DeleteRequest
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import StorageFactory
from paravon.core.service.coordinator import Coordinator
from paravon.core.service.meta import NodeMetaManager
from paravon.core.service.topology import TopologyManager
from paravon.core.space.partition import PartitionPlacement, Partitioner
from paravon.core.storage.partitioned import PartitionedStorage
from paravon.core.storage.versioned import VersionedStorageFactory, VersionedStorage


class StorageService:
    def __init__(
        self,
        peer_config: PeerConfig,
        backend_factory: StorageFactory,
        serializer: Serializer,
        topology: TopologyManager,
        conflict_resolver: ConflictResolver,
        meta_manager: NodeMetaManager,
        probe_manager: ProbeManager,
        peer_clients: ClientConnectionPool,
        spawner: TaskSpawner,
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
        self._partitioner = Partitioner(
            partition_shift=peer_config.partition_shift
        )
        self._peer_config = peer_config
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
            timeout=data.get("timeout", 600),
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

    async def local_get(self, data: dict) -> Message:
        key = data["key"]
        request_id = data.get("request_id", str(uuid.uuid4()))
        placement = await self._find_key_placement(key)
        value = await self._storage.get(placement.keyspace, key)
        membership = await self._meta_manager.get_membership()
        return Message(
            type="replica/get",
            data={
                "value": value,
                "request_id": request_id,
                "source": membership.node_id
            },
        )

    async def local_put(self, data: dict) -> Message:
        key = data["key"]
        value = data["value"]
        request_id = data.get("request_id", str(uuid.uuid4()))
        placement = await self._find_key_placement(key)
        await self._storage.put(placement.keyspace, key, value)
        membership = await self._meta_manager.get_membership()
        return Message(
            type="replica/put",
            data={
                "request_id": request_id,
                "source": membership.node_id
            },
        )

    async def local_delete(self, data: dict) -> Message:
        key = data["key"]
        request_id = data.get("request_id", str(uuid.uuid4()))
        placement = await self._find_key_placement(key)
        await self._storage.delete(placement.keyspace, key)
        membership = await self._meta_manager.get_membership()
        return Message(
            type="replica/delete",
            data={
                "request_id": request_id,
                "source": membership.node_id
            },
        )

    async def apply(
        self,
        keyspace: bytes,
        index_key: bytes,
        value: bytes,
    ) -> HLC | None:
        backend = await self._storage.select_backend(keyspace)
        if isinstance(backend, VersionedStorage):
            return await backend.apply_remote(keyspace, index_key, value)

        raise RuntimeError("Backend storage does not support remote apply")

    async def get_since_hlc(
        self, data: dict
    ) -> AsyncIterator[tuple[bytes, bytes, bytes]]:
        keyspace = data["keyspace"]
        hlc = HLC.from_dict(data["hlc"])
        backend = await self._storage.select_backend(keyspace)
        if not isinstance(backend, VersionedStorage):
            raise RuntimeError("Backend storage does not support temporal get")

        async for index_key, user_key, value in backend.iter_from_hlc(
            keyspace, hlc.encode()
        ):
            yield index_key, user_key, value

    async def _find_key_placement(self, key: bytes) -> PartitionPlacement:
        ring = await self._topology.get_ring()
        placement = self._partitioner.find_placement_by_key(key, ring)
        return placement
