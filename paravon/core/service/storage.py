from typing import AsyncIterator

from paravon.core.helpers.hlc import HLC, ConflictResolver
from paravon.core.models.config import PeerConfig
from paravon.core.models.message import Message
from paravon.core.ports.serializer import Serializer
from paravon.core.ports.storage import StorageFactory
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
    ) -> None:
        self._peer_config = peer_config
        self._backend_factory = backend_factory
        self._serializer = serializer
        self._topology = topology
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

    async def get(self, data: dict) -> Message:
        key = data["key"]
        placement = await self._find_key_placement(key)
        local_node = self._peer_config.node_id
        if placement.vnode.node_id != self._peer_config.node_id:
            return Message(
                type="ko",
                data={
                    "message": (
                        f"The local node {local_node} is not owner of key"
                    ),
                    "key": key
                }
            )

        value = await self._storage.get(placement.keyspace, key)
        return Message(type="get", data={"key": key, "value": value})

    async def put(self, data: dict) -> Message:
        key = data["key"]
        value = data["value"]
        placement = await self._find_key_placement(key)
        local_node = self._peer_config.node_id
        if placement.vnode.node_id != local_node:
            return Message(
                type="ko",
                data={
                    "message": (
                        f"The local node {local_node} is not owner of key. "
                        "Coordination is not implemented yet"
                    ),
                    "key": key
                }
            )

        await self._storage.put(placement.keyspace, key, value)
        return Message(type="put", data={"key": key})

    async def delete(self, data: dict) -> Message:
        key = data["key"]
        placement = await self._find_key_placement(key)
        local_node = self._peer_config.node_id
        if placement.vnode.node_id != local_node:
            return Message(
                type="ko",
                data={
                    "message": (
                        f"The local node {local_node} is not owner of key. "
                        "Coordination is not implemented yet"
                    ),
                    "key": key
                }
            )

        await self._storage.delete(placement.keyspace, key)
        return Message(type="delete", data={"key": key})

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
