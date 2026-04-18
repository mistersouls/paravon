import asyncio
import struct

import pytest

from paravon.core.models.version import HLC, ValueVersion
from paravon.core.space.partition import LogicalPartition


@pytest.fixture
def keyspace() -> bytes:
    partition = LogicalPartition(pid=0, start=0, end=10)
    return partition.pid_bytes


async def insert_versions(storage_service, keyspace: bytes, count: int):
    for i in range(count):
        version = ValueVersion(
            value=f"value-{i}".encode(),
            hlc=HLC.initial("node-1").tick_local(),
            is_tombstone=False,
        )
        data = {
            "keyspace": keyspace,
            "key": f"key-{i}".encode(),
            "version": version.to_dict(),
        }
        await storage_service.apply(data)
        await asyncio.sleep(0.0001)


@pytest.mark.it
@pytest.mark.asyncio
async def test_fetch_data_single_entry(storage_service, keyspace):
    await insert_versions(storage_service, keyspace, 1)

    req = {
        "keyspace": keyspace,
        "hlc": HLC.initial("node-1").to_dict(),
        "batch_size": 100,
    }

    msg = await storage_service.fetch_data(req)
    data = msg.data

    assert data["count"] == 1
    assert "chunk" in data

    chunk = data["chunk"]
    # decode first KV
    klen = struct.unpack(">I", chunk[:4])[0]
    key = chunk[4:4+klen]
    assert key == b"key-0"


@pytest.mark.it
@pytest.mark.asyncio
async def test_fetch_data_multiple_entries_single_chunk(storage_service, keyspace):
    storage_service.MAX_CHUNK = 10_000  # large enough
    await insert_versions(storage_service, keyspace, 10)

    req = {
        "keyspace": keyspace,
        "hlc": HLC.initial("node-1").to_dict(),
        "batch_size": 100,
    }

    msg = await storage_service.fetch_data(req)
    data = msg.data

    assert data["count"] == 10
    assert "chunk" in data


@pytest.mark.it
@pytest.mark.asyncio
async def test_fetch_data_chunking(storage_service, keyspace):
    storage_service.MAX_CHUNK = 200
    await insert_versions(storage_service, keyspace, 20)

    req = {
        "keyspace": keyspace,
        "hlc": HLC.initial("node-1").to_dict(),
        "batch_size": 100,
    }

    msg = await storage_service.fetch_data(req)
    data = msg.data

    assert data["count"] < 20
    assert "chunk" in data


@pytest.mark.it
@pytest.mark.asyncio
async def test_fetch_data_paginated(storage_service, keyspace):
    storage_service.MAX_CHUNK = 1000
    await insert_versions(storage_service, keyspace, 30)

    hlc = HLC.initial("node-1")
    collected = 0

    while True:
        req = {
            "keyspace": keyspace,
            "hlc": hlc.to_dict(),
            "batch_size": 100,
        }
        msg = await storage_service.fetch_data(req)
        data = msg.data

        collected += data["count"]
        hlc = HLC.from_dict(data["hlc"])

        if data["count"] == 0:
            break

    assert collected == 30


@pytest.mark.it
@pytest.mark.asyncio
async def test_fetch_data_empty(storage_service, keyspace):
    req = {
        "keyspace": keyspace,
        "hlc": HLC.initial("node-id").to_dict(),
        "batch_size": 100,
    }

    msg = await storage_service.fetch_data(req)
    data = msg.data

    assert data["count"] == 0
    assert "chunk" not in data
