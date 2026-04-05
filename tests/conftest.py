import argparse
import os
import ssl
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml
from typing import Generator

from paravon.bootstrap.config.loader import get_configfile
from paravon.bootstrap.config.settings import TLSSettings, ParavonConfig
from paravon.bootstrap.deps import get_config
from paravon.core.cluster.table import BucketTable
from paravon.core.controlplane import ControlPlane
from paravon.core.models.membership import Membership, NodePhase, NodeSize
from paravon.core.routing.app import RoutedApplication
from paravon.infra.lmdb_storage.aiobackend import LMDBStorageFactory
from paravon.infra.msgpack_serializer import MsgPackSerializer
from tests.fake.fake_transport import FakeTransport, JsonSerializer
from tests.helpers import FakeParaConfig
from tests.utils import generate_cert_pair, write_pem, env


@pytest.fixture
def serializer():
    return JsonSerializer()


@pytest.fixture
def transport():
    return FakeTransport()


@pytest.fixture(scope="session")
def tls_settings(tmp_path_factory) -> tuple[TLSSettings, TLSSettings]:
    ca_cert, server_key, server_cert, client_key, client_cert = generate_cert_pair()
    base = tmp_path_factory.mktemp("mtls")
    ca_path = base / "ca.pem"
    server_cert_path = base / "server.pem"
    server_key_path = base / "server.key"
    client_cert_path = base / "client.pem"
    client_key_path = base / "client.key"

    write_pem(ca_cert, ca_path)
    write_pem(server_cert, server_cert_path)
    write_pem(server_key, server_key_path)
    write_pem(client_cert, client_cert_path)
    write_pem(client_key, client_key_path)

    server_tls = TLSSettings(
        certfile=base / "server.pem",
        keyfile=base / "server.key",
        cafile=base / "ca.pem"
    )
    client_tls = TLSSettings(
        certfile=base / "client.pem",
        keyfile=base / "client.key",
        cafile=base / "ca.pem"
    )

    return server_tls, client_tls


@pytest.fixture(scope="session")
def config_file(tmp_path_factory, tls_settings):
    server_tls, _ = tls_settings
    base = tmp_path_factory.mktemp("config")
    file = base / "paranode.yaml"

    data = {
        "node": {
            "id": "node-1"
        },
        "server": {
            "api": {
                "host": "127.0.0.1",
                "port": 0,
            },
            "peer": {
                "host": "127.0.0.1",
                "port": 0,
                "seeds": []
            },
            "tls": {
                "certfile": str(server_tls.certfile),
                "keyfile": str(server_tls.keyfile),
                "cafile": str(server_tls.cafile),
            },
            "backlog": 10,
            "timeout_graceful_shutdown": 1,
            "limit_concurrency": 10,
            "max_buffer_size": 1024 * 1024,
            "max_message_size": 1024 * 1024,
        },
        "storage": {
            "data_dir": str(base / "data")
        }
    }

    file.write_text(yaml.dump(data))
    return file


@pytest.fixture(scope="session")
def para_config(config_file, tls_settings) -> Generator[ParavonConfig, None, None]:
    backup = os.environ.copy()

    try:
        os.environ["TEST_PARANODECONFIG"] = str(config_file)
        yield FakeParaConfig()  # noqa
    finally:
        os.environ.clear()
        os.environ.update(backup)


@pytest.fixture(scope="session")
def mtls_contexts(tmp_path_factory, tls_settings, para_config):
    _, client_tls = tls_settings

    # Server SSLContext (requires client cert)
    server_ctx = para_config.get_server_ssl_ctx()

    # Client SSLContext (presents client cert)
    client_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    client_ctx.load_cert_chain(client_tls.certfile, client_tls.keyfile)
    client_ctx.load_verify_locations(cafile=client_tls.cafile)

    return server_ctx, client_ctx


@pytest.fixture
def meta_manager():
    mm = AsyncMock()
    mm.get_membership = AsyncMock()
    mm.set_incarnation = AsyncMock()
    mm.bump_incarnation = AsyncMock()

    # Default local membership
    m = Membership(
        epoch=1,
        incarnation=1,
        node_id="local",
        tokens=[1],
        phase=NodePhase.ready,
        size=NodeSize.XS,
        peer_address="1.2.3.4:6000",
    )
    mm.get_membership.return_value = m
    return mm


@pytest.fixture
def table(meta_manager, serializer):
    return BucketTable(
        total_buckets=8,
        serializer=serializer,
        meta_manager=meta_manager,
        delta=3
    )


@pytest.fixture
def control_plane(tmp_path: Path, tls_settings, monkeypatch):
    get_configfile.cache_clear()
    get_config.cache_clear()

    monkeypatch.setattr(
        "paravon.bootstrap.config.loader.get_cli_args",
        lambda: argparse.Namespace(config=None)
    )

    config_dir = tmp_path / "config"
    config_dir.mkdir()

    data_dir = tmp_path / "data"
    data_dir.mkdir()

    tls_dir = tmp_path / "tls"
    tls_dir.mkdir()

    server_tls, _ = tls_settings
    cafile = server_tls.cafile
    keyfile = server_tls.keyfile
    certfile = server_tls.certfile

    # 2. Construire le YAML complet
    config_yaml = {
        "node": {
            "id": "node-1",
            "size": 1,
        },
        "server": {
            "api": {"port": 2001},
            "peer": {"port": 6001},
            "replication": {"port": 13001},
            "timeout_graceful_shutdown": 5,
            "tls": {
                "cafile": str(cafile),
                "keyfile": str(keyfile),
                "certfile": str(certfile),
            },
            "backlog": 128,
            "limit_concurrency": 128,
            "max_buffer_size": 2**20,
            "max_message_size": 2**20,
        },
        "storage": {
            "data_dir": str(data_dir),
        },
        "placement": {
            "shift": 4,
            "replication_factor": 2,
        },
    }

    config_file = config_dir / "paranode.yaml"
    config_file.write_text(yaml.safe_dump(config_yaml))

    with env(PARANODECONFIG=str(config_file)):
        config = get_config()
        storage_factory = LMDBStorageFactory(
            path=config.storage.data_dir,
            map_size=1<<24
        )

        return ControlPlane(
            config=config,
            api_app=RoutedApplication(),
            peer_app=RoutedApplication(),
            serializer=MsgPackSerializer(),
            storage_factory=storage_factory
        )

@pytest.fixture
def core(control_plane):
    loop = control_plane.loop
    core = control_plane.build_core()
    try:
        yield core
    finally:
        loop.run_until_complete(core.storage._storage.close())


@pytest.fixture
def storage_service(core):
    return core.storage
