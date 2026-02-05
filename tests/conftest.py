import os
import ssl

import pytest
import yaml
from typing import Generator
from tests.fake.fake_transport import FakeTransport, FakeSerializer
from tests.helpers import FakeParaConfig
from tests.utils import generate_cert_pair, write_pem

from paravon.bootstrap.config.settings import ParavonConfig, TLSSettings


@pytest.fixture
def serializer():
    return FakeSerializer()


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
        yield FakeParaConfig()
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
