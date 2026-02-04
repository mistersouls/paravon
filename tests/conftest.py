import ssl

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from tests.fake.fake_transport import FakeTransport
from tests.utils import generate_cert_pair


@pytest.fixture
def serializer():
    class FakeSerializer:
        @staticmethod
        def serialize(obj):
            return f"X-{obj}".encode()

        @staticmethod
        def deserialize(data):
            return {"type": "ping", "data": {"v": data.decode()}}

    return FakeSerializer()


@pytest.fixture
def transport():
    return FakeTransport()


@pytest.fixture(scope="session")
def mtls_contexts(tmp_path_factory):
    ca_cert, server_key, server_cert, client_key, client_cert = generate_cert_pair()

    # Create temp directory for PEM files
    base = tmp_path_factory.mktemp("mtls")

    def write_pem(obj, name):
        path = base / name
        if isinstance(obj, x509.Certificate):
            data = obj.public_bytes(serialization.Encoding.PEM)
        else:
            data = obj.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,
                serialization.NoEncryption(),
            )
        path.write_bytes(data)
        return str(path)

    ca_path = write_pem(ca_cert, "ca.pem")
    server_cert_path = write_pem(server_cert, "server.pem")
    server_key_path = write_pem(server_key, "server.key")
    client_cert_path = write_pem(client_cert, "client.pem")
    client_key_path = write_pem(client_key, "client.key")

    # Server SSLContext (requires client cert)
    server_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    server_ctx.load_cert_chain(server_cert_path, server_key_path)
    server_ctx.load_verify_locations(cafile=ca_path)
    server_ctx.verify_mode = ssl.CERT_REQUIRED

    # Client SSLContext (presents client cert)
    client_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    client_ctx.load_cert_chain(client_cert_path, client_key_path)
    client_ctx.load_verify_locations(cafile=ca_path)

    return server_ctx, client_ctx
