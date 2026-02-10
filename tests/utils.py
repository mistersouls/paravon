import ipaddress
from datetime import datetime, timedelta, UTC
from pathlib import Path

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from paravon.core.models.membership import Membership, NodePhase, NodeSize


def generate_cert_pair():
    # Generate CA key
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # Build CA certificate with SKI + KeyUsage
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "Test CA"),
    ])

    ca_cert_builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(tz=UTC) - timedelta(days=1))
        .not_valid_after(datetime.now(tz=UTC) + timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,   # indispensable pour un CA
                crl_sign=True,        # indispensable pour un CA
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
    )

    ca_cert = ca_cert_builder.sign(ca_key, hashes.SHA256())

    # Extract SKI for AKI
    ski = ca_cert.extensions.get_extension_for_class(x509.SubjectKeyIdentifier).value

    def generate_cert(common_name):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ])

        cert_builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_cert.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(tz=UTC) - timedelta(days=1))
            .not_valid_after(datetime.now(tz=UTC) + timedelta(days=1))
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ski),
                critical=False,
            )
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                critical=False,
            )
            .add_extension(
                x509.SubjectAlternativeName([
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))
                ]),
                critical=False,
            )
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
        )

        cert = cert_builder.sign(ca_key, hashes.SHA256())
        return key, cert

    server_key, server_cert = generate_cert("server.test")
    client_key, client_cert = generate_cert("client.test")

    return ca_cert, server_key, server_cert, client_key, client_cert


def write_pem(obj, path: Path) -> None:
    if isinstance(obj, x509.Certificate):
        data = obj.public_bytes(serialization.Encoding.PEM)
    else:
        data = obj.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    path.write_bytes(data)


def make_member(node_id, epoch=1, incarnation=1):
    return Membership(
        epoch=epoch,
        incarnation=incarnation,
        node_id=node_id,
        tokens=[1, 2, 3, 4, 5, 6, 7, 8],
        phase=NodePhase.ready,
        size=NodeSize.L
    )
