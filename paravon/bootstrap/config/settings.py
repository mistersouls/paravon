import ssl
from pathlib import Path

from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Annotated
from pydantic_settings import BaseSettings, SettingsConfigDict, PydanticBaseSettingsSource, YamlConfigSettingsSource

from pydantic_core.core_schema import ValidationInfo

from paravon.bootstrap.config.loader import get_configfile


class ApiServerSettings(BaseModel):
    host: Annotated[
        str,
        Field(
            description="Bind address for the client-facing API.",
            default="127.0.0.1"
        )
    ]

    port: Annotated[
        int,
        Field(
            description="TCP port for client requests (GET/PUT/DELETE).",
            default=2000
        )
    ]


class PeerServerSettings(BaseModel):
    host: Annotated[
        str,
        Field(
            description="Bind address for inter-node communication.",
            default="127.0.0.1"
        )
    ]

    port: Annotated[
        int,
        Field(
            description="TCP port for inter-node communication.",
            default=12000
        )
    ]


class TLSSettings(BaseModel):
    certfile: Annotated[
        Path,
        Field(
            description=(
                "Path to the node's TLS certificate (PEM).\n"
                "Paravon uses mandatory mutual TLS (mTLS): all traffic between nodes is "
                "encrypted and authenticated.\n"
            ),
        )
    ]

    keyfile: Annotated[
        Path,
        Field(
            description=(
                "Path to the node's TLS private key (PEM).\n"
                "Required for mTLS: the node proves its identity using this key.\n"
            )
        )
    ]

    cafile: Annotated[
        Path,
        Field(
            description=(
                "Path to the CA certificate (PEM) used to verify peer node certificates.\n"
                "Paravon enforces mutual TLS (mTLS): nodes must present a certificate "
                "signed by this CA. Connections without valid certificates are rejected.\n"
            )
        )
    ]

    @field_validator("certfile", "keyfile", "cafile")
    @classmethod
    def validate_path(cls, v: Path, _: ValidationInfo) -> Path:
        if not v.exists():
            raise ValidationError(f"Path {v} does not exist.")
        return v


class ServerSettings(BaseModel):
    api: Annotated[
        ApiServerSettings,
        Field(description="Client-facing server configuration.")
    ]

    peer: Annotated[
        PeerServerSettings,
        Field(description="Inter-node server configuration.")
    ]

    tls: Annotated[
        TLSSettings,
        Field(description="TLS configuration shared by client and peer servers.")
    ]

    backlog: Annotated[
        int,
        Field(
            description="Maximum number of pending TCP connections.",
            default=128
        )
    ]

    timeout_graceful_shutdown: Annotated[
        float,
        Field(
            description="Maximum time allowed for graceful shutdown.",
            default=5.0
        )
    ]

    limit_concurrency: Annotated[
        int,
        Field(
            description="Maximum number of concurrent in-flight requests.",
            default=1024
        )
    ]

    max_buffer_size: Annotated[
        int,
        Field(
            description="Maximum allowed buffer size for incoming data.",
            default=4 * 1024 * 1024
        )
    ]

    max_message_size: Annotated[
        int,
        Field(
            description="Maximum allowed size for a single decoded message.",
            default=1 * 1024 * 1024
        )
    ]


class ParavonConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PARAVON_",
        yaml_file=get_configfile(),
        extra="allow"
    )

    server: Annotated[
        ServerSettings,
        Field(
            description=(
                "Local server configuration.\n"
                "Controls how the node listens for incoming TCP connections, enforces\n"
                "TLS security, and applies runtime limits such as concurrency, buffer\n"
                "sizes, and graceful shutdown behavior."
            )
        )
    ]

    @classmethod
    def settings_customise_sources(
            cls,
            settings_cls: type[BaseSettings],
            init_settings: PydanticBaseSettingsSource,
            env_settings: PydanticBaseSettingsSource,
            dotenv_settings: PydanticBaseSettingsSource,
            file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (YamlConfigSettingsSource(settings_cls),)

    def get_server_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(
            certfile=self.server.tls.certfile,
            keyfile=self.server.tls.keyfile
        )
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=self.server.tls.cafile)

        return ctx

    def get_client_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.load_cert_chain(
            certfile=self.server.tls.certfile,
            keyfile=self.server.tls.keyfile
        )
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=self.server.tls.cafile)

        return ctx
