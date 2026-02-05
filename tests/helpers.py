import asyncio
import os
from dataclasses import dataclass

from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, YamlConfigSettingsSource

from paravon.bootstrap.config.settings import ParavonConfig


@dataclass
class TestClient:
    peer_reader: asyncio.StreamReader
    peer_writer: asyncio.StreamWriter
    api_reader: asyncio.StreamReader
    api_writer: asyncio.StreamWriter

    async def close(self) -> None:
        self.peer_writer.close()
        self.api_writer.close()
        await self.peer_writer.wait_closed()
        await self.api_writer.wait_closed()


class FakeParaConfig(ParavonConfig, BaseSettings):
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (YamlConfigSettingsSource(settings_cls, yaml_file=os.environ["TEST_PARANODECONFIG"]),)
