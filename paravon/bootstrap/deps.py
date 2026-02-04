import json
from functools import lru_cache

from pydantic import ValidationError

from paravon.bootstrap.config.settings import ParavonConfig
from paravon.core.controlplane import ControlPlane
from paravon.core.facade import ParaCore
from paravon.core.routing.app import RoutedApplication
from paravon.infra.msgpack_serializer import MsgPackSerializer


@lru_cache
def get_cp() -> ControlPlane:
    return ControlPlane(
        config=get_config(),
        api_app=get_api_app(),
        peer_app=get_peer_app(),
        serializer=MsgPackSerializer()
    )


@lru_cache
def get_core() -> ParaCore:
    cp = get_cp()
    return cp.build_core()


@lru_cache
def get_api_app() -> RoutedApplication:
    app = RoutedApplication()
    return app


@lru_cache
def get_peer_app() -> RoutedApplication:
    app = RoutedApplication()
    return app


@lru_cache
def get_config() -> ParavonConfig:
    try:
        return ParavonConfig()  # type: ignore[call-arg]
    except FileNotFoundError as ex:
        raise SystemExit(f"Provide a correct configuration file path: {ex}")
    except ValidationError as ex:
        msg = ["Configuration validation failed:"]
        errs = json.loads(ex.json())
        for err in errs:
            msg.append(f"  {'.'.join(err['loc'])}: {err['msg']}")
        raise SystemExit("\n".join(msg))
