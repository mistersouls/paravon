from functools import lru_cache

from paractl.core.cmd import ParaCmd
from paractl.infra.format_renderer import YamlRenderer
from paravon.infra.msgpack_serializer import MsgPackSerializer


@lru_cache
def get_cli():
    serializer = MsgPackSerializer()
    renderer = YamlRenderer(serializer)
    cli = ParaCmd(serializer, renderer)
    return cli
