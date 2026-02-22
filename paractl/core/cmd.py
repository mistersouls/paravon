import argparse
import cmd
import shlex
import ssl

from paractl.core.client import ParavonClient
from paractl.core.dispatcher import CommandDispatcher, CommandHandler
from paractl.core.loader import ParaConfLoader
from paractl.core.parser import validate_key, parse_value, ParseError, split_key_value
from paractl.core.ports.render import Renderer
from paractl.core.utils import resolve_context
from paravon.core.ports.serializer import Serializer


class ParaCmd(cmd.Cmd):
    intro = "Entering paractl interactive mode. Type 'exit' or 'quit' to leave."
    prompt = "paractl> "

    def __init__(self, serializer: Serializer, renderer: Renderer) -> None:
        super().__init__()

        self._serializer = serializer
        self._renderer = renderer
        self._argparser = self._argparse()
        self._args = self._argparser.parse_args()
        self._loader = ParaConfLoader(self._args.paraconf)
        self._client: ParavonClient = self._get_client()
        self._dispatcher = CommandDispatcher()

    @property
    def args(self) -> argparse.Namespace:
        return self._args

    @property
    def interactive(self) -> bool:
        return self._args.namespace is None

    def command(self, *arguments: str) -> CommandHandler:
        return self._dispatcher.command(*arguments)

    def close(self):
        self._client.close()

    def handle(self, *arguments: str) -> None:
        try:
            msg = self._dispatcher.dispatch(
                *arguments,
                client=self._client,
                loader=self._loader,
                namespace=self.args
            )
            print(self._renderer.render(msg.to_dict()))
        except Exception as ex:
            print(str(ex))

    def do_admin(self, line):
        admin_cmd = getattr(self.args, "admin_cmd", None)
        if admin_cmd is None:
            argv = shlex.split(line)
            if not argv:
                print(
                    "Usage: admin [argument <join|drain|remove>]\n"
                    "admin argument is required."
                )
                return

            admin_cmd = argv[0]

        self.handle("admin", admin_cmd)

    def do_config(self, line):
        if self.interactive:
            print("config command is not support in interactive mode.")
            self._argparser.print_usage()
            return

        config_cmd = getattr(self.args, "config_cmd", None)
        if config_cmd is None:
            print(
                "Usage: config [argument <current-context|get-contexts|use-context>]\n"
                "config argument is required."
            )
            return

        self.handle("config", config_cmd)

    def do_get(self, line):
        key = line or getattr(self.args, "key", None)
        if not key:
            print("Usage: get [argument <key>]")
            return

        parts = shlex.split(key)

        if len(parts) != 1:
            print("Usage: get <key>")
            return

        self._args.key = self._serializer.serialize(key)
        self.handle("get")
        self._args.key = None

    def do_put(self, line):
        argv = split_key_value(line) if line else []

        key = getattr(self.args, "key", None)
        value = getattr(self.args, "value", None)

        # Interactive mode
        if self.interactive:
            if len(argv) != 2:
                print("Usage: put <key> <value>")
                return
            key, value = argv

        if key is None or value is None:
            print("Usage: put <key> <value>")
            return

        # Validate + parse
        try:
            key = validate_key(key)
            value = parse_value(value)
        except ParseError as ex:
            print(str(ex))
            return

        # Serialize
        self._args.key = self._serializer.serialize(key)
        self._args.value = self._serializer.serialize(value)

        # Dispatch
        self.handle("put")

        # Cleanup
        self._args.key = None
        self._args.value = None

    def do_delete(self, line):
        key = line or getattr(self.args, "key", None)
        if not key:
            print("Usage: delete [argument <key>]")
            return

        parts = shlex.split(key)

        if len(parts) != 1:
            print("Usage: delete <key>")
            return

        self._args.key = self._serializer.serialize(key)
        self.handle("delete")
        self._args.key = None

    def do_exit(self, arg):
        return True

    def do_quit(self, arg):
        return True

    def do_EOF(self, arg):
        print()
        return True

    def _get_client(self) -> ParavonClient:
        conf = self._loader.load()
        ctx_name, ctx = resolve_context(
            conf,
            self._args.context,
            self._args.server,
        )

        host, port_str = ctx.server.split(":", 1)
        port = int(port_str)

        ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if ctx.tls:
            ssl_ctx.load_cert_chain(ctx.tls.cert, ctx.tls.key)
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
            ssl_ctx.load_verify_locations(cafile=ctx.tls.ca)

        client = ParavonClient(host, port, ssl_ctx, self._serializer)
        self.prompt = f"paractl({ctx_name})# "
        return client

    @staticmethod
    def _argparse() -> argparse.ArgumentParser:
        global_opts = argparse.ArgumentParser(prog="paractl")
        global_opts.add_argument("--paraconf")
        global_opts.add_argument("--context")
        global_opts.add_argument("--server")

        sub = global_opts.add_subparsers(dest="namespace")

        cfg = sub.add_parser("config")
        cfg_sub = cfg.add_subparsers(dest="config_cmd", required=True)
        cfg_sub.add_parser("current-context")
        cfg_sub.add_parser("get-contexts")
        use_ctx = cfg_sub.add_parser("use-context")
        use_ctx.add_argument("name")

        admin = sub.add_parser("admin")
        admin_sub = admin.add_subparsers(required=True, dest="admin_cmd")
        admin_sub.add_parser("join")
        admin_sub.add_parser("drain")
        admin_sub.add_parser("remove")
        admin_sub.add_parser("node-status")

        get = sub.add_parser("get")
        get.add_argument("key")

        put = sub.add_parser("put")
        put.add_argument("key")
        put.add_argument("value")

        delete = sub.add_parser("delete")
        delete.add_argument("key")

        return global_opts
