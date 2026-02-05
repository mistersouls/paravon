from paravon.bootstrap.config.loader import get_cli_args
from paravon.bootstrap.deps import get_cp
from paravon.core.helpers.utils import setup_signal_handler, setup_logging, scan


@scan("paravon.bootstrap.handlers")
def main():
    controlplane = get_cp()
    loop = controlplane.loop
    cli = get_cli_args()

    setup_logging(cli.log_level)

    try:
        with setup_signal_handler() as stop_event:
            loop.run_until_complete(controlplane.start(stop_event))
    except KeyboardInterrupt:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            loop.close()
