from paractl.bootstrap.deps import get_cli
from paravon.core.helpers.utils import scan


@scan("paractl.bootstrap.commands")
def main():
    cli = get_cli()

    try:
        if cli.interactive:
            cli.cmdloop()
        else:
            cli.onecmd(cli.args.namespace)
    finally:
        cli.close()


if __name__ == "__main__":
    main()
