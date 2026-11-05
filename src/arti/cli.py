"""The `arti` command line interface."""

from __future__ import annotations

import argparse
import contextlib
import importlib
import sys
import webbrowser
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import arti
from arti.backends import Backend
from arti.graphs import Graph


class TargetError(ValueError):
    """Raised when a `module:attribute` target can't be loaded."""


def _make_cwd_importable() -> None:
    # Like `python -m`, so local modules (and the types recorded in a Backend) can be imported.
    if "" not in sys.path:
        sys.path.insert(0, "")


def load_target(target: str) -> Backend[Any] | Graph:
    """
    Import a `module:attribute` reference to a Backend or Graph, like `my_project.graphs:graph`.

    The current directory is importable (like `python -m`), so local modules can be referenced.
    Importing the module runs it, so only load trusted code.
    """
    module_name, separator, attribute = target.partition(":")
    if not separator or not module_name or not attribute:
        raise TargetError(f"Expected a `module:attribute` target, got {target!r}")
    _make_cwd_importable()
    try:
        value: Any = importlib.import_module(module_name)
    except ImportError as error:
        raise TargetError(f"Unable to import {module_name!r}: {error}") from error
    for part in attribute.split("."):
        try:
            value = getattr(value, part)
        except AttributeError as error:
            raise TargetError(f"{target!r} has no attribute {part!r}") from error
    if not isinstance(value, Backend | Graph):
        raise TargetError(f"{target!r} must be a Backend or Graph, got {type(value).__name__}")
    return cast(Backend[Any] | Graph, value)


def _dashboard(args: argparse.Namespace) -> int:
    from arti.dashboard import DashboardData, make_server

    if (args.target is None) == (args.sqlite is None):
        raise TargetError("Pass either a `module:attribute` target or `--sqlite PATH`")
    if args.sqlite is not None:
        from arti.backends.sqlite import SQLiteBackend

        if not args.sqlite.is_file():
            raise TargetError(f"No SQLite database at {str(args.sqlite)!r}")
        _make_cwd_importable()
        # Never migrate a database that is only being browsed.
        target: Backend[Any] | Graph = SQLiteBackend(path=args.sqlite, auto_upgrade=False)
        name = str(args.sqlite)
    else:
        target = load_target(args.target)
        name = args.target
    if isinstance(target, Graph):
        data = DashboardData(target.backend, definitions=[target])
    else:
        data = DashboardData(target)
    try:
        with make_server(data, host=args.host, port=args.port) as server:
            host, port = server.server_address[:2]
            url = f"http://{host}:{port}/"
            print(f"Serving the Artigraph dashboard for {name} at {url} (Ctrl+C to stop)")
            if args.open:
                webbrowser.open(url)
            with contextlib.suppress(KeyboardInterrupt):
                server.serve_forever()
    finally:
        if args.sqlite is not None:
            data.backend.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="arti", description="Artigraph command line tools.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {arti.__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    dashboard = subparsers.add_parser(
        "dashboard",
        help="browse graphs, snapshots, and partitions in a web page",
        description=" ".join(
            [
                "Serve a read-only dashboard of the metadata in a Backend.",
                "With a Graph target, the dashboard browses the Graph's Backend and also shows the",
                "Graph's current definition, even if it was never snapshotted.",
            ]
        ),
    )
    dashboard.add_argument(
        "target",
        nargs="?",
        help="a `module:attribute` reference to a Backend or Graph, eg: `my_project.graphs:graph`",
    )
    dashboard.add_argument(
        "--sqlite",
        type=Path,
        metavar="PATH",
        help="browse a SQLite metadata database instead of a `module:attribute` target",
    )
    dashboard.add_argument(
        "--host", default="127.0.0.1", help="address to bind (default: %(default)s)"
    )
    dashboard.add_argument(
        "--port", default=8000, type=int, help="port to bind, or 0 for any (default: %(default)s)"
    )
    dashboard.add_argument("--open", action="store_true", help="open the dashboard in a browser")
    dashboard.set_defaults(handler=_dashboard)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except TargetError as error:
        parser.exit(2, f"arti: error: {error}\n")
