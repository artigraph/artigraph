from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

import arti
from arti import dashboard
from arti.backends.memory import MemoryBackend
from arti.backends.sqlite import SQLiteBackend
from arti.cli import TargetError, load_target, main
from arti.dashboard import DashboardData
from arti.graphs import Graph
from tests.arti.dashboard.example import build_graph, write_transactions

backend = MemoryBackend()
graph = Graph(name="cli", backend=backend)
not_a_target = 1


def run_dashboard(*args: str) -> tuple[DashboardData, mock.MagicMock, mock.MagicMock]:
    """Run `arti dashboard`, stopping it (like Ctrl+C) instead of serving forever."""
    with (
        mock.patch("arti.cli.webbrowser.open") as open_,
        mock.patch("arti.dashboard._server.HTTPServer.serve_forever") as serve,
        mock.patch("arti.dashboard.make_server", wraps=dashboard.make_server) as make,
    ):
        serve.side_effect = KeyboardInterrupt
        assert main(["dashboard", *args, "--port", "0"]) == 0
    serve.assert_called_once_with()
    data = make.call_args.args[0]
    assert isinstance(data, DashboardData)
    return data, open_, make


def test_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out == f"arti {arti.__version__}\n"


def test_console_script() -> None:
    result = subprocess.run(  # noqa: S603 - run the installed entry point with this interpreter
        [Path(sys.executable).with_name("arti"), "dashboard", "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "module:attribute" in result.stdout


def test_load_target() -> None:
    assert load_target("tests.arti.test_cli:backend") is backend
    assert load_target("tests.arti.test_cli:graph") is graph

    for target, match in [
        ("tests.arti.test_cli", "Expected a `module:attribute` target"),
        (":backend", "Expected a `module:attribute` target"),
        ("tests.arti.missing:backend", "Unable to import 'tests.arti.missing'"),
        ("tests.arti.test_cli:missing", "has no attribute 'missing'"),
        ("tests.arti.test_cli:not_a_target", "must be a Backend or Graph, got int"),
    ]:
        with pytest.raises(TargetError, match=match):
            load_target(target)


def test_load_target_imports_from_the_current_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "local_module_for_cli.py").write_text(
        "from arti.backends.memory import MemoryBackend\nbackend = MemoryBackend()\n"
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != ""])
    monkeypatch.delitem(sys.modules, "local_module_for_cli", raising=False)

    assert isinstance(load_target("local_module_for_cli:backend"), MemoryBackend)


def test_dashboard_with_a_backend(capsys: pytest.CaptureFixture[str]) -> None:
    data, open_, make = run_dashboard("tests.arti.test_cli:backend", "--open")

    assert data.backend is backend
    assert data.definitions == {}
    assert make.call_args.kwargs == {"host": "127.0.0.1", "port": 0}
    out = capsys.readouterr().out
    assert out.startswith("Serving the Artigraph dashboard for tests.arti.test_cli:backend at ")
    open_.assert_called_once()


def test_dashboard_with_a_graph() -> None:
    data, open_, _ = run_dashboard("tests.arti.test_cli:graph")

    assert data.backend is backend
    assert list(data.definitions.values()) == [graph]
    open_.assert_not_called()


def test_dashboard_with_sqlite(tmp_path: Path) -> None:
    path = tmp_path / "metadata.sqlite"
    write_transactions(tmp_path)
    with SQLiteBackend(path=path) as sqlite:
        build_graph(tmp_path, sqlite).build()

    close = SQLiteBackend.close
    with mock.patch.object(SQLiteBackend, "close", autospec=True, side_effect=close) as closed:
        data, _, _ = run_dashboard("--sqlite", str(path))

    assert isinstance(data.backend, SQLiteBackend)
    assert data.backend.path == path
    assert data.backend.auto_upgrade is False
    # The CLI opened the backend, so it closes it too.
    closed.assert_called_once_with(data.backend)


@pytest.mark.parametrize(
    ("args", "message"),
    [
        ([], "Pass either a `module:attribute` target or `--sqlite PATH`"),
        (["tests.arti.test_cli:backend", "--sqlite", "x"], "Pass either"),
        (["--sqlite", "missing.sqlite"], "No SQLite database at 'missing.sqlite'"),
        (["tests.arti.test_cli:not_a_target"], "must be a Backend or Graph"),
    ],
)
def test_dashboard_rejects_bad_targets(
    args: list[str], message: str, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["dashboard", *args])
    assert exc.value.code == 2
    assert message in capsys.readouterr().err
