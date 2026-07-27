import logging
import runpy
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
from docs.examples.snapshot_parquet_duckdb.demo import GRAPH_NAME, run_demo
from docs.examples.snapshot_parquet_duckdb.snapshot_duckdb import (
    register_snapshot_view,
    snapshot_parquet_paths,
)

from arti import GraphSnapshot
from arti.backends.sqlite import SQLiteBackend
from arti.formats.json import JSON


def test_spend_example(caplog: pytest.LogCaptureFixture) -> None:
    example = Path(__file__).parents[1] / "docs" / "examples" / "spend" / "demo.py"

    with caplog.at_level(logging.INFO):
        runpy.run_path(str(example), run_name="__main__")

    assert "Total Spend: 42.0" in caplog.messages


def test_snapshot_parquet_duckdb_example(tmp_path: Path) -> None:
    example = (
        Path(__file__).parents[1] / "docs" / "examples" / "snapshot_parquet_duckdb" / "demo.py"
    )
    work_dir = tmp_path / "demo"

    result = subprocess.run(  # noqa: S603 - execute the checked-in example with this interpreter
        [sys.executable, str(example), "--work-dir", str(work_dir)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Baseline snapshot total: 42.00" in result.stdout
    assert "Corrected snapshot total: 39.50" in result.stdout
    assert "Naive glob total: 51.50" in result.stdout
    assert len(list((work_dir / "data" / "produced").rglob("*.parquet"))) == 3
    assert not list(work_dir.rglob("*.duckdb"))


def test_snapshot_duckdb_rejects_changed_partition(tmp_path: Path) -> None:
    work_dir = tmp_path / "demo"
    assert run_demo(work_dir) == {
        "baseline": 42.0,
        "corrected": 39.5,
        "naive": 51.5,
    }

    backend = SQLiteBackend(path=work_dir / "artigraph.sqlite")
    snapshot = GraphSnapshot.from_tag(
        GRAPH_NAME,
        "baseline",
        connectable=backend,
    )
    corrected = GraphSnapshot.from_tag(
        GRAPH_NAME,
        "corrected",
        connectable=backend,
    )
    artifact = snapshot.artifacts.clean
    paths = snapshot_parquet_paths(snapshot, artifact)
    corrected_paths = snapshot_parquet_paths(corrected, corrected.artifacts.clean)
    assert len(paths) == len(corrected_paths) == 2
    assert len(set(paths) & set(corrected_paths)) == 1
    assert len(set(paths) | set(corrected_paths)) == 3

    with pytest.raises(TypeError, match="only supports Parquet"):
        snapshot_parquet_paths(
            snapshot,
            artifact.model_copy(update={"format": JSON()}),
        )

    changed_path = Path(paths[0])
    with changed_path.open("ab") as file:
        file.write(b"changed")
    with duckdb.connect(":memory:") as connection:
        with pytest.raises(ValueError, match="Partition content changed since snapshot"):
            register_snapshot_view(
                connection,
                snapshot=snapshot,
                artifact=artifact,
                view_name="changed_transactions",
            )

    changed_path.unlink()
    with pytest.raises(FileNotFoundError, match="Snapshot partition is missing"):
        snapshot_parquet_paths(snapshot, artifact)


def test_minimal_import_without_parquet_dependencies() -> None:
    root = Path(__file__).parents[1]
    spend = root / "docs" / "examples" / "spend" / "demo.py"
    code = f"""
import importlib.abc
import importlib
import runpy
import sys

class BlockOptionalImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split(".", 1)[0] in {{"duckdb", "pandas", "pyarrow"}}:
            raise ModuleNotFoundError(name=fullname)
        return None

sys.meta_path.insert(0, BlockOptionalImports())
import arti
runpy.run_path({str(spend)!r}, run_name="__main__")
try:
    importlib.import_module("arti.formats.parquet")
except ModuleNotFoundError as error:
    assert "Parquet support requires the `parquet` extra" in str(error)
else:
    raise AssertionError("Parquet unexpectedly imported")
"""

    subprocess.run(  # noqa: S603 - execute a fixed import-safety probe with this interpreter
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        cwd=root,
    )
