from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

from arti.backends.memory import MemoryBackend
from arti.backends.sqlite import SQLiteBackend
from arti.executors.concurrent import ConcurrentExecutor
from tests.arti.executors import graphs
from tests.arti.executors.graphs import (
    BACKEND_ENV,
    GRAPH_REF,
    N_PARTITIONS,
    RAW_ROOT_ENV,
    make_graph,
    write_raw,
)


def test_threads_build_partitions_concurrently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Every `square` partition waits for the others, so building them one at a time would time out.
    monkeypatch.setattr(graphs, "square_barrier", threading.Barrier(N_PARTITIONS, timeout=10))
    with SQLiteBackend(path=":memory:") as backend:
        graph = make_graph(backend, str(tmp_path))
        write_raw(graph)

        snapshot = graph.build(ConcurrentExecutor(max_workers=N_PARTITIONS))

        assert snapshot.read(graph.artifacts.next, annotation=int) == 31


def test_threads_stop_after_a_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(value: int) -> int:
        raise RuntimeError("boom")

    monkeypatch.setattr(graphs.double, "build", staticmethod(fail))
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    snapshot = graph.snapshot()

    with pytest.raises(RuntimeError, match="boom"):
        ConcurrentExecutor().build(snapshot)
    with graph.backend.connect() as connection:
        assert (
            connection.read_snapshot_partitions(snapshot, "doubled", graph.artifacts.doubled) == ()
        )


def test_processes_share_a_sqlite_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = tmp_path / "artigraph.sqlite"
    monkeypatch.setenv(BACKEND_ENV, str(database))
    monkeypatch.setenv(RAW_ROOT_ENV, str(tmp_path))
    with SQLiteBackend(path=database) as backend:
        graph = make_graph(backend, str(tmp_path))
        write_raw(graph)
        monkeypatch.setattr(graphs, "graph", graph)

        executor = ConcurrentExecutor(kind="process", graph_ref=GRAPH_REF, max_workers=2)
        snapshot = graph.build(executor)

        assert snapshot.read(graph.artifacts.next, annotation=int) == 31


def test_processes_require_shared_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    monkeypatch.setattr(graphs, "graph", graph)

    with pytest.raises(ValueError, match="must be shared across a host"):
        graph.build(ConcurrentExecutor(kind="process", graph_ref=GRAPH_REF))


def test_processes_require_the_same_graph(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with SQLiteBackend(path=tmp_path / "a.sqlite") as backend:
        graph = make_graph(backend, str(tmp_path))
        write_raw(graph)
        monkeypatch.setattr(graphs, "graph", make_graph(backend, str(tmp_path / "other")))

        with pytest.raises(ValueError, match="imports a different Graph"):
            graph.build(ConcurrentExecutor(kind="process", graph_ref=GRAPH_REF))


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"kind": "process"}, "`graph_ref` is required to build in processes"),
        ({"graph_ref": GRAPH_REF}, "`graph_ref` is only used to build in processes"),
    ],
)
def test_graph_ref_validation(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ConcurrentExecutor(**kwargs)
