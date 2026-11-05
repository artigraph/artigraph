from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from arti import Fingerprint, Graph
from arti.backends.memory import MemoryBackend
from arti.backends.postgres import PostgresBackend
from arti.backends.sqlite import SQLiteBackend
from arti.executors import producers_in_order
from arti.executors.local import LocalExecutor
from arti.executors.work_units import (
    WorkUnit,
    check_graph_ref,
    check_reach,
    plan_work_units,
    producer_key,
    resolve_graph,
    resolve_producer,
    resolve_snapshot,
    run_work_unit,
    snapshot_graph,
)
from tests.arti.executors import graphs
from tests.arti.executors.graphs import GRAPH_REF, make_graph, sorted_rows, write_raw

not_a_graph = object()


def replace(unit: WorkUnit, **changes: Any) -> WorkUnit:
    return WorkUnit(**{name: getattr(unit, name) for name in WorkUnit.model_fields} | changes)


@pytest.fixture
def graph(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Graph:
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    monkeypatch.setattr(graphs, "graph", graph)
    return graph


def read_all(graph: Graph, snapshot_id: Fingerprint) -> dict[str, Any]:
    snapshot = resolve_snapshot(GRAPH_REF, snapshot_id)
    assert snapshot.graph is graph
    return {
        key: sorted_rows(
            snapshot.read(artifact, annotation=list if key in {"raw", "squared"} else int)
        )
        for key, artifact in graph.artifacts.walk()
    }


def test_work_units_build_the_same_results(graph: Graph) -> None:
    snapshot_id = snapshot_graph(GRAPH_REF)
    for producer in producers_in_order(graph):
        units = plan_work_units(GRAPH_REF, snapshot_id, producer_key(graph, producer))
        assert units
        for unit in units:
            assert WorkUnit.from_json(unit.to_json()) == unit
            run_work_unit(unit)
            run_work_unit(unit)  # Retries are no-ops
        assert plan_work_units(GRAPH_REF, snapshot_id, producer_key(graph, producer)) == ()

    reference = make_graph(MemoryBackend(), str(Path(graph.artifacts.raw.storage.path).parents[1]))
    expected = reference.build(LocalExecutor())
    assert expected.id == snapshot_id
    assert read_all(graph, snapshot_id) == {
        key: sorted_rows(
            expected.read(artifact, annotation=list if key in {"raw", "squared"} else int)
        )
        for key, artifact in reference.artifacts.walk()
    }


def test_work_unit_labels(graph: Graph) -> None:
    snapshot_id = snapshot_graph(GRAPH_REF)
    units = plan_work_units(GRAPH_REF, snapshot_id, "squared")
    assert sorted(unit.label for unit in units) == [f"squared[i.value={i}]" for i in range(4)]
    assert replace(units[0], partition_key={}).label == "squared"


def test_run_work_unit_rejects_stale_units(graph: Graph) -> None:
    snapshot_id = snapshot_graph(GRAPH_REF)
    (unit, *_) = plan_work_units(GRAPH_REF, snapshot_id, "squared")

    unknown = replace(unit, partition_key={"i": {"value": "99"}})
    with pytest.raises(ValueError, match=r"squared\[i.value=99\] is not a partition"):
        run_work_unit(unknown)
    changed = replace(unit, input_fingerprint=Fingerprint.from_int(1))
    with pytest.raises(ValueError, match="inputs changed since planning"):
        run_work_unit(changed)


def test_resolve_snapshot_detects_drift(graph: Graph) -> None:
    with pytest.raises(ValueError, match="is not in the Backend"):
        resolve_snapshot(GRAPH_REF, Fingerprint.from_int(1))


def test_resolve_producer(graph: Graph) -> None:
    assert resolve_producer(graph, "previous") is resolve_producer(graph, "previous")
    with pytest.raises(ValueError, match="`next` is not the first output"):
        resolve_producer(graph, "next")
    with pytest.raises(ValueError, match="`raw` is not the first output"):
        resolve_producer(graph, "raw")


def test_resolve_graph(graph: Graph) -> None:
    assert resolve_graph(GRAPH_REF) is graph
    assert resolve_graph("tests.arti.executors:graphs.graph") is graph
    for invalid in ("tests.arti.executors.graphs", ":graph", "tests.arti.executors.graphs:"):
        with pytest.raises(ValueError, match="Expected a graph reference"):
            resolve_graph(invalid)
    with pytest.raises(TypeError, match="must reference a Graph, got: object"):
        resolve_graph(f"{__name__}:not_a_graph")


def test_check_graph_ref(graph: Graph, tmp_path: Path) -> None:
    check_graph_ref(GRAPH_REF, graph)
    other = make_graph(MemoryBackend(), str(tmp_path / "other"))
    with pytest.raises(ValueError, match="imports a different Graph"):
        check_graph_ref(GRAPH_REF, other)
    copy = graph.model_copy(update={"backend": MemoryBackend()})
    with pytest.raises(ValueError, match="imports a Graph with a different MemoryBackend"):
        check_graph_ref(GRAPH_REF, copy)


def test_check_reach(tmp_path: Path) -> None:
    local = make_graph(MemoryBackend(), str(tmp_path))
    check_reach(local, "process")
    with pytest.raises(
        ValueError,
        match=r"across a host, but these can't be: the MemoryBackend \(reach: process\)\.",
    ):
        check_reach(local, "host")

    sqlite = make_graph(SQLiteBackend(path=tmp_path / "db.sqlite"), str(tmp_path))
    check_reach(sqlite, "host")
    with pytest.raises(
        ValueError,
        match=r"the SQLiteBackend \(reach: host\), `raw`'s LocalFile \(reach: host\)\.",
    ):
        check_reach(sqlite, "network")
    with pytest.raises(ValueError, match=r"the SQLiteBackend \(reach: process\)\."):
        check_reach(
            make_graph(SQLiteBackend(path=":memory:"), str(tmp_path)),
            "host",
        )

    remote = make_graph(PostgresBackend.from_url("postgresql://unused"), "gs://bucket")
    check_reach(remote, "network")
