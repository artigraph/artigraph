from __future__ import annotations

from pathlib import Path

import pytest

from arti import Graph
from arti.backends.memory import MemoryBackend
from arti.backends.sqlite import SQLiteBackend
from arti.executors.airflow import group_id, make_dag
from arti.storage.literal import StringLiteral
from tests.arti.executors import graphs
from tests.arti.executors.graphs import GRAPH_REF, make_graph


def test_make_dag_structure(remote_graph: Graph) -> None:
    dag = make_dag(GRAPH_REF, schedule=None, tags=["arti"])

    assert dag.dag_id == "arti-executor-conformance"
    assert dag.tags == {"arti"}
    assert sorted(dag.task_ids) == [
        "doubled.build",
        "doubled.plan",
        "previous.build",
        "previous.plan",
        "snapshot",
        "squared.build",
        "squared.plan",
        "total.build",
        "total.plan",
    ]
    assert dag.get_task("squared.plan").upstream_task_ids == {"snapshot"}
    assert dag.get_task("total.plan").upstream_task_ids == {"snapshot", "squared.build"}
    assert dag.get_task("doubled.plan").upstream_task_ids == {"snapshot", "total.build"}
    assert dag.get_task("total.build").upstream_task_ids == {"total.plan"}


def test_make_dag_checks_reach(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with SQLiteBackend(path=tmp_path / "db.sqlite") as backend:
        monkeypatch.setattr(graphs, "graph", make_graph(backend, str(tmp_path)))
        with pytest.raises(ValueError, match="must be shared across a network"):
            make_dag(GRAPH_REF)
        assert make_dag(GRAPH_REF, dag_id="local", reach="host").dag_id == "local"

    monkeypatch.setattr(graphs, "graph", make_graph(MemoryBackend(), str(tmp_path)))
    with pytest.raises(ValueError, match="must be shared across a host"):
        make_dag(GRAPH_REF, reach="host")


def test_group_ids() -> None:
    assert group_id("root.clean") == "root__clean"


def test_make_dag_rejects_ambiguous_group_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    with Graph(name="ambiguous") as graph:
        graph.artifacts.value = graphs.Number(storage=StringLiteral(value="1"))
        graph.artifacts.a.b = graphs.double(value=graph.artifacts.value).out()
        graph.artifacts.a__b, graph.artifacts.c = graphs.neighbors(
            value=graph.artifacts.value
        ).out()
    monkeypatch.setattr(graphs, "graph", graph)

    with pytest.raises(ValueError, match="must map to unique Airflow group IDs"):
        make_dag(GRAPH_REF, reach="process")
