from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from prefect.task_runners import ProcessPoolTaskRunner, TaskRunner, ThreadPoolTaskRunner

from arti.backends.memory import MemoryBackend
from arti.executors.prefect import PrefectExecutor, build_graph, required_reach
from tests.arti.executors import graphs
from tests.arti.executors.graphs import GRAPH_REF, make_graph, write_raw


def test_prefect_flow_snapshots_and_builds(
    prefect_harness: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    monkeypatch.setattr(graphs, "graph", graph)

    snapshot_id = build_graph(GRAPH_REF)

    snapshot = graph.snapshot()
    assert snapshot.id == snapshot_id
    assert snapshot.read(graph.artifacts.next, annotation=int) == 31


def test_prefect_checks_the_task_runner_reach(
    prefect_harness: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    monkeypatch.setattr(graphs, "graph", graph)

    executor = PrefectExecutor(graph_ref=GRAPH_REF, task_runner=ProcessPoolTaskRunner())
    with pytest.raises(ValueError, match="must be shared across a host"):
        graph.build(executor)


def test_required_reach() -> None:
    assert required_reach(ThreadPoolTaskRunner[Any]()) == "process"
    assert required_reach(ProcessPoolTaskRunner()) == "host"
    assert required_reach(Mock(spec=TaskRunner)) == "network"
