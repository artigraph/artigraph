"""Check that every Executor builds the same snapshot results for the same Graph."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from arti import Executor, Graph, GraphSnapshot
from arti.backends.memory import MemoryBackend
from arti.executors.concurrent import ConcurrentExecutor
from arti.executors.local import LocalExecutor
from arti.executors.work_units import encode_partition_key
from tests.arti.executors.graphs import GRAPH_REF, make_graph, sorted_rows

EXECUTORS: dict[str, Callable[[], Executor]] = {
    "local": LocalExecutor,
    "thread": lambda: ConcurrentExecutor(max_workers=4),
    "process": lambda: ConcurrentExecutor(kind="process", graph_ref=GRAPH_REF, max_workers=2),
}


def snapshot_results(snapshot: GraphSnapshot) -> dict[str, list[tuple[str, int]]]:
    """Summarize the partitions (keys and contents) linked to each Artifact of the snapshot."""
    with snapshot.backend.connect() as connection:
        return {
            key: sorted(
                (str(encode_partition_key(partition.partition_key)), partition.content_fingerprint)
                for partition in connection.read_snapshot_partitions(snapshot, key, artifact)
            )
            for key, artifact in snapshot.graph.artifacts.walk()
        }


def read_values(snapshot: GraphSnapshot) -> dict[str, Any]:
    return {
        key: sorted_rows(
            snapshot.read(artifact, annotation=list if key in {"raw", "squared"} else int)
        )
        for key, artifact in snapshot.graph.artifacts.walk()
    }


@pytest.fixture(params=sorted(EXECUTORS))
def executor(request: pytest.FixtureRequest) -> Executor:
    return EXECUTORS[request.param]()


def test_executor_conformance(executor: Executor, remote_graph: Graph) -> None:
    raw_root = f"gs://{remote_graph.artifacts.raw.storage.bucket}"
    reference = make_graph(MemoryBackend(), raw_root).build(LocalExecutor())

    snapshot = remote_graph.snapshot()
    executor.build(snapshot)

    assert snapshot.id == reference.id
    results = snapshot_results(snapshot)
    assert results == snapshot_results(reference)
    values = read_values(snapshot)
    assert values == read_values(reference)
    assert {key: value for key, value in values.items() if isinstance(value, int)} == {
        "total": 30,
        "doubled": 60,
        "previous": 29,
        "next": 31,
    }
    # Building again links the existing partitions without building new ones.
    executor.build(snapshot)
    assert snapshot_results(snapshot) == results
