from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest
from distributed import Client, LocalCluster

from arti import Graph
from arti.backends.memory import MemoryBackend
from arti.executors.dask import DaskExecutor
from tests.arti.executors import graphs
from tests.arti.executors.graphs import GRAPH_REF, make_graph, write_raw


@pytest.fixture
def threaded_client(remote_graph: Graph) -> Generator[Client]:
    """Run Dask workers in this process, so they see monkeypatched code."""
    with (
        LocalCluster(n_workers=1, processes=False, dashboard_address=None) as cluster,
        Client(cluster) as client,
    ):
        yield client


def test_dask_connects_to_an_address(remote_graph: Graph, dask_cluster: LocalCluster) -> None:
    executor = DaskExecutor(graph_ref=GRAPH_REF, address=dask_cluster.scheduler_address)

    snapshot = remote_graph.build(executor)

    assert snapshot.read(remote_graph.artifacts.next, annotation=int) == 31


def test_dask_stops_after_a_failure(
    remote_graph: Graph, threaded_client: Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(rows: list[dict[str, int]]) -> int:
        raise RuntimeError("boom")

    monkeypatch.setattr(graphs.total, "build", staticmethod(fail))
    snapshot = remote_graph.snapshot()

    with pytest.raises(RuntimeError, match="boom"):
        DaskExecutor(graph_ref=GRAPH_REF, client=threaded_client).build(snapshot)
    with remote_graph.backend.connect() as connection:
        doubled = remote_graph.artifacts.doubled
        assert connection.read_snapshot_partitions(snapshot, "doubled", doubled) == ()


def test_dask_requires_shared_resources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    graph = make_graph(MemoryBackend(), str(tmp_path))
    write_raw(graph)
    monkeypatch.setattr(graphs, "graph", graph)

    with pytest.raises(ValueError, match="must be shared across a network"):
        graph.build(DaskExecutor(graph_ref=GRAPH_REF))


def test_dask_client_or_address() -> None:
    with pytest.raises(ValueError, match="either a `client` or an `address`"):
        DaskExecutor(graph_ref=GRAPH_REF, address="tcp://localhost:8786", client=object())  # type: ignore[arg-type]
