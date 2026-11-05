from __future__ import annotations

from collections.abc import Generator

import gcsfs
import pytest
from distributed import Client, LocalCluster

from arti import Graph
from arti.backends.postgres import PostgresBackend
from tests.arti.executors import graphs
from tests.arti.executors.graphs import BACKEND_ENV, RAW_ROOT_ENV, make_graph, write_raw


@pytest.fixture(scope="session")
def prefect_harness() -> Generator[None]:
    """Run Prefect flows against a temporary Prefect server and database."""
    from prefect.testing.utilities import prefect_test_harness

    with prefect_test_harness():
        yield


# A fixed bucket keeps the Graph (and snapshot) identical across tests and executors.
CONFORMANCE_BUCKET = "arti-executor-conformance"


@pytest.fixture
def remote_graph(
    gcs: gcsfs.GCSFileSystem,
    monkeypatch: pytest.MonkeyPatch,
    postgres_backend: PostgresBackend,
) -> Generator[Graph]:
    """
    Prepare `graphs.graph` with a Postgres Backend and raw data in (emulated) GCS.

    Other processes build the same Graph from the environment variables.
    """
    raw_root = f"gs://{CONFORMANCE_BUCKET}"
    gcs.mkdir(CONFORMANCE_BUCKET)
    try:
        monkeypatch.setenv(BACKEND_ENV, postgres_backend.url.get_secret_value())
        monkeypatch.setenv(RAW_ROOT_ENV, raw_root)
        graph = make_graph(postgres_backend, raw_root)
        write_raw(graph)
        monkeypatch.setattr(graphs, "graph", graph)
        yield graph
    finally:
        gcs.rm(CONFORMANCE_BUCKET, recursive=True)


@pytest.fixture
def dask_cluster(remote_graph: Graph) -> Generator[LocalCluster]:
    """Start Dask worker processes, which build `remote_graph` from the environment variables."""
    with LocalCluster(
        n_workers=2, threads_per_worker=1, processes=True, dashboard_address=None
    ) as cluster:
        yield cluster


@pytest.fixture
def dask_client(dask_cluster: LocalCluster) -> Generator[Client]:
    with Client(dask_cluster) as client:
        yield client
