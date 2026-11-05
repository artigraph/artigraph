from __future__ import annotations

from collections.abc import Generator

import gcsfs
import pytest

from arti import Graph
from arti.backends.postgres import PostgresBackend
from tests.arti.executors import graphs
from tests.arti.executors.graphs import BACKEND_ENV, RAW_ROOT_ENV, make_graph, write_raw

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
