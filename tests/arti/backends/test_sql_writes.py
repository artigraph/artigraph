from __future__ import annotations

import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import sqlalchemy as sa

from arti import Artifact, Graph, Storage, Type
from arti.backends.postgres import PostgresBackend
from arti.backends.sql.codec import MetadataCodecError
from arti.backends.sqlite import SQLiteBackend
from arti.formats.json import JSON
from arti.storage.literal import StringLiteral
from arti.types import Int64


class Scaled(Artifact):
    type: Type = Int64()
    format: JSON = JSON()
    storage: Storage = StringLiteral()
    # Postgres JSONB normalizes JSON numbers, eg: `1e+16` reads back as an int.
    scale: float = 1e16
    offset: float = -0.0


def build_graph(backend: SQLiteBackend | PostgresBackend, name: str = "writes") -> Graph:
    with Graph(name=name, backend=backend) as graph:
        graph.artifacts.value = Scaled(storage=StringLiteral(value="1"))
    return graph


def test_postgres_round_trips_floats(postgres_backend: PostgresBackend) -> None:
    graph = build_graph(postgres_backend)
    with postgres_backend.connect() as connection:
        connection.write_graph(graph)
    with postgres_backend.connect() as connection:
        restored = connection.read_graph(graph.name, graph.fingerprint)

    assert restored.fingerprint == graph.fingerprint


def test_postgres_concurrent_writes_of_the_same_graph(postgres_backend: PostgresBackend) -> None:
    graph = build_graph(postgres_backend)
    other = PostgresBackend(url=postgres_backend.url)

    def write_graph() -> None:
        with other.connect() as connection:
            connection.write_graph(graph)

    with ThreadPoolExecutor(1) as executor, postgres_backend.connect() as connection:
        connection.write_graph(graph)
        other_write = executor.submit(write_graph)
        # Commit only once the other writer is blocked on our uncommitted primary key.
        with postgres_backend.engine.connect() as monitor:
            deadline = time.monotonic() + 10
            while not monitor.execute(
                sa.text(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'"
                )
            ).scalar_one():
                assert time.monotonic() < deadline, "The other writer never blocked"
                time.sleep(0.01)
    other_write.result()
    other.close()


def test_sqlite_transactions_take_the_write_lock(tmp_path: Path) -> None:
    # Deferred transactions that read and then write fail immediately (ignoring busy_timeout) if
    # another connection committed in between, so each transaction locks up front instead.
    path = tmp_path / "artigraph.sqlite"
    backend = SQLiteBackend(path=path)
    graph = build_graph(backend)
    other = sqlite3.connect(path, isolation_level=None, timeout=0)
    with backend.connect() as connection:
        connection.read_artifact_partitions(graph.artifacts.value)
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            other.execute("BEGIN IMMEDIATE")
    other.execute("BEGIN IMMEDIATE")
    other.execute("ROLLBACK")
    other.close()
    backend.close()


def test_sql_partition_reads_validate_fingerprints() -> None:
    backend = SQLiteBackend(path=":memory:")
    graph = build_graph(backend)
    snapshot = graph.snapshot()
    artifact = snapshot.artifacts.value
    with backend.engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE arti_storage_partition SET metadata_json = json_set(metadata_json, '$.value.fields.content_fingerprint.value', 1)"
            )
        )

    with backend.connect() as connection:
        with pytest.raises(MetadataCodecError, match="fingerprint mismatch"):
            connection.read_artifact_partitions(artifact)
        with pytest.raises(MetadataCodecError, match="fingerprint mismatch"):
            connection.read_snapshot_partitions(snapshot, "value", artifact)
    backend.close()
