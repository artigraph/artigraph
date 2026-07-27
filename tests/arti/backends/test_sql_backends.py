from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy.pool import StaticPool

from arti import Fingerprint, Graph, GraphSnapshot
from arti.backends.postgres import PostgresBackend
from arti.backends.sql.backend import BackendSchemaError
from arti.backends.sql.codec import MetadataCodecError
from arti.backends.sql.models import GraphRow, StoragePartitionRow
from arti.backends.sqlite import SQLiteBackend
from arti.storage.literal import StringLiteral
from tests.arti.backends.pipeline import Number, build_pipeline


def test_sqlite_in_memory_pipeline() -> None:
    backend = SQLiteBackend(path=":memory:")
    snapshot = build_pipeline(backend, "sqlite-memory")

    assert isinstance(backend.engine.pool, StaticPool)
    assert snapshot.read(snapshot.artifacts.incremented, annotation=int) == 2
    assert (
        GraphSnapshot.from_tag(snapshot.name, "latest", connectable=backend).fingerprint
        == snapshot.fingerprint
    )
    backend.close()


def test_sqlite_requires_a_path() -> None:
    with pytest.raises(ValidationError, match="path"):
        SQLiteBackend()  # pyright: ignore[reportCallIssue]


def test_sql_backend_rejects_use_after_close() -> None:
    with SQLiteBackend(path=":memory:") as backend:
        build_pipeline(backend, "closed")
    with pytest.raises(RuntimeError, match="SQLiteBackend is closed"):
        backend.connect().__enter__()
    backend.close()


def test_sqlite_file_pipeline_reopens_with_structured_metadata(tmp_path: Path) -> None:
    path = tmp_path / "artigraph.sqlite"
    backend = SQLiteBackend(path=path)
    snapshot = build_pipeline(backend, "sqlite-file")
    with backend.engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
        metadata = connection.execute(sa.text("SELECT metadata_json FROM arti_graph")).scalar_one()
        assert isinstance(metadata, str)
        assert '"type": "arti.graphs:Graph"' in metadata
    backend.close()

    reopened = SQLiteBackend(path=path)
    restored = GraphSnapshot.from_tag(snapshot.name, "latest", connectable=reopened)

    assert restored.read(restored.artifacts.incremented, annotation=int) == 2
    assert restored.backend is reopened
    reopened.close()


def test_sqlite_manual_upgrade_and_rollback() -> None:
    backend = SQLiteBackend(path=":memory:", auto_upgrade=False)
    with pytest.raises(BackendSchemaError, match=r"Call backend\.upgrade"):
        backend.connect().__enter__()

    backend.upgrade()
    with Graph(name="rolled-back", backend=backend) as graph:
        graph.artifacts.value = Number(storage=StringLiteral(value="1"))

    def write_then_rollback() -> None:
        with backend.connect() as connection:
            connection.write_graph(graph)
            raise RuntimeError("rollback")

    with pytest.raises(RuntimeError, match="rollback"):
        write_then_rollback()
    with backend.connect() as connection:
        with pytest.raises(KeyError):
            connection.read_graph(graph.name, graph.fingerprint)
    backend.close()


def test_sql_backend_snapshot_tags() -> None:
    backend = SQLiteBackend(path=":memory:")
    snapshot = build_pipeline(backend, "tags")
    retagged = GraphSnapshot(id=snapshot.id.combine(Fingerprint.from_int(1)), graph=snapshot.graph)

    with backend.connect() as connection:
        with pytest.raises(KeyError):
            connection.read_snapshot("missing", Fingerprint.from_int(1))
        with pytest.raises(ValueError, match="No known `missing` tag"):
            connection.read_snapshot_tag(snapshot.name, "missing")
        with pytest.raises(ValueError, match="Existing `latest` tag"):
            connection.write_snapshot_tag(snapshot, "latest")
        connection.write_snapshot(retagged)
        connection.write_snapshot_tag(retagged, "latest", overwrite=True)
        assert (
            connection.read_snapshot_tag(snapshot.name, "latest").fingerprint
            == retagged.fingerprint
        )
    backend.close()


def test_sql_backend_rejects_mismatched_partitions() -> None:
    backend = SQLiteBackend(path=":memory:")
    snapshot = build_pipeline(backend, "mismatched")
    output = snapshot.artifacts.incremented
    other = Number(storage=StringLiteral(id="other", value="10"))
    wrong = other.storage.discover_partitions()
    assert wrong

    with backend.connect() as connection:
        with pytest.raises(ValueError, match="does not match Artifact storage"):
            connection.write_artifact_partitions(output, wrong)
        with pytest.raises(ValueError, match="does not match Artifact storage"):
            connection.write_snapshot_partitions(snapshot, "incremented", output, wrong)
    backend.close()


def test_sql_backend_detects_corrupted_graph_metadata() -> None:
    backend = SQLiteBackend(path=":memory:")
    snapshot = build_pipeline(backend, "corrupt-graph")
    with backend.connect() as connection:
        row = connection.session.get(
            GraphRow,
            (snapshot.name, int(snapshot.graph.fingerprint)),
        )
        assert row is not None
        metadata = deepcopy(row.metadata_json)
        fields = cast(dict[str, Any], metadata["value"]["fields"])
        fields["name"] = "wrong"
        row.metadata_json = metadata

    with backend.connect() as connection:
        with pytest.raises(MetadataCodecError, match="name mismatch"):
            connection.read_graph(snapshot.name, snapshot.graph.fingerprint)
    backend.close()


def test_sql_backend_detects_corrupted_partition_storage() -> None:
    backend = SQLiteBackend(path=":memory:")
    snapshot = build_pipeline(backend, "corrupt-partition")
    output = snapshot.artifacts.incremented
    with backend.connect() as connection:
        partition = connection.read_artifact_partitions(output)[0]
        row = connection.session.get(StoragePartitionRow, int(partition.fingerprint))
        assert row is not None
        row.storage_fingerprint = int(
            Number(storage=StringLiteral(id="different")).storage.fingerprint
        )

    mismatched = Number(storage=StringLiteral(id="different"))
    with backend.connect() as connection:
        with pytest.raises(MetadataCodecError, match="does not match its storage fingerprint"):
            connection.read_artifact_partitions(mismatched)
    backend.close()


def test_sql_backend_detects_corrupted_snapshot_partition_link() -> None:
    backend = SQLiteBackend(path=":memory:")
    snapshot = build_pipeline(backend, "corrupt-snapshot-partition")
    output = snapshot.artifacts.incremented
    other = Number(storage=StringLiteral(id="other", value="10"))
    wrong = other.storage.discover_partitions()
    assert wrong

    with backend.connect() as connection:
        connection.write_artifact_partitions(other, wrong)
        connection.write_snapshot_partitions(
            snapshot,
            "incremented",
            other,
            wrong,
        )

    with backend.connect() as connection:
        with pytest.raises(MetadataCodecError, match="does not match its Artifact"):
            connection.read_snapshot_partitions(
                snapshot,
                "incremented",
                output,
            )
    backend.close()


def test_sql_backend_manual_schema_check_succeeds(tmp_path: Path) -> None:
    path = tmp_path / "manual-schema.sqlite"
    migrated = SQLiteBackend(path=path)
    migrated.upgrade()
    migrated.close()

    backend = SQLiteBackend(path=path, auto_upgrade=False)
    with backend.connect():
        assert backend.schema_revision == backend.expected_schema_revision
    backend.close()


def test_sql_backend_concurrent_schema_initialization(tmp_path: Path) -> None:
    backend = SQLiteBackend(path=tmp_path / "concurrent-schema.sqlite")

    def connect() -> None:
        with backend.connect():
            pass

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(connect) for _ in range(2)]
        for future in futures:
            future.result()

    backend.close()


def test_postgres_pipeline_reopens_with_jsonb(
    postgres_backend: PostgresBackend,
) -> None:
    snapshot = build_pipeline(postgres_backend, "postgres")
    with postgres_backend.engine.connect() as connection:
        metadata, metadata_type = connection.execute(
            sa.text("SELECT metadata_json, pg_typeof(metadata_json) FROM arti_graph")
        ).one()
        assert isinstance(metadata, dict)
        assert metadata_type == "jsonb"

    reopened = PostgresBackend(url=postgres_backend.url)
    restored = GraphSnapshot.from_tag(snapshot.name, "latest", connectable=reopened)

    assert restored.read(restored.artifacts.incremented, annotation=int) == 2
    assert restored.backend is reopened
    reopened.close()
