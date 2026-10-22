from __future__ import annotations

from abc import abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from pydantic import PrivateAttr
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from arti.artifacts import Artifact
from arti.backends import Backend, BackendConnection
from arti.backends.sql.codec import MetadataCodecError, decode_document, encode_document
from arti.backends.sql.models import (
    Base,
    GraphRow,
    GraphSnapshotRow,
    SnapshotPartitionRow,
    SnapshotTagRow,
    StoragePartitionRow,
)
from arti.fingerprints import Fingerprint
from arti.internal.utils import NoCopyMixin
from arti.partitions import InputFingerprints
from arti.storage import StoragePartitionSnapshot, StoragePartitionSnapshots

if TYPE_CHECKING:
    from arti.graphs import Graph, GraphSnapshot

_MIGRATION_LOCK = RLock()
_POSTGRES_MIGRATION_LOCK_ID = 1_158_547_311_702_653_556


class BackendSchemaError(RuntimeError):
    """Raised when a SQL backend schema is not at the expected revision."""


def _alembic_config(connection: sa.Connection | None = None) -> Config:
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).with_name("migrations")))
    if connection is not None:
        config.attributes["connection"] = connection
    return config


def _validate_model_key(
    value: Any,
    *,
    expected_name: str | None = None,
    expected_fingerprint: Fingerprint,
) -> None:
    if expected_name is not None and value.name != expected_name:
        raise MetadataCodecError(
            f"Persisted metadata name mismatch: expected {expected_name!r}, got {value.name!r}"
        )
    if value.fingerprint != expected_fingerprint:
        raise MetadataCodecError(
            f"Persisted metadata fingerprint mismatch: expected {expected_fingerprint}, got {value.fingerprint}"
        )


class SQLBackendConnection(BackendConnection):
    def __init__(self, backend: SQLBackend, session: Session) -> None:
        self.backend = backend
        self.session = session

    def _restore_graph(self, row: GraphRow) -> Graph:
        from arti.graphs import Graph

        graph = decode_document(row.metadata_json, Graph)
        graph = graph.model_copy(update={"backend": self.backend})
        _validate_model_key(
            graph,
            expected_name=row.name,
            expected_fingerprint=Fingerprint.from_int(row.fingerprint),
        )
        return graph

    def _restore_partition(self, row: StoragePartitionRow) -> StoragePartitionSnapshot:
        partition = decode_document(row.metadata_json, StoragePartitionSnapshot)
        _validate_model_key(
            partition,
            expected_fingerprint=Fingerprint.from_int(row.fingerprint),
        )
        return partition

    def _insert(self, row_type: type[Base]) -> postgresql.Insert | sqlite.Insert:
        dialect = self.session.connection().dialect.name
        if dialect == "sqlite":
            return sqlite.insert(row_type)
        if dialect == "postgresql":
            return postgresql.insert(row_type)
        raise NotImplementedError(f"Unsupported SQL backend dialect: {dialect}")  # coverage: ignore

    def _insert_missing(self, row_type: type[Base], rows: list[dict[str, Any]]) -> None:
        # Rows other than tags are keyed by content fingerprints, so an existing row already holds
        # the same metadata. Ignoring conflicts (rather than select-then-insert) also keeps
        # concurrent writers of the same Graph from racing on the primary key.
        if rows:
            self.session.execute(self._insert(row_type).on_conflict_do_nothing(), rows)

    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints | None = None
    ) -> StoragePartitionSnapshots:
        rows = self.session.scalars(
            sa.select(StoragePartitionRow)
            .where(StoragePartitionRow.storage_fingerprint == int(artifact.storage.fingerprint))
            .order_by(StoragePartitionRow.fingerprint)
        )
        partitions = tuple(self._restore_partition(row) for row in rows)
        for partition in partitions:
            if partition.storage != artifact.storage:
                raise MetadataCodecError(
                    "Persisted storage partition does not match its storage fingerprint"
                )
        if input_fingerprints is not None:
            partitions = tuple(
                partition
                for partition in partitions
                if input_fingerprints.get(partition.partition_key) == partition.input_fingerprint
            )
        return partitions

    def write_artifact_partitions(
        self, artifact: Artifact, partitions: StoragePartitionSnapshots
    ) -> None:
        rows = list[dict[str, Any]]()
        for partition in partitions:
            if partition.storage != artifact.storage:
                raise ValueError(
                    f"Partition storage {partition.storage} does not match Artifact storage {artifact.storage}"
                )
            rows.append(
                {
                    "fingerprint": int(partition.fingerprint),
                    "storage_fingerprint": int(artifact.storage.fingerprint),
                    "input_fingerprint": (
                        None
                        if partition.input_fingerprint is None
                        else int(partition.input_fingerprint)
                    ),
                    "metadata_json": encode_document(partition),
                }
            )
        self._insert_missing(StoragePartitionRow, rows)

    def read_graph(self, name: str, fingerprint: Fingerprint) -> Graph:
        row = self.session.get(GraphRow, (name, int(fingerprint)))
        if row is None:
            raise KeyError((name, fingerprint))
        return self._restore_graph(row)

    def write_graph(self, graph: Graph) -> None:
        self._insert_missing(
            GraphRow,
            [
                {
                    "name": graph.name,
                    "fingerprint": int(graph.fingerprint),
                    "metadata_json": encode_document(graph),
                }
            ],
        )

    def read_snapshot(self, name: str, fingerprint: Fingerprint) -> GraphSnapshot:
        from arti.graphs import GraphSnapshot

        row = self.session.get(GraphSnapshotRow, (name, int(fingerprint)))
        if row is None:
            raise KeyError((name, fingerprint))
        graph = self.read_graph(row.name, Fingerprint.from_int(row.graph_fingerprint))
        snapshot = GraphSnapshot(
            id=Fingerprint.from_int(row.snapshot_id),
            graph=graph,
        )
        _validate_model_key(
            snapshot,
            expected_name=row.name,
            expected_fingerprint=Fingerprint.from_int(row.fingerprint),
        )
        return snapshot

    def write_snapshot(self, snapshot: GraphSnapshot) -> None:
        self._insert_missing(
            GraphSnapshotRow,
            [
                {
                    "name": snapshot.name,
                    "fingerprint": int(snapshot.fingerprint),
                    "snapshot_id": int(snapshot.id),
                    "graph_fingerprint": int(snapshot.graph.fingerprint),
                }
            ],
        )

    def read_snapshot_tag(self, name: str, tag: str) -> GraphSnapshot:
        row = self.session.get(SnapshotTagRow, (name, tag))
        if row is None:
            raise ValueError(f"No known `{tag}` tag for GraphSnapshot `{name}`")
        return self.read_snapshot(
            row.graph_name,
            Fingerprint.from_int(row.snapshot_fingerprint),
        )

    def write_snapshot_tag(
        self, snapshot: GraphSnapshot, tag: str, overwrite: bool = False
    ) -> None:
        values = {
            "graph_name": snapshot.name,
            "tag": tag,
            "snapshot_fingerprint": int(snapshot.fingerprint),
        }
        statement = self._insert(SnapshotTagRow).values(**values)
        if overwrite:
            statement = statement.on_conflict_do_update(
                index_elements=["graph_name", "tag"],
                set_={"snapshot_fingerprint": int(snapshot.fingerprint)},
            )
        else:
            statement = statement.on_conflict_do_nothing(index_elements=["graph_name", "tag"])
        result = self.session.execute(statement.returning(SnapshotTagRow.graph_name))
        if not overwrite and result.scalar_one_or_none() is None:
            existing = self.read_snapshot_tag(snapshot.name, tag)
            raise ValueError(
                f"Existing `{tag}` tag for Graph `{snapshot.name}` points to {existing}"
            )

    def read_snapshot_partitions(
        self, snapshot: GraphSnapshot, artifact_key: str, artifact: Artifact
    ) -> StoragePartitionSnapshots:
        rows = self.session.scalars(
            sa.select(StoragePartitionRow)
            .join(
                SnapshotPartitionRow,
                SnapshotPartitionRow.partition_fingerprint == StoragePartitionRow.fingerprint,
            )
            .where(
                SnapshotPartitionRow.snapshot_name == snapshot.name,
                SnapshotPartitionRow.snapshot_fingerprint == int(snapshot.fingerprint),
                SnapshotPartitionRow.artifact_key == artifact_key,
            )
            .order_by(StoragePartitionRow.fingerprint)
        )
        partitions = tuple(self._restore_partition(row) for row in rows)
        for partition in partitions:
            if partition.storage != artifact.storage:
                raise MetadataCodecError(
                    f"Persisted partition for {artifact_key!r} does not match its Artifact"
                )
        return partitions

    def write_snapshot_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitionSnapshots,
    ) -> None:
        rows = list[dict[str, Any]]()
        for partition in partitions:
            if partition.storage != artifact.storage:
                raise ValueError(
                    f"Partition storage {partition.storage} does not match Artifact storage {artifact.storage}"
                )
            rows.append(
                {
                    "snapshot_name": snapshot.name,
                    "snapshot_fingerprint": int(snapshot.fingerprint),
                    "artifact_key": artifact_key,
                    "partition_fingerprint": int(partition.fingerprint),
                }
            )
        self._insert_missing(SnapshotPartitionRow, rows)


class _SharedEngine(NoCopyMixin):
    """
    The engine and its state, shared by every copy of a SQLBackend.

    Like the MemoryBackend's container, this bypasses (deep)copying so that copies of a Backend (eg:
    in a deep copied Graph) keep using the same database. Engines can't be deep copied anyway.
    """

    def __init__(self) -> None:
        self.engine: Engine | None = None
        self.migration_checked = False
        self.closed = False


class SQLBackend(Backend[SQLBackendConnection]):
    """Shared transactional SQLAlchemy implementation for Artigraph metadata."""

    _abstract_: ClassVar[bool] = True

    auto_upgrade: bool = True

    _shared: _SharedEngine = PrivateAttr(default_factory=_SharedEngine)

    @abstractmethod
    def _create_engine(self) -> Engine:
        raise NotImplementedError()

    @property
    def engine(self) -> Engine:
        if self._shared.closed:
            raise RuntimeError(f"{type(self).__name__} is closed")
        if self._shared.engine is None:
            self._shared.engine = self._create_engine()
        return self._shared.engine

    @property
    def schema_revision(self) -> str | None:
        with self.engine.connect() as connection:
            return MigrationContext.configure(connection).get_current_revision()

    @property
    def expected_schema_revision(self) -> str:
        heads = ScriptDirectory.from_config(_alembic_config()).get_heads()
        if len(heads) != 1:  # coverage: ignore
            raise BackendSchemaError(f"Expected one migration head, got: {heads}")
        return heads[0]

    def upgrade(self) -> None:
        """Upgrade the backend schema to the bundled migration head."""
        # Alembic installs process-global context proxies while running an environment, so it
        # cannot safely execute concurrently in multiple threads. Postgres also gets a
        # transaction-scoped advisory lock to coordinate separate application processes.
        with _MIGRATION_LOCK:
            with self.engine.begin() as connection:
                if connection.dialect.name == "postgresql":
                    connection.execute(
                        sa.text("SELECT pg_advisory_xact_lock(:lock_id)"),
                        {"lock_id": _POSTGRES_MIGRATION_LOCK_ID},
                    )
                command.upgrade(_alembic_config(connection), "head")
            self._shared.migration_checked = True

    def _ensure_schema(self) -> None:
        if self._shared.migration_checked:
            return
        with _MIGRATION_LOCK:
            if self._shared.migration_checked:
                return
            if self.auto_upgrade:
                self.upgrade()
            else:
                current = self.schema_revision
                expected = self.expected_schema_revision
                if current != expected:
                    raise BackendSchemaError(
                        f"Backend schema is at revision {current!r}, expected {expected!r}. Call backend.upgrade() before connecting."
                    )
            self._shared.migration_checked = True

    @contextmanager
    def connect(self) -> Generator[SQLBackendConnection]:
        self._ensure_schema()
        with Session(self.engine) as session, session.begin():
            yield SQLBackendConnection(self, session)

    def close(self) -> None:
        """Dispose pooled connections. The backend can't be used afterwards."""
        if self._shared.engine is not None:
            self._shared.engine.dispose()
            self._shared.engine = None
        self._shared.closed = True


__all__ = [
    "BackendSchemaError",
    "SQLBackend",
    "SQLBackendConnection",
]
