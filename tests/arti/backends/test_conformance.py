from __future__ import annotations

from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue
from typing import Any, Literal, cast

import pytest

from arti import (
    Artifact,
    Backend,
    Fingerprint,
    Graph,
    GraphSnapshot,
    InputFingerprints,
    PartitionKey,
)
from arti.backends.memory import MemoryBackend
from arti.backends.postgres import PostgresBackend
from arti.backends.sql import SQLBackend
from arti.backends.sql.codec import MetadataCodecError
from arti.backends.sql.models import GraphRow
from arti.backends.sqlite import SQLiteBackend
from arti.formats.json import JSON
from arti.partitions import Int32Field
from arti.storage.literal import StringLiteral
from arti.storage.local import LocalFile
from arti.types import Collection, Int32, Int64, Struct
from arti.versions import SemVer
from tests.arti.backends.pipeline import Number, build_pipeline, increment

type BackendKind = Literal["memory", "sqlite-memory", "sqlite-file", "postgres"]


@dataclass
class BackendHarness:
    kind: BackendKind
    factory: Callable[[], Backend[Any]]
    opened: list[Backend[Any]] = field(default_factory=lambda: list[Backend[Any]]())

    def open(self) -> Backend[Any]:
        backend = self.factory()
        self.opened.append(backend)
        return backend

    def close(self) -> None:
        for backend in self.opened:
            backend.close()


def _harness(
    kind: BackendKind,
    *,
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> BackendHarness:
    match kind:
        case "memory":
            memory = MemoryBackend()
            return BackendHarness(kind, lambda: memory)
        case "sqlite-memory":
            return BackendHarness(kind, lambda: SQLiteBackend(path=":memory:"))
        case "sqlite-file":
            path = tmp_path / "conformance.sqlite"
            return BackendHarness(kind, lambda: SQLiteBackend(path=path))
        case "postgres":
            seed = cast(PostgresBackend, request.getfixturevalue("postgres_backend"))
            return BackendHarness(kind, lambda: PostgresBackend(url=seed.url))


@pytest.fixture(params=["memory", "sqlite-memory", "sqlite-file", "postgres"])
def backend_harness(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[BackendHarness]:
    harness = _harness(cast(BackendKind, request.param), request=request, tmp_path=tmp_path)
    try:
        yield harness
    finally:
        harness.close()


@pytest.fixture(params=["sqlite-file", "postgres"])
def persistent_backend_harness(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Generator[BackendHarness]:
    harness = _harness(cast(BackendKind, request.param), request=request, tmp_path=tmp_path)
    try:
        yield harness
    finally:
        harness.close()


def test_backend_pipeline_conformance(backend_harness: BackendHarness) -> None:
    backend = backend_harness.open()
    snapshot = build_pipeline(backend, f"pipeline-{backend_harness.kind}")

    assert snapshot.read(snapshot.artifacts.value, annotation=int) == 1
    assert snapshot.read(snapshot.artifacts.incremented, annotation=int) == 2
    assert snapshot.read(snapshot.artifacts.doubled, annotation=int) == 4
    assert snapshot.read(snapshot.artifacts.previous, annotation=int) == 3
    assert snapshot.read(snapshot.artifacts.next, annotation=int) == 5

    with backend.connect() as connection:
        restored_graph = connection.read_graph(
            snapshot.name,
            snapshot.graph.fingerprint,
        )
        restored_snapshot = connection.read_snapshot(
            snapshot.name,
            snapshot.fingerprint,
        )
        output = snapshot.artifacts.incremented
        artifact_partitions = connection.read_artifact_partitions(output)
        snapshot_partitions = connection.read_snapshot_partitions(
            snapshot,
            "incremented",
            output,
        )
        connection.write_graph(snapshot.graph)
        connection.write_snapshot(snapshot)
        connection.write_artifact_partitions(output, artifact_partitions)
        connection.write_snapshot_partitions(
            snapshot,
            "incremented",
            output,
            snapshot_partitions,
        )

    assert restored_graph.model_dump() == snapshot.graph.model_dump()
    assert restored_snapshot.fingerprint == snapshot.fingerprint
    assert artifact_partitions == snapshot_partitions
    assert len(artifact_partitions) == 1

    expected_inputs = InputFingerprints(
        {
            artifact_partitions[0].partition_key: artifact_partitions[0].input_fingerprint,
        }
    )
    with backend.connect() as connection:
        assert connection.read_artifact_partitions(output, expected_inputs) == artifact_partitions
        # Unlike no filter, an empty filter matches no partitions.
        assert connection.read_artifact_partitions(output, InputFingerprints()) == ()

        with pytest.raises(KeyError):
            connection.read_graph("missing", Fingerprint.from_int(1))
        with pytest.raises(KeyError):
            connection.read_snapshot("missing", Fingerprint.from_int(1))
        with pytest.raises(ValueError, match="No known `missing` tag"):
            connection.read_snapshot_tag(snapshot.name, "missing")
        with pytest.raises(ValueError, match="Existing `latest` tag"):
            connection.write_snapshot_tag(snapshot, "latest")

    retagged = GraphSnapshot(id=snapshot.id.combine(Fingerprint.from_int(1)), graph=snapshot.graph)
    with backend.connect() as connection:
        connection.write_snapshot(retagged)
        connection.write_snapshot_tag(retagged, "latest", overwrite=True)
        assert (
            connection.read_snapshot_tag(snapshot.name, "latest").fingerprint
            == retagged.fingerprint
        )


def test_backend_keeps_graphs_that_differ_only_by_producer(
    backend_harness: BackendHarness,
) -> None:
    backend = backend_harness.open()

    def build(major: int) -> GraphSnapshot:
        with Graph(name=f"producer-change-{backend_harness.kind}", backend=backend) as graph:
            graph.artifacts.value = Number(storage=StringLiteral(value="1"))
            version = SemVer(major=major, minor=0, patch=0)
            graph.artifacts.incremented = (
                increment(value=graph.artifacts.value).model_copy(update={"version": version}).out()
            )
        return graph.build()

    v1, v2 = build(1), build(2)
    # Artifact identity is Producer independent, but Graph identity is not.
    assert v1.artifacts.incremented.fingerprint == v2.artifacts.incremented.fingerprint
    assert v1.graph.fingerprint != v2.graph.fingerprint
    for snapshot in (v1, v2):
        with backend.connect() as connection:
            restored = connection.read_snapshot(snapshot.name, snapshot.fingerprint)
        assert restored.graph == snapshot.graph
        assert restored.read(restored.artifacts.incremented, annotation=int) == 2


def test_backend_separates_storage_by_partition_key_types(
    backend_harness: BackendHarness, tmp_path: Path
) -> None:
    backend = backend_harness.open()
    int32, int64 = (
        Artifact(
            type=Collection(element=Struct(fields={"i": type_}), partition_by=("i",)),
            format=JSON(),
            storage=LocalFile(path=str(tmp_path / "{i.value}.json")),
        )
        for type_ in (Int32(), Int64())
    )
    (tmp_path / "1.json").write_text("[]")
    partition = int32.storage.generate_partition(
        partition_key=PartitionKey(i=Int32Field(value=1))
    ).snapshot()
    with backend.connect() as connection:
        connection.write_artifact_partitions(int32, (partition,))

    with backend.connect() as connection:
        assert connection.read_artifact_partitions(int32) == (partition,)
        # The MemoryBackend discovers the file again, but only as an Int64 partition.
        for partitions in (
            connection.read_artifact_partitions(int64),
            connection.read_artifact_partitions(int64, InputFingerprints()),
        ):
            assert all(partition.storage == int64.storage for partition in partitions)


def test_persistent_backend_reopens(
    persistent_backend_harness: BackendHarness,
) -> None:
    first = persistent_backend_harness.open()
    snapshot = build_pipeline(first, f"reopen-{persistent_backend_harness.kind}")
    first.close()

    reopened = persistent_backend_harness.open()
    restored = GraphSnapshot.from_tag(snapshot.name, "latest", connectable=reopened)

    assert restored.fingerprint == snapshot.fingerprint
    assert restored.backend is reopened
    assert restored.read(restored.artifacts.next, annotation=int) == 5


def test_sql_backend_transaction_rollback(
    persistent_backend_harness: BackendHarness,
) -> None:
    backend = cast(SQLBackend, persistent_backend_harness.open())
    with Graph(name=f"rollback-{persistent_backend_harness.kind}", backend=backend) as graph:
        graph.artifacts.value = Number(storage=StringLiteral(value="1"))

    def write_then_fail() -> None:
        with backend.connect() as connection:
            connection.write_graph(graph)
            raise RuntimeError("rollback")

    with pytest.raises(RuntimeError, match="rollback"):
        write_then_fail()
    with backend.connect() as connection:
        with pytest.raises(KeyError):
            connection.read_graph(graph.name, graph.fingerprint)


def test_sql_backend_tag_race(
    persistent_backend_harness: BackendHarness,
) -> None:
    backend = cast(SQLBackend, persistent_backend_harness.open())
    snapshot = build_pipeline(backend, f"race-{persistent_backend_harness.kind}")
    second = GraphSnapshot(
        id=snapshot.id.combine(Fingerprint.from_int(1)),
        graph=snapshot.graph,
    )
    with backend.connect() as connection:
        connection.write_snapshot(second)

    results = Queue[GraphSnapshot | Exception]()

    def tag(candidate: GraphSnapshot) -> None:
        try:
            candidate.tag("race")
            results.put(candidate)
        except ValueError as error:
            results.put(error)

    # Rebind each candidate to a distinct backend/connection while preserving its metadata.
    candidates = [
        candidate.model_copy(
            update={
                "graph": candidate.graph.model_copy(
                    update={"backend": persistent_backend_harness.open()}
                )
            }
        )
        for candidate in (snapshot, second)
    ]
    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(tag, candidates))

    outcomes = [results.get(), results.get()]
    assert sum(isinstance(outcome, GraphSnapshot) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, ValueError) for outcome in outcomes) == 1
    with backend.connect() as connection:
        assert connection.read_snapshot_tag(snapshot.name, "race").fingerprint in {
            snapshot.fingerprint,
            second.fingerprint,
        }


def test_sql_backend_detects_corrupted_graph_fingerprint() -> None:
    backend = SQLiteBackend(path=":memory:")
    snapshot = build_pipeline(backend, "corrupt-graph-fingerprint")
    with backend.connect() as connection:
        row = connection.session.get(
            GraphRow,
            (snapshot.name, int(snapshot.graph.fingerprint)),
        )
        assert row is not None
        metadata = deepcopy(row.metadata_json)
        fields = cast(dict[str, Any], metadata["value"]["fields"])
        encoded_tags = cast(dict[str, Any], fields["path_tags"])
        items = cast(list[Any], encoded_tags["items"])
        items.append(["corrupt", "value"])
        row.metadata_json = metadata

    with backend.connect() as connection:
        with pytest.raises(MetadataCodecError, match="fingerprint mismatch"):
            connection.read_graph(snapshot.name, snapshot.graph.fingerprint)
    backend.close()


def test_sql_backend_context_manager() -> None:
    with SQLiteBackend(path=":memory:") as backend:
        with backend.connect():
            assert backend.schema_revision == backend.expected_schema_revision

    backend.close()
