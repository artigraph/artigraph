from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from arti import (
    Backend,
    BackendConnection,
    Fingerprint,
    Graph,
    GraphSnapshot,
    InputFingerprints,
    PartitionKey,
)
from arti.backends.memory import MemoryBackend
from arti.backends.sqlite import SQLiteBackend
from arti.storage import StoragePartitionSnapshot
from arti.storage.literal import StringLiteral
from tests.arti.backends.pipeline import Number


def _initialize(
    backend: Backend[Any],
) -> tuple[
    dict[int, GraphSnapshot],
    Number,
    tuple[StoragePartitionSnapshot, StoragePartitionSnapshot],
]:
    with Graph(name="property-backend", backend=backend) as graph:
        pass
    snapshots = {
        index: GraphSnapshot(id=Fingerprint.from_int(index + 1), graph=graph) for index in range(2)
    }
    artifact = Number(storage=StringLiteral(id="{input_fingerprint}"))
    partitions = tuple(
        StoragePartitionSnapshot(
            content_fingerprint=Fingerprint.from_int(100 + index),
            storage_partition=artifact.storage.generate_partition(
                input_fingerprint=Fingerprint.from_int(index + 10)
            ),
        )
        for index in range(2)
    )
    assert len(partitions) == 2
    with backend.connect() as connection:
        connection.write_graph(graph)
        for snapshot in snapshots.values():
            connection.write_snapshot(snapshot)
        connection.write_artifact_partitions(artifact, partitions)
    return snapshots, artifact, partitions


def _tag(
    connection: BackendConnection,
    snapshot: GraphSnapshot,
    *,
    overwrite: bool,
) -> str:
    try:
        connection.write_snapshot_tag(snapshot, "property", overwrite)
    except ValueError:
        return "conflict"
    return "written"


def _observe(
    connection: BackendConnection,
    snapshots: Mapping[int, GraphSnapshot],
    artifact: Number,
    partitions: tuple[StoragePartitionSnapshot, StoragePartitionSnapshot],
) -> tuple[Any, ...]:
    snapshot_partitions = tuple(
        frozenset(
            partition.fingerprint
            for partition in connection.read_snapshot_partitions(
                snapshot,
                "artifact",
                artifact,
            )
        )
        for snapshot in snapshots.values()
    )
    filtered = tuple(
        tuple(
            partition.fingerprint
            for partition in connection.read_artifact_partitions(
                artifact,
                InputFingerprints(
                    {
                        PartitionKey(): partition.input_fingerprint,
                    }
                ),
            )
        )
        for partition in partitions
    )
    try:
        tag = connection.read_snapshot_tag("property-backend", "property").fingerprint
    except ValueError:
        tag = None
    return snapshot_partitions, filtered, tag


@settings(max_examples=40, deadline=None)
@given(
    operations=st.lists(
        st.tuples(
            st.sampled_from(("link", "tag")),
            st.integers(min_value=0, max_value=1),
            st.booleans(),
        ),
        max_size=30,
    )
)
def test_sqlite_matches_memory_backend(
    operations: list[tuple[str, int, bool]],
) -> None:
    memory = MemoryBackend()
    sqlite = SQLiteBackend(path=":memory:")
    memory_state = _initialize(memory)
    sqlite_state = _initialize(sqlite)

    for action, snapshot_index, flag in operations:
        outcomes = list[str]()
        for backend, (snapshots, artifact, partitions) in (
            (memory, memory_state),
            (sqlite, sqlite_state),
        ):
            with backend.connect() as connection:
                if action == "link":
                    connection.write_snapshot_partitions(
                        snapshots[snapshot_index],
                        "artifact",
                        artifact,
                        (partitions[int(flag)],),
                    )
                    outcomes.append("linked")
                else:
                    outcomes.append(
                        _tag(
                            connection,
                            snapshots[snapshot_index],
                            overwrite=flag,
                        )
                    )
        assert outcomes[0] == outcomes[1]

        with memory.connect() as memory_connection, sqlite.connect() as sqlite_connection:
            assert _observe(memory_connection, *memory_state) == _observe(
                sqlite_connection,
                *sqlite_state,
            )

    sqlite.close()
