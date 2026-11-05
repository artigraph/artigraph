from __future__ import annotations

import logging
from typing import cast

import pytest

from arti import Artifact, GraphSnapshot
from arti.backends.memory import MemoryBackend
from arti.executors import producer_dependencies, producers_in_order
from arti.executors.local import LocalExecutor
from arti.internal.type_hints import assert_not_none
from arti.storage.literal import StringLiteralPartition
from tests.arti.backends.pipeline import build_pipeline


def _outputs(snapshot: GraphSnapshot, *keys: str) -> tuple[Artifact, ...]:
    return tuple(cast(Artifact, snapshot.artifacts[key]) for key in keys)


def test_producer_order_and_dependencies() -> None:
    snapshot = build_pipeline(MemoryBackend(), "order")
    incremented, doubled, previous = _outputs(snapshot, "incremented", "doubled", "previous")
    increment, double, neighbors = (
        assert_not_none(artifact.producer_output).producer
        for artifact in (incremented, doubled, previous)
    )

    assert list(producers_in_order(snapshot.graph)) == [increment, double, neighbors]
    assert producer_dependencies(snapshot.graph) == {
        increment: frozenset(),
        double: frozenset({increment}),
        neighbors: frozenset({double}),
    }


def test_plan_links_existing_partitions() -> None:
    backend = MemoryBackend()
    snapshot = build_pipeline(backend, "plan")
    (incremented,) = _outputs(snapshot, "incremented")
    assert incremented.producer_output is not None

    plan = LocalExecutor().plan_producer(snapshot, incremented.producer_output.producer)

    assert plan.pending == ()
    assert plan.existing == set(plan.dependencies)


def test_build_skips_a_partition_published_since_planning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Plan against an empty snapshot, then let a "concurrent writer" publish the partition.
    first = build_pipeline(MemoryBackend(), "race")
    backend = MemoryBackend()
    snapshot = first.graph.model_copy(update={"backend": backend}).snapshot()
    (incremented,) = _outputs(snapshot, "incremented")
    assert incremented.producer_output is not None
    executor = LocalExecutor()
    plan = executor.plan_producer(snapshot, incremented.producer_output.producer)
    assert len(plan.pending) == 1
    LocalExecutor().build(snapshot)

    with caplog.at_level(logging.INFO, logger="arti.executors"):
        executor.build_planned_partition(snapshot, plan, plan.pending[0])

    assert "Skipping existing increment output" in caplog.messages
    with backend.connect() as connection:
        assert len(connection.read_artifact_partitions(incremented)) == 1


def test_plan_rejects_conflicting_partitions() -> None:
    backend = MemoryBackend()
    snapshot = build_pipeline(backend, "conflict")
    (incremented,) = _outputs(snapshot, "incremented")
    with backend.connect() as connection:
        (existing,) = connection.read_snapshot_partitions(snapshot, "incremented", incremented)
        partition = cast(StringLiteralPartition, existing.storage_partition)
        conflicting = partition.model_copy(update={"value": "99"}).snapshot()
        connection.write_artifact_partitions(incremented, (conflicting,))

    with pytest.raises(ValueError, match="several partitions with different contents"):
        snapshot.build()


def test_plan_rejects_partially_registered_outputs() -> None:
    complete = build_pipeline(MemoryBackend(), "partial")
    (previous,) = _outputs(complete, "previous")
    with complete.backend.connect() as connection:
        previous_partitions = connection.read_snapshot_partitions(complete, "previous", previous)

    # Register only one of the two `neighbors` outputs in a fresh backend.
    backend = MemoryBackend()
    graph = complete.graph.model_copy(update={"backend": backend})
    with backend.connect() as connection:
        connection.write_artifact_partitions(previous, previous_partitions)

    with pytest.raises(ValueError, match="`next` is missing partitions"):
        graph.build()
