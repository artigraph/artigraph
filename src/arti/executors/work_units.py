"""
Build Producer partitions in other processes or machines from small, serializable references.

A `WorkUnit` names one Producer partition of a GraphSnapshot without embedding (or pickling) any
Graph, Producer, or data. Workers run the same code as the coordinator: they import the Graph from
its `graph_ref`, check that the snapshot exists for that Graph (catching code or image drift), and
read the inputs from the Backend and Storage.

The module level functions here are the whole protocol between a coordinator and its workers:

1. `snapshot_graph(graph_ref)` snapshots the Graph and returns the snapshot ID.
2. `plan_work_units(graph_ref, snapshot_id, producer_key)` plans a Producer, once its inputs are
   built, and returns a `WorkUnit` for each partition that still needs to be built.
3. `run_work_unit(unit)` builds one partition.
"""

from __future__ import annotations

import importlib
import json

from arti.backends import Reach
from arti.executors import Executor, ProducerPlan, logger
from arti.fingerprints import Fingerprint
from arti.graphs import Graph, GraphSnapshot
from arti.internal.mappings import FrozenMapping, frozendict
from arti.internal.models import Model
from arti.internal.type_hints import assert_not_none
from arti.partitions import PartitionKey
from arti.producers import Producer

_REACH_ORDER: tuple[Reach, ...] = ("process", "host", "network")


def check_reach(graph: Graph, required: Reach) -> None:
    """Raise an error if the Graph's Backend or any Artifact's Storage can't be shared that widely."""
    needed = _REACH_ORDER.index(required)
    problems = list[str]()
    if _REACH_ORDER.index(graph.backend.reach) < needed:
        problems.append(f"the {type(graph.backend).__name__} (reach: {graph.backend.reach})")
    for key, artifact in sorted(graph.artifacts.walk()):
        storage = artifact.storage
        if _REACH_ORDER.index(storage.reach) < needed:
            problems.append(f"`{key}`'s {type(storage).__name__} (reach: {storage.reach})")
    if problems:
        raise ValueError(
            f"Graph `{graph.name}` must be shared across a {required}, but these can't be: {', '.join(problems)}."
        )


def resolve_graph(graph_ref: str) -> Graph:
    """Import a Graph from a `"package.module:attribute"` reference."""
    module_name, sep, attribute = graph_ref.partition(":")
    if not sep or not module_name or not attribute:
        raise ValueError(
            f"Expected a graph reference like 'package.module:graph', got: {graph_ref!r}"
        )
    value: object = importlib.import_module(module_name)
    for name in attribute.split("."):
        value = getattr(value, name)
    if not isinstance(value, Graph):
        raise TypeError(f"{graph_ref!r} must reference a Graph, got: {type(value).__name__}")
    return value


def check_graph_ref(graph_ref: str, graph: Graph) -> None:
    """Raise an error unless the reference imports a Graph with the same fingerprint."""
    imported = resolve_graph(graph_ref)
    if imported.fingerprint != graph.fingerprint:
        raise ValueError(
            f"{graph_ref!r} imports a different Graph than the one being built (`{imported.name}` {imported.fingerprint} != `{graph.name}` {graph.fingerprint})."
        )


def resolve_snapshot(graph_ref: str, snapshot_id: Fingerprint) -> GraphSnapshot:
    """
    Import the Graph and return its snapshot with this ID.

    The snapshot must already be recorded in the Graph's Backend. A missing snapshot usually means
    this process runs different code (or a different image) than the one that took the snapshot.
    """
    graph = resolve_graph(graph_ref)
    snapshot = GraphSnapshot(id=snapshot_id, graph=graph)
    with graph.backend.connect() as connection:
        try:
            connection.read_snapshot(graph.name, snapshot.fingerprint)
        except KeyError:
            raise ValueError(
                f"Snapshot {snapshot_id} of `{graph.name}` (from {graph_ref!r}) is not in the Backend. Check that this process runs the same code as the one that took the snapshot, and that they share the Backend."
            ) from None
    return snapshot


def producer_key(graph: Graph, producer: Producer) -> str:
    """Identify a Producer in a Graph by the key of its first output Artifact."""
    return graph.artifact_to_key[graph.producer_outputs[producer][0]]


def resolve_producer(graph: Graph, key: str) -> Producer:
    """Return the Producer identified by `producer_key`."""
    for producer in graph.producers:
        if producer_key(graph, producer) == key:
            return producer
    raise ValueError(f"`{key}` is not the first output of a Producer in `{graph.name}`")


type EncodedPartitionKey = FrozenMapping[str, FrozenMapping[str, str]]


def encode_partition_key(partition_key: PartitionKey) -> frozendict[str, frozendict[str, str]]:
    """Encode a PartitionKey as plain strings, eg: `{"date": {"value": "2024-01-01"}}`."""
    return frozendict(
        {
            name: frozendict(
                {
                    component: str(getattr(field, component))
                    for component in type(field).model_fields
                }
            )
            for name, field in sorted(partition_key.items())
        }
    )


class WorkUnit(Model):
    """A reference to one Producer partition of a GraphSnapshot."""

    graph_ref: str
    snapshot_id: Fingerprint
    producer_key: str
    partition_key: EncodedPartitionKey
    input_fingerprint: Fingerprint

    @property
    def label(self) -> str:
        """A short, human readable name, eg: `doubled[date.value=2024-01-01]`."""
        components = ",".join(
            f"{name}.{component}={value}"
            for name, field in self.partition_key.items()
            for component, value in field.items()
        )
        return f"{self.producer_key}[{components}]" if components else self.producer_key

    def to_json(self) -> str:
        return json.dumps(
            {
                "graph_ref": self.graph_ref,
                "snapshot_id": int(self.snapshot_id),
                "producer_key": self.producer_key,
                "partition_key": {k: dict(v) for k, v in self.partition_key.items()},
                "input_fingerprint": int(self.input_fingerprint),
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, data: str) -> WorkUnit:
        return cls.model_validate_json(data)


def work_units(graph_ref: str, snapshot: GraphSnapshot, plan: ProducerPlan) -> tuple[WorkUnit, ...]:
    """Create a WorkUnit for each pending partition in the plan."""
    key = producer_key(snapshot.graph, plan.producer)
    return tuple(
        WorkUnit(
            graph_ref=graph_ref,
            snapshot_id=snapshot.id,
            producer_key=key,
            partition_key=encode_partition_key(partition_key),
            input_fingerprint=assert_not_none(plan.input_fingerprints[partition_key]),
        )
        for partition_key in plan.pending
    )


def snapshot_graph(graph_ref: str) -> Fingerprint:
    """Import the Graph, snapshot it (recording the snapshot in the Backend), and return its ID."""
    return resolve_graph(graph_ref).snapshot().id


def plan_work_units(
    graph_ref: str, snapshot_id: Fingerprint, producer_key: str
) -> tuple[WorkUnit, ...]:
    """Plan a Producer whose inputs are built and return a WorkUnit for each pending partition."""
    snapshot = resolve_snapshot(graph_ref, snapshot_id)
    producer = resolve_producer(snapshot.graph, producer_key)
    plan = _Worker().plan_producer(snapshot, producer)
    return work_units(graph_ref, snapshot, plan)


def run_work_unit(unit: WorkUnit) -> None:
    """
    Build the partition named by the WorkUnit, unless it already exists.

    The worker recomputes the Producer's partitions from the inputs linked to the snapshot, which
    yields this partition's input partitions, and checks that the input fingerprint still matches
    the planned one.
    """
    snapshot = resolve_snapshot(unit.graph_ref, unit.snapshot_id)
    producer = resolve_producer(snapshot.graph, unit.producer_key)
    worker = _Worker()
    with snapshot.backend.connect() as connection:
        input_partitions = worker.get_producer_inputs(snapshot, connection, producer)
    dependencies, input_fingerprints = producer.compute_dependencies(input_partitions)
    planned = {name: dict(components) for name, components in unit.partition_key.items()}
    matches = [key for key in dependencies if encode_partition_key(key) == planned]
    if not matches:
        raise ValueError(f"{unit.label} is not a partition of `{unit.producer_key}` anymore")
    (partition_key,) = matches
    if (input_fingerprint := input_fingerprints[partition_key]) != unit.input_fingerprint:
        raise ValueError(
            f"{unit.label} inputs changed since planning (input fingerprint {input_fingerprint} != {unit.input_fingerprint})"
        )
    logger.info(f"Running {unit.label} of snapshot {unit.snapshot_id}")
    worker.build_producer_partition(
        snapshot,
        producer,
        input_fingerprint=unit.input_fingerprint,
        input_partitions=dependencies[partition_key],
        partition_key=partition_key,
    )


class _Worker(Executor):
    """Reuse the shared Executor steps to plan and build individual partitions."""

    def build(self, snapshot: GraphSnapshot) -> None:
        raise NotImplementedError("Use the executor that created the WorkUnits")


__all__ = [
    "WorkUnit",
    "check_graph_ref",
    "check_reach",
    "encode_partition_key",
    "plan_work_units",
    "producer_key",
    "resolve_graph",
    "resolve_producer",
    "resolve_snapshot",
    "run_work_unit",
    "snapshot_graph",
    "work_units",
]
