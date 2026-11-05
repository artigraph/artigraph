from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
import logging
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from graphlib import TopologicalSorter

from arti.backends import BackendConnection
from arti.fingerprints import Fingerprint
from arti.graphs import Graph, GraphSnapshot
from arti.internal.mappings import frozendict
from arti.internal.models import Model
from arti.internal.type_hints import assert_not_none
from arti.partitions import InputFingerprints, PartitionKey
from arti.producers import InputPartitions, PartitionDependencies, Producer
from arti.storage import StoragePartitionSnapshot

logger = logging.getLogger(__name__)


def producer_dependencies(graph: Graph) -> frozendict[Producer, frozenset[Producer]]:
    """Map each Producer to the Producers that build its inputs."""
    return frozendict(
        {
            producer: frozenset(
                upstream.producer
                for artifact in producer.inputs.values()
                if (upstream := artifact.producer_output) is not None
            )
            for producer in graph.producers
        }
    )


def producers_in_order(graph: Graph) -> Iterator[Producer]:
    """Yield the Graph's Producers so that every Producer follows the Producers it depends on."""
    for node in TopologicalSorter(graph.dependencies).static_order():
        if isinstance(node, Producer):
            yield node


@dataclass(frozen=True)
class ProducerPlan:
    """
    The partitions a Producer must build for a GraphSnapshot.

    A plan can only be computed once all of the Producer's inputs are built, since `Producer.map`
    decides the output partitions from the input partitions.
    """

    producer: Producer
    dependencies: PartitionDependencies
    input_fingerprints: InputFingerprints
    # Partitions that were already built (eg: by a prior snapshot) and are now linked to this one.
    existing: frozenset[PartitionKey]

    @property
    def pending(self) -> tuple[PartitionKey, ...]:
        """The partitions that still need to be built, in `Producer.map` order."""
        return tuple(key for key in self.dependencies if key not in self.existing)


class Executor(Model):
    """
    Executor controls how the Producer partitions of a GraphSnapshot are built.

    Executors share the planning and building steps defined here, so every Executor records the
    same fingerprints, partitions, and snapshot links. They differ in *where* and *when* each
    partition is built.
    """

    @abc.abstractmethod
    def build(self, snapshot: GraphSnapshot) -> None:
        raise NotImplementedError()

    def get_producer_inputs(
        self, snapshot: GraphSnapshot, connection: BackendConnection, producer: Producer
    ) -> InputPartitions:
        return InputPartitions(
            {
                name: connection.read_snapshot_partitions(
                    snapshot, snapshot.graph.artifact_to_key[artifact], artifact
                )
                for name, artifact in producer.inputs.items()
            }
        )

    def discover_producer_partitions(
        self,
        snapshot: GraphSnapshot,
        connection: BackendConnection,
        producer: Producer,
        *,
        partition_input_fingerprints: InputFingerprints,
    ) -> set[PartitionKey]:
        """
        Link the already built output partitions to the snapshot and return their keys.

        The output partitions may be built, but not yet associated with this GraphSnapshot (eg: raw
        input data changed, but no changes trickled into this specific Producer). Hence, we fetch
        the known partitions for each output, filtered to the partition keys and input fingerprints
        computed for this snapshot, and link them to the snapshot.

        Raises an error, rather than rebuilding, if the known partitions are inconsistent: a
        partition known for only some outputs or a partition known with several contents.
        """
        graph = snapshot.graph
        outputs = graph.producer_outputs[producer]
        output_partitions = {
            output: connection.read_artifact_partitions(output, partition_input_fingerprints)
            for output in outputs
        }
        all_keys = {
            partition.partition_key
            for partitions in output_partitions.values()
            for partition in partitions
        }
        for output, partitions in output_partitions.items():
            output_key = graph.artifact_to_key[output]
            counts = Counter(partition.partition_key for partition in partitions)
            if duplicated := sorted(str(dict(key)) for key, n in counts.items() if n > 1):
                raise ValueError(
                    f"`{output_key}` has several partitions with different contents for the same partition key and input fingerprint: {duplicated}. This happens when concurrent builds of a non-deterministic Producer race."
                )
            if missing := sorted(str(dict(key)) for key in all_keys - set(counts)):
                raise ValueError(
                    f"`{output_key}` is missing partitions that the other outputs of its Producer have: {missing}."
                )
        for output, partitions in output_partitions.items():
            connection.write_snapshot_partitions(
                snapshot, graph.artifact_to_key[output], output, partitions
            )
        return all_keys

    def plan_producer(self, snapshot: GraphSnapshot, producer: Producer) -> ProducerPlan:
        """Compute the Producer's partitions and link any that are already built."""
        # Keep database transactions away from user-defined map/build functions. Besides reducing
        # lock contention, this avoids trying to nest a second connection on the shared physical
        # connection used by an in-memory SQLite backend.
        with snapshot.backend.connect() as connection:
            input_partitions = self.get_producer_inputs(snapshot, connection, producer)
        dependencies, input_fingerprints = producer.compute_dependencies(input_partitions)
        with snapshot.backend.connect() as connection:
            existing = self.discover_producer_partitions(
                snapshot, connection, producer, partition_input_fingerprints=input_fingerprints
            )
        return ProducerPlan(
            producer=producer,
            dependencies=dependencies,
            input_fingerprints=input_fingerprints,
            existing=frozenset(existing),
        )

    def build_planned_partition(
        self, snapshot: GraphSnapshot, plan: ProducerPlan, partition_key: PartitionKey
    ) -> None:
        self.build_producer_partition(
            snapshot,
            plan.producer,
            input_fingerprint=assert_not_none(plan.input_fingerprints[partition_key]),
            input_partitions=plan.dependencies[partition_key],
            partition_key=partition_key,
        )

    def build_producer_partition(
        self,
        snapshot: GraphSnapshot,
        producer: Producer,
        *,
        input_fingerprint: Fingerprint,
        input_partitions: InputPartitions,
        partition_key: PartitionKey,
    ) -> None:
        """
        Build one Producer partition, unless it already exists.

        Existence is checked again here, rather than only when planning, so a partition that was
        published by a concurrent writer (or a retried attempt) since planning is linked instead of
        rebuilt.

        The outputs are written to Storage first and then registered with the Backend in one
        transaction, so a failure part way through never registers only some of the outputs.
        Storage writes are not transactional though: a failed attempt may leave unregistered data
        behind, which a later attempt overwrites.
        """
        pk_str = f" for: {dict(partition_key)}" if partition_key else ""
        with snapshot.backend.connect() as connection:
            if self.discover_producer_partitions(
                snapshot,
                connection,
                producer,
                partition_input_fingerprints=InputFingerprints({partition_key: input_fingerprint}),
            ):
                logger.info(f"Skipping existing {type(producer).__name__} output{pk_str}")
                return
        logger.info(
            f"Building {type(producer).__name__} output{pk_str} (inputs {input_fingerprint})"
        )
        arguments = {
            name: snapshot.read(
                artifact=producer.inputs[name],
                storage_partition_snapshots=input_partitions[name],
                view=view,
            )
            for name, view in producer._build_inputs_.items()  # pyright: ignore[reportPrivateUsage]
        }
        outputs = producer.build(**arguments)
        if len(producer._outputs_) == 1:  # pyright: ignore[reportPrivateUsage]
            outputs = (outputs,)
        validation_passed, validation_message = producer.validate_outputs(*outputs)
        if not validation_passed:
            raise ValueError(validation_message)
        graph = snapshot.graph
        artifacts = graph.producer_outputs[producer]
        written: list[StoragePartitionSnapshot] = [
            graph._write_to_storage(  # pyright: ignore[reportPrivateUsage]
                output,
                artifact=artifact,
                input_fingerprint=input_fingerprint,
                partition_key=partition_key,
                view=producer._outputs_[i],  # pyright: ignore[reportPrivateUsage]
            )
            for i, (output, artifact) in enumerate(zip(outputs, artifacts, strict=True))
        ]
        with snapshot.backend.connect() as connection:
            for artifact, partition in zip(artifacts, written, strict=True):
                connection.write_artifact_and_graph_partitions(
                    snapshot, graph.artifact_to_key[artifact], artifact, (partition,)
                )


__all__ = [
    "Executor",
    "ProducerPlan",
    "producer_dependencies",
    "producers_in_order",
]
