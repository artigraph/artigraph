from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
import logging
from itertools import chain

from arti.backends import Backend
from arti.fingerprints import Fingerprint
from arti.graphs import GraphSnapshot
from arti.internal.models import Model
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, InputFingerprints
from arti.producers import InputPartitions, Producer
from arti.storage import StoragePartitions


class Executor(Model):
    @abc.abstractmethod
    def build(self, snapshot: GraphSnapshot) -> None:
        raise NotImplementedError()

    def get_producer_inputs(
        self, snapshot: GraphSnapshot, backend: Backend, producer: Producer
    ) -> InputPartitions:
        return InputPartitions(
            {
                name: backend.read_graph_partitions(
                    snapshot.name, snapshot.id, snapshot.graph.artifact_to_key[artifact], artifact
                )
                for name, artifact in producer.inputs.items()
            }
        )

    def discover_producer_partitions(
        self,
        snapshot: GraphSnapshot,
        backend: Backend,
        producer: Producer,
        *,
        partition_input_fingerprints: InputFingerprints,
    ) -> set[CompositeKey]:
        # NOTE: The output partitions may be built, but not yet associated with this GraphSnapshot
        # (eg: raw input data changed, but no changes trickled into this specific Producer). Hence
        # we'll fetch all StoragePartitions for each Storage, filtered to the PKs and
        # input_fingerprints we've computed *are* for this snapshot - and then link them to the
        # snapshot.
        existing_output_partitions = {
            output: backend.read_artifact_partitions(output, partition_input_fingerprints)
            for output in snapshot.graph.producer_outputs[producer]
        }
        for artifact, partitions in existing_output_partitions.items():
            backend.write_graph_partitions(
                snapshot.name,
                snapshot.id,
                snapshot.graph.artifact_to_key[artifact],
                artifact,
                partitions,
            )
        # TODO: Guarantee all outputs have the same set of identified partitions. Currently, this
        # pretends a partition is built for all outputs if _any_ are built for that partition.
        return {
            partition.keys for partition in chain.from_iterable(existing_output_partitions.values())
        }

    def build_producer_partition(
        self,
        snapshot: GraphSnapshot,
        backend: Backend,
        producer: Producer,
        *,
        existing_partition_keys: set[CompositeKey],
        input_fingerprint: Fingerprint,
        partition_dependencies: frozendict[str, StoragePartitions],
        partition_key: CompositeKey,
    ) -> None:
        # TODO: Should this "skip if exists" live here or higher up?
        if partition_key in existing_partition_keys:
            pk_str = f" for: {dict(partition_key)}" if partition_key else "."
            logging.info(f"Skipping existing {type(producer).__name__} output{pk_str}")
            return
        logging.info(f"Building {producer} output for {partition_key}...")
        # TODO: Catch DispatchError and give a nicer error... maybe add this to our
        # @dispatch wrapper (eg: msg arg, or even fn that returns the message to
        # raise).
        arguments = {
            name: snapshot.read(
                artifact=producer.inputs[name],
                storage_partitions=partition_dependencies[name],
                view=view,
            )
            for name, view in producer._build_inputs_.items()
        }
        outputs = producer.build(**arguments)
        if len(producer._outputs_) == 1:
            outputs = (outputs,)
        validation_passed, validation_message = producer.validate_outputs(*outputs)
        if not validation_passed:
            raise ValueError(validation_message)
        for i, output in enumerate(outputs):
            snapshot.write(
                output,
                artifact=snapshot.graph.producer_outputs[producer][i],
                input_fingerprint=input_fingerprint,
                keys=partition_key,
                view=producer._outputs_[i],
            )
