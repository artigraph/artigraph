from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
import logging
from itertools import chain

from arti.backends import BackendConnection
from arti.fingerprints import Fingerprint
from arti.graphs import GraphSnapshot
from arti.internal.models import Model
from arti.internal.utils import frozendict
from arti.partitions import InputFingerprints, PartitionKey
from arti.producers import InputPartitions, Producer
from arti.storage import StoragePartitions


class Executor(Model):
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
        # NOTE: The output partitions may be built, but not yet associated with this GraphSnapshot
        # (eg: raw input data changed, but no changes trickled into this specific Producer). Hence
        # we'll fetch all StoragePartitions for each Storage, filtered to the PKs and
        # input_fingerprints we've computed *are* for this snapshot - and then link them to the
        # snapshot.
        existing_output_partitions = {
            output: connection.read_artifact_partitions(output, partition_input_fingerprints)
            for output in snapshot.graph.producer_outputs[producer]
        }
        for artifact, partitions in existing_output_partitions.items():
            connection.write_snapshot_partitions(
                snapshot, snapshot.graph.artifact_to_key[artifact], artifact, partitions
            )
        # TODO: Guarantee all outputs have the same set of identified partitions. Currently, this
        # pretends a partition is built for all outputs if _any_ are built for that partition.
        return {
            partition.partition_key
            for partition in chain.from_iterable(existing_output_partitions.values())
        }

    def build_producer_partition(
        self,
        snapshot: GraphSnapshot,
        connection: BackendConnection,
        producer: Producer,
        *,
        existing_partition_keys: set[PartitionKey],
        input_fingerprint: Fingerprint,
        partition_dependencies: frozendict[str, StoragePartitions],
        partition_key: PartitionKey,
    ) -> None:
        # TODO: Should this "skip if exists" live here or higher up?
        if partition_key in existing_partition_keys:
            pk_str = f" for: {dict(partition_key)}" if partition_key else "."
            logging.info(f"Skipping existing {type(producer).__name__} output{pk_str}")
            return
        logging.info(
            f"Building {type(producer)} output for {partition_key} and inputs {input_fingerprint}..."
        )
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
                partition_key=partition_key,
                view=producer._outputs_[i],
            )
