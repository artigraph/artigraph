import logging
from graphlib import TopologicalSorter
from itertools import chain

from arti.artifacts import Artifact
from arti.backends import Backend
from arti.executors import Executor
from arti.graphs import Graph
from arti.producers import Producer
from arti.storage import InputFingerprints

# TODO: Factor this code out to reusable helpers/better homes.
#   - Perhaps a lot of the _build_producer logic can live in a few Producer methods. We
#   still want to expose each partition-to-build as a parallelizable unit of execution,
#   so maybe one Producer method kicks out all to-be-built partitions+metadata, and then
#   Executor can parallelize the `.build`.


class LocalExecutor(Executor):
    # TODO: Separate .map and .build steps so we can:
    # - add "sync" / "dry run" sort of things
    # - parallelize build
    #
    # We may still want to repeat the .map phase in the future, if we wanted to support some sort of
    # iterated or cyclic Producers (eg: first pass output feeds into second run - in that case,
    # `.map` should describe how to "converge" by returning the same outputs as a prior call).
    def _build_producer(
        self,
        graph: Graph,
        backend: Backend,
        producer: Producer,
    ) -> None:
        input_partitions = {
            name: backend.read_graph_partitions(
                graph.name, graph.get_snapshot_id(), graph.artifact_to_key[artifact], artifact
            )
            for name, artifact in producer.inputs.items()
        }
        partition_dependencies = producer.map(
            **{
                name: partitions
                for name, partitions in input_partitions.items()
                if name in producer._map_inputs_
            }
        )
        # TODO: Need to validate the partition_dependencies against the Producer's
        # partitioning scheme and such (basically, check user error). eg: if output is
        # not partitioned, we expect only 1 entry in partition_dependencies
        # (NotPartitioned).
        partition_input_fingerprints = InputFingerprints(
            {
                composite_key: producer.compute_input_fingerprint(dependency_partitions)
                for composite_key, dependency_partitions in partition_dependencies.items()
            }
        )
        output_artifacts = graph.producer_outputs[producer]
        # NOTE: The output partitions may be built, but not yet associated with this snapshot_id
        # (eg: raw input data changed, but no changes trickled into this specific Producer). Hence
        # we'll fetch all StoragePartitions for this Storage, filtered to the PKs and
        # input_fingerprints we've computed *are* for this Graph - and then link them to the graph.
        existing_output_partitions = {
            output: backend.read_artifact_partitions(output, partition_input_fingerprints)
            for output in output_artifacts
        }
        for artifact, partitions in existing_output_partitions.items():
            backend.write_graph_partitions(
                graph.name,
                graph.get_snapshot_id(),
                graph.artifact_to_key[artifact],
                artifact,
                partitions,
            )
        # TODO: Guarantee all outputs have the same set of identified partitions. Currently, this
        # pretends a partition is built for all outputs if _any_ are built for that partition.
        existing_output_keys = {
            partition.keys for partition in chain.from_iterable(existing_output_partitions.values())
        }
        for output_partition_key, dependencies in partition_dependencies.items():
            if output_partition_key in existing_output_keys:
                pk_str = f" for: {dict(output_partition_key)}" if output_partition_key else "."
                logging.info(f"Skipping existing {type(producer).__name__} output{pk_str}")
                continue
            # TODO: Catch DispatchError and give a nicer error... maybe add this to our
            # @dispatch wrapper (eg: msg arg, or even fn that returns the message to
            # raise).
            arguments = {
                name: graph.read(
                    artifact=producer.inputs[name],
                    storage_partitions=partition_dependencies[output_partition_key][name],
                    view=ioinfo.view,
                )
                for name, ioinfo in producer._build_inputs_.items()
            }
            outputs = producer.build(**arguments)
            if len(producer._outputs_) == 1:
                outputs = (outputs,)
            validation_passed, validation_message = producer.validate_outputs(*outputs)
            if not validation_passed:
                raise ValueError(validation_message)
            for i, output in enumerate(outputs):
                graph.write(
                    output,
                    artifact=output_artifacts[i],
                    input_fingerprint=partition_input_fingerprints[output_partition_key],
                    keys=output_partition_key,
                    view=producer._outputs_[i].view,
                )

    def build(self, graph: Graph) -> None:
        # NOTE: Raw Artifacts will already be discovered and linked in the backend to this graph
        # snapshot.
        assert graph.snapshot_id is not None
        with graph.backend.connect() as backend:
            for node in TopologicalSorter(graph.dependencies).static_order():
                if isinstance(node, Artifact):
                    # TODO: Compute Statistics (if not already computed for the partition) and check
                    # Thresholds (every time, as they may be changed, dynamic, or overridden).
                    pass
                elif isinstance(node, Producer):
                    logging.info(f"Building {node}...")
                    self._build_producer(graph, backend, node)
                else:
                    raise NotImplementedError()
        logging.info("Build finished.")
