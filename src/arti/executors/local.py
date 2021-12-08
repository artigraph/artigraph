import logging
from graphlib import TopologicalSorter
from itertools import chain

from arti.artifacts import Artifact
from arti.backends import Backend
from arti.executors import Executor
from arti.fingerprints import Fingerprint
from arti.graphs import Graph
from arti.producers import Producer
from arti.storage import InputFingerprints

# TODO: Factor this code out to reusable helpers/better homes.
#   - Perhaps a lot of the _build_producer logic can live in a few Producer methods. We
#   still want to expose each partition-to-build as a parallelizable unit of execution,
#   so maybe one Producer method kicks out all to-be-built partitions+metadata, and then
#   Executor can parallelize the `.build`.


class LocalExecutor(Executor):
    def _sync_artifact(
        self,
        graph: Graph,
        graph_id: Fingerprint,
        backend: Backend,
        artifact: Artifact,
    ) -> None:
        # NOTE: Should we do different things for "raw" vs produced artifacts?
        #
        # Eg: for raw we discover partitions, but for generated we... just
        # lookup from the Backend somehow? For all, we should probably compute
        # the statistics (or maybe compute should be part of the Producer step
        # using the exiting Views?) and check thresholds. Threshold checking
        # should apply for every run, but the statitics should no-op if already
        # computed obviously.
        #
        # We can only discover raw Artifacts - all produced ones should be written after built.
        if artifact.producer_output is None:
            partitions = artifact.discover_storage_partitions()
            backend.write_storage_partitions_and_link_to_graph(
                artifact.storage, partitions, graph_id, graph.artifact_to_key[artifact]
            )
        # TODO: Check any assumptions about what/how many should be in the db?
        # TODO: Calculate stats, run validation, etc...

    # NOTE: This should probably be broken up to separate the .map and .build (ie: so we
    # can map ahead of time and then parallelize build calls).
    #
    # TODO: Split into "see what partitions need built" and "build partition" so we can implement
    # "sync" / "dry run" sort of things?
    def _build_producer(
        self,
        graph: Graph,
        graph_id: Fingerprint,
        backend: Backend,
        producer: Producer,
    ) -> None:
        input_partitions = {
            name: backend.read_graph_partitions(graph_id, graph.artifact_to_key[artifact])
            for name, artifact in producer.inputs.items()
        }
        partition_dependencies = producer.map(
            **{
                name: partitions
                for name, partitions in input_partitions.items()
                if name in producer._map_input_metadata_
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
        # NOTE: The output partitions may be built, but not yet associated with this graph_id (eg:
        # raw input data changed, but no changes trickled into this specific Producer). Hence we'll
        # fetch all StoragePartitions for this Storage, filtered to the PKs and input_fingerprints
        # we've computed *are* for this Graph - and then link them to the graph.
        existing_output_partitions = {
            output: backend.read_storage_partitions(output.storage, partition_input_fingerprints)
            for output in output_artifacts
        }
        for artifact, partitions in existing_output_partitions.items():
            backend.link_graph_partitions(graph_id, graph.artifact_to_key[artifact], partitions)
        # TODO: Guarantee all outputs have the same set of identified partitions. Currently, this
        # pretends a partition is built for all outputs if _any_ are built for that partition.
        existing_output_keys = set(
            partition.keys for partition in chain.from_iterable(existing_output_partitions.values())
        )
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
                    graph_id=graph_id,
                    storage_partitions=partition_dependencies[output_partition_key][name],
                    # TODO: We'll probably need to update Producer._build_input_views_
                    # to hold *instances* (either objects set in Annotated or init
                    # during Producer.__init_subclass__)
                    view=view_class(),
                )
                for name, view_class in producer._build_input_views_.items()
            }
            outputs = producer.build(**arguments)
            if len(producer._output_metadata_) == 1:
                outputs = (outputs,)
            validation_passed, validation_message = producer.validate_outputs(*outputs)
            if not validation_passed:
                raise ValueError(validation_message)
            for i, output in enumerate(outputs):
                graph.write(
                    output,
                    artifact=output_artifacts[i],
                    graph_id=graph_id,
                    input_fingerprint=partition_input_fingerprints[output_partition_key],
                    keys=output_partition_key,
                    view=producer._output_metadata_[i][1](),
                )

    # TODO: Support "dry_run"?
    def build(self, graph: Graph) -> None:
        graph_id = graph.compute_id()
        with graph.backend.connect() as backend:
            for node in TopologicalSorter(graph.dependencies).static_order():
                if isinstance(node, Artifact):
                    logging.info(f"Syncing {node}...")
                    self._sync_artifact(graph, graph_id, backend, node)
                elif isinstance(node, Producer):
                    logging.info(f"Building {node}...")
                    self._build_producer(graph, graph_id, backend, node)
                else:
                    raise NotImplementedError()
        logging.info("Build finished.")
