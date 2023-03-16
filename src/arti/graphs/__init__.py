from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import cached_property, wraps
from graphlib import TopologicalSorter
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeVar, Union, cast

from pydantic import Field, PrivateAttr

import arti
from arti import io
from arti.artifacts import Artifact
from arti.backends import Backend
from arti.backends.memory import MemoryBackend
from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.internal.utils import TypedBox, frozendict
from arti.partitions import CompositeKey
from arti.producers import Producer
from arti.storage import StoragePartition, StoragePartitions
from arti.types import is_partitioned
from arti.views import View

if TYPE_CHECKING:
    from arti.executors import Executor
else:
    from arti.internal.patches import patch_TopologicalSorter_class_getitem

    patch_TopologicalSorter_class_getitem()


SEALED: Literal[True] = True
OPEN: Literal[False] = False
BOX_KWARGS = {
    status: {
        "box_dots": True,
        "default_box": status is OPEN,
        "frozen_box": status is SEALED,
    }
    for status in (OPEN, SEALED)
}

_Return = TypeVar("_Return")


def requires_sealed(fn: Callable[..., _Return]) -> Callable[..., _Return]:
    @wraps(fn)
    def check_if_sealed(self: Graph, *args: Any, **kwargs: Any) -> _Return:
        if self._status is not SEALED:
            raise ValueError(f"{fn.__name__} cannot be used while the Graph is still being defined")
        return fn(self, *args, **kwargs)

    return check_if_sealed


class ArtifactBox(TypedBox[Artifact]):
    def _TypedBox__cast_value(self, item: str, artifact: Any) -> Artifact:
        artifact = super()._TypedBox__cast_value(item, artifact)  # type: ignore[misc]
        storage_template_values = dict[str, Any]()
        if (graph := arti.context.graph) is not None:
            storage_template_values.update(
                {
                    "graph_name": graph.name,
                    "names": (*self._box_config["box_namespace"], item),
                    "path_tags": graph.path_tags,
                }
            )
        # Require an {input_fingerprint} template in the Storage if this Artifact is being generated
        # by a Producer. Otherwise, strip the {input_fingerprint} template (if set) for "raw"
        # Artifacts.
        #
        # We can't validate this at Artifact instantiation because the Producer is tracked later (by
        # copying the instance and setting the `producer_output` attribute). We won't know the
        # "final" instance until assignment here to the Graph.
        if artifact.producer_output is None:
            storage_template_values["input_fingerprint"] = Fingerprint.empty()
        elif not artifact.storage.includes_input_fingerprint_template:
            raise ValueError(
                "Produced Artifacts must have a '{input_fingerprint}' template in their Storage"
            )
        return cast(
            Artifact,
            artifact.copy(
                update={"storage": artifact.storage.resolve_templates(**storage_template_values)}
            ),
        )


Node = Union[Artifact, Producer]
NodeDependencies = frozendict[Node, frozenset[Node]]


class Graph(Model):
    """Graph stores a web of Artifacts connected by Producers."""

    _fingerprint_excludes_ = frozenset(["backend"])

    name: str
    backend: Backend = Field(default_factory=MemoryBackend)
    path_tags: frozendict[str, str] = frozendict()

    # Graph starts off sealed, but is opened within a `with Graph(...)` context
    _status: Optional[bool] = PrivateAttr(None)
    _artifacts: ArtifactBox = PrivateAttr(default_factory=lambda: ArtifactBox(**BOX_KWARGS[SEALED]))
    _artifact_to_key: frozendict[Artifact, str] = PrivateAttr(frozendict())

    def __enter__(self) -> Graph:
        if arti.context.graph is not None:
            raise ValueError(f"Another graph is being defined: {arti.context.graph}")
        arti.context.graph = self
        self._toggle(OPEN)
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> None:
        arti.context.graph = None
        self._toggle(SEALED)
        # Confirm the dependencies are acyclic
        TopologicalSorter(self.dependencies).prepare()

    def _toggle(self, status: bool) -> None:
        self._status = status
        self._artifacts = ArtifactBox(self.artifacts, **BOX_KWARGS[status])
        self._artifact_to_key = frozendict(
            {artifact: key for key, artifact in self.artifacts.walk()}
        )

    @property
    def artifacts(self) -> ArtifactBox:
        return self._artifacts

    @property
    def artifact_to_key(self) -> frozendict[Artifact, str]:
        return self._artifact_to_key

    @requires_sealed
    def build(self, executor: Optional[Executor] = None) -> GraphSnapshot:
        return self.snapshot().build(executor)

    @requires_sealed
    def snapshot(self) -> GraphSnapshot:
        """Identify a "unique" ID for this Graph at this point in time.

        The ID aims to encode the structure of the Graph plus a _snapshot_ of the raw Artifact data
        (partition kinds and contents). Any change that would affect data should prompt an ID
        change, however changes to this ID don't directly cause data to be reproduced.

        NOTE: There is currently a gap (and thus race condition) between when the Graph ID is
        computed and when we read raw Artifacts data during Producer builds.
        """
        return GraphSnapshot.from_graph(self)

    @cached_property
    @requires_sealed
    def dependencies(self) -> NodeDependencies:
        artifact_deps = {
            artifact: (
                frozenset({artifact.producer_output.producer})
                if artifact.producer_output is not None
                else frozenset()
            )
            for _, artifact in self.artifacts.walk()
        }
        producer_deps = {
            # NOTE: multi-output Producers will appear multiple times (but be deduped)
            producer_output.producer: frozenset(producer_output.producer.inputs.values())
            for artifact in artifact_deps
            if (producer_output := artifact.producer_output) is not None
        }
        return NodeDependencies(artifact_deps | producer_deps)

    @cached_property
    @requires_sealed
    def producers(self) -> frozenset[Producer]:
        return frozenset(self.producer_outputs)

    @cached_property
    @requires_sealed
    def producer_outputs(self) -> frozendict[Producer, tuple[Artifact, ...]]:
        d = defaultdict[Producer, dict[int, Artifact]](dict)
        for _, artifact in self.artifacts.walk():
            if artifact.producer_output is None:
                continue
            output = artifact.producer_output
            d[output.producer][output.position] = artifact
        return frozendict(
            (producer, tuple(artifacts_by_position[i] for i in sorted(artifacts_by_position)))
            for producer, artifacts_by_position in d.items()
        )


class GraphSnapshot(Model):
    """GraphSnapshot represents the state of a Graph and the referenced raw data at a point in time.

    GraphSnapshot encodes the structure of the Graph plus a snapshot of the raw Artifact data
    (partition kinds and contents) at a point in time. Any change that would affect data should
    prompt an ID change.
    """

    id: Fingerprint
    graph: Graph

    @property
    def artifacts(self) -> ArtifactBox:
        return self.graph.artifacts

    @property
    def backend(self) -> Backend:
        return self.graph.backend

    @property
    def name(self) -> str:
        return self.graph.name

    @classmethod  # TODO: Should this use a (TTL) cache? Raw data changes (especially in tests) still need to be detected.
    def from_graph(cls, graph: Graph) -> GraphSnapshot:
        """Snapshot the Graph and all existing raw data.

        NOTE: There is currently a gap (and thus race condition) between when the Graph ID is
        computed and when we read raw Artifact data during Producer builds.
        """
        # TODO: Resolve and statically set all available fingerprints. Specifically, we should pin
        # the Producer.fingerprint, which may by dynamic (eg: version is a Timestamp). Unbuilt
        # Artifact (partitions) won't be fully resolved yet.
        snapshot_id, known_artifact_partitions = graph.fingerprint, dict[str, StoragePartitions]()
        for node, _ in graph.dependencies.items():
            snapshot_id = snapshot_id.combine(node.fingerprint)
            if isinstance(node, Artifact):
                key = graph.artifact_to_key[node]
                snapshot_id = snapshot_id.combine(Fingerprint.from_string(key))
                # Include fingerprints (including content_fingerprint!) for all raw Artifact
                # partitions, triggering a graph ID change if these artifacts change out-of-band.
                #
                # TODO: Should we *also* inspect Producer.inputs for Artifacts _not_ inside this
                # Graph and inspect their contents too? I guess we'll have to handle things a bit
                # differently depending on if the external Artifacts are Produced (in an upstream
                # Graph) or not.
                if node.producer_output is None:
                    known_artifact_partitions[key] = StoragePartitions(
                        partition.with_content_fingerprint()
                        for partition in node.storage.discover_partitions()
                    )
                    if not known_artifact_partitions[key]:
                        content_str = "partitions" if is_partitioned(node.type) else "data"
                        raise ValueError(f"No {content_str} found for `{key}`: {node}")
                    snapshot_id = snapshot_id.combine(
                        *[partition.fingerprint for partition in known_artifact_partitions[key]]
                    )
        if snapshot_id.is_empty or snapshot_id.is_identity:  # pragma: no cover
            # NOTE: This shouldn't happen unless the logic above is faulty.
            raise ValueError("Fingerprint is empty!")
        snapshot = cls(graph=graph, id=snapshot_id)
        # Write the discovered partitions (if not already known) and link to this new snapshot.
        with snapshot.backend.connect() as backend:
            for key, partitions in known_artifact_partitions.items():
                backend.write_artifact_and_graph_partitions(
                    snapshot.artifacts[key], partitions, graph.name, snapshot.id, key
                )
        return snapshot

    def build(self, executor: Optional[Executor] = None) -> GraphSnapshot:
        if executor is None:
            from arti.executors.local import LocalExecutor

            executor = LocalExecutor()
        executor.build(self)
        return self

    def tag(self, tag: str, overwrite: bool = False) -> None:
        with self.backend.connect() as backend:
            backend.write_graph_tag(self.name, self.id, tag, overwrite)

    # TODO: We need to implement building a Graph(Snapshot) by pulling *all* info from the Backend.
    #
    # @classmethod
    # def from_tag(cls, name: str, tag: str, *, backend: Backend) -> GraphSnapshot:
    #     with backend.connect() as backend:
    #         snapshot_id = backend.read_graph_tag(name, tag)})

    # TODO: io.read/write probably need a bit of sanity checking (probably somewhere else), eg: type
    # ~= view. Doing validation on the data, etc. Should some of this live on the View?

    def read(
        self,
        artifact: Artifact,
        *,
        annotation: Optional[Any] = None,
        storage_partitions: Optional[Sequence[StoragePartition]] = None,
        view: Optional[View] = None,
    ) -> Any:
        key = self.graph.artifact_to_key[artifact]
        if annotation is None and view is None:
            raise ValueError("Either `annotation` or `view` must be passed")
        if annotation is not None and view is not None:
            raise ValueError("Only one of `annotation` or `view` may be passed")
        if annotation is not None:
            view = View.get_class_for(annotation)(
                artifact_class=type(artifact), type=artifact.type, mode="READ"
            )
            view.check_annotation_compatibility(annotation)
            view.check_artifact_compatibility(artifact)
        assert view is not None  # mypy gets mixed up with ^
        if storage_partitions is None:
            # We want to allow reading raw Artifacts even if other raw Artifacts are missing (which prevents
            # snapshotting).
            if artifact.producer_output is None:
                # NOTE: We're not using read_artifact_partitions as the underlying data may have
                # changed. The backend may have pointers to old versions (which is expected), but we
                # only want to return the current values.
                storage_partitions = artifact.storage.discover_partitions()
            else:
                with self.backend.connect() as backend:
                    storage_partitions = backend.read_graph_partitions(
                        self.name, self.id, key, artifact
                    )
        return io.read(
            type_=artifact.type,
            format=artifact.format,
            storage_partitions=storage_partitions,
            view=view,
        )

    def write(
        self,
        data: Any,
        *,
        artifact: Artifact,
        input_fingerprint: Fingerprint = Fingerprint.empty(),
        keys: CompositeKey = CompositeKey(),
        view: Optional[View] = None,
    ) -> StoragePartition:
        key = self.graph.artifact_to_key[artifact]
        if artifact.producer_output is None:
            raise ValueError(
                f"Writing to a raw Artifact (`{key}`) is not supported - update the original `Graph` instead."
            )
        if view is None:
            view = View.get_class_for(type(data))(
                artifact_class=type(artifact), type=artifact.type, mode="WRITE"
            )
        view.check_annotation_compatibility(type(data))
        view.check_artifact_compatibility(artifact)
        storage_partition = artifact.storage.generate_partition(
            input_fingerprint=input_fingerprint, keys=keys, with_content_fingerprint=False
        )
        storage_partition = io.write(
            data,
            type_=artifact.type,
            format=artifact.format,
            storage_partition=storage_partition,
            view=view,
        ).with_content_fingerprint()
        # TODO: Should we only do this in bulk? We might want the backends to transparently batch
        # requests, but that's not so friendly with the transient ".connect".
        with self.backend.connect() as backend:
            backend.write_artifact_partitions(artifact, (storage_partition,))
            backend.write_graph_partitions(self.name, self.id, key, artifact, (storage_partition,))
        return cast(StoragePartition, storage_partition)
