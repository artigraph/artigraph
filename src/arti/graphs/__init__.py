from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from collections import defaultdict
from collections.abc import Callable
from functools import cached_property, wraps
from graphlib import TopologicalSorter
from types import TracebackType
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypeVar

from pydantic import Field, PrivateAttr, field_validator

import arti
from arti import io
from arti.artifacts import Artifact
from arti.backends import Backend, BackendConnection
from arti.fingerprints import Fingerprint, SkipFingerprint
from arti.internal.mappings import FrozenMapping, TypedBox, frozendict
from arti.internal.models import Model
from arti.partitions import PartitionKey
from arti.producers import Producer
from arti.storage import StoragePartitionSnapshot, StoragePartitionSnapshots
from arti.types import is_partitioned
from arti.views import View

if TYPE_CHECKING:
    from arti.backends.memory import MemoryBackend
    from arti.executors import Executor


def _get_memory_backend() -> MemoryBackend:
    # Avoid importing non-root modules upon import
    from arti.backends.memory import MemoryBackend

    return MemoryBackend()


type STATUS = Literal["open", "closed"]
OPEN: Literal["open"] = "open"
CLOSED: Literal["closed"] = "closed"

_Return = TypeVar("_Return")


def requires_sealed(fn: Callable[..., _Return]) -> Callable[..., _Return]:
    @wraps(fn)
    def check_if_sealed(self: Graph, *args: Any, **kwargs: Any) -> _Return:
        if self._status is not CLOSED:
            raise ValueError(f"{fn.__name__} cannot be used while the Graph is still being defined")
        return fn(self, *args, **kwargs)

    return check_if_sealed


class ArtifactBox(TypedBox[Artifact]):
    def _cast_value(self, key: str, value: Any) -> Artifact:
        artifact = super()._cast_value(key, value)
        storage = artifact.storage
        if (graph := arti.context.graph) is not None:
            # NOTE: While we could `._visit_names` even without the Graph, it's helpful to late bind
            # in case the structure changes.
            storage = storage._visit_graph(graph)._visit_names((*self._namespace, key))
        # Require an {input_fingerprint} template in the Storage if this Artifact is being generated
        # by a Producer. Otherwise, strip the {input_fingerprint} template (if set) for "raw"
        # Artifacts.
        #
        # We can't validate this at Artifact instantiation because the Producer is tracked later (by
        # copying the instance and setting the `producer_output` attribute). We won't know the
        # "final" instance until assignment here to the Graph.
        if artifact.producer_output is None:
            storage = storage._visit_input_fingerprint(None)
        elif not storage.includes_input_fingerprint_template:
            raise ValueError(
                "Produced Artifacts must have a '{input_fingerprint}' template in their Storage"
            )
        # TODO: Replace references to the original in any downstream Producers...
        return artifact.model_copy(update={"storage": storage})

    # Override __getattr__ to Any (instead of `Artifact | ArtifactBox`) to reduce type checking
    # noise for common usage. Otherwise:
    # - every access after Graph definition needs to be narrowed to `Artifact`
    # - every intermediate attribute with nested keys needs to be narrowed to `TypedBox[Artifact]`
    #
    # This isn't correct, but the ergonomics are worth the tradeoff.
    def __getattr__(self, key: str) -> Any:
        return super().__getattr__(key)


Node = Artifact | Producer
NodeDependencies = frozendict[Node, frozenset[Node]]


class Graph(Model):
    """Graph stores a web of Artifacts connected by Producers."""

    name: str
    artifacts: ArtifactBox = Field(default_factory=ArtifactBox)
    # The Backend *itself* should not affect the results of a Graph build, though the contents
    # certainly may (eg: stored annotations), so we avoid serializing it. This also prevents
    # embedding any credentials.
    backend: Annotated[Backend, SkipFingerprint()] = Field(
        default_factory=_get_memory_backend, exclude=True
    )
    path_tags: FrozenMapping[str, str] = frozendict()

    # Graph starts off sealed, but is opened within a `with Graph(...)` context
    _status: STATUS | None = PrivateAttr(default=None)
    _artifact_to_key: FrozenMapping[Artifact, str] = frozendict()

    @field_validator("artifacts")
    @classmethod
    def _convert_artifacts(cls, artifacts: ArtifactBox) -> ArtifactBox:
        artifacts._status.root = CLOSED
        return artifacts

    def __enter__(self) -> Graph:
        if arti.context.graph is not None:
            raise ValueError(f"Another graph is being defined: {arti.context.graph}")
        arti.context.graph = self
        self._toggle(OPEN)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        arti.context.graph = None
        self._toggle(CLOSED)
        # Confirm the dependencies are acyclic
        TopologicalSorter(self.dependencies).prepare()

    def _toggle(self, status: Literal["open", "closed"]) -> None:
        # The Graph object is "frozen", so we must bypass the assignment checks.
        self.artifacts._status.root = status
        self._status = status
        self._artifact_to_key = frozendict(
            {artifact: key for key, artifact in self.artifacts.walk()}
        )

    @property
    def artifact_to_key(self) -> frozendict[Artifact, str]:
        return self._artifact_to_key

    @requires_sealed
    def build(self, executor: Executor | None = None) -> GraphSnapshot:
        return self.snapshot().build(executor)

    @requires_sealed
    def snapshot(self, *, connection: BackendConnection | None = None) -> GraphSnapshot:
        """Identify a "unique" ID for this Graph at this point in time.

        The ID aims to encode the structure of the Graph plus a _snapshot_ of the raw Artifact data
        (partition kinds and contents). Any change that would affect data should prompt an ID
        change, however changes to this ID don't directly cause data to be reproduced.

        NOTE: There is currently a gap (and thus race condition) between when the Graph ID is
        computed and when we read raw Artifacts data during Producer builds.
        """
        return GraphSnapshot.from_graph(self, connection=connection)

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
        return NodeDependencies(artifact_deps | producer_deps)  # type: ignore[operator]

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

    # TODO: io.read/write probably need a bit of sanity checking (probably somewhere else), eg: type
    # ~= view. Doing validation on the data, etc. Should some of this live on the View?

    @requires_sealed
    def read(
        self,
        artifact: Artifact,
        *,
        annotation: Any | None = None,
        connection: BackendConnection | None = None,
        snapshot: GraphSnapshot | None = None,
        storage_partition_snapshots: StoragePartitionSnapshots | None = None,
        view: View | None = None,
    ) -> Any:
        key = self.artifact_to_key[artifact]
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
        if storage_partition_snapshots is None:
            # We want to allow reading raw Artifacts even if other raw Artifacts are missing (which
            # prevents snapshotting).
            if snapshot is None and artifact.producer_output is None:
                # NOTE: We're not using read_artifact_partitions as the underlying data may have
                # changed. The backend may have pointers to old versions (which is expected), but we
                # only want to return the current values.
                storage_partition_snapshots = artifact.storage.discover_partitions()
            else:
                snapshot = snapshot or self.snapshot()
                with (connection or self.backend).connect() as conn:
                    storage_partition_snapshots = conn.read_snapshot_partitions(
                        snapshot, key, artifact
                    )
        return io.read(
            type_=artifact.type,
            format=artifact.format,
            storage_partition_snapshots=storage_partition_snapshots,
            view=view,
        )

    @requires_sealed
    def write(
        self,
        data: Any,
        *,
        artifact: Artifact,
        input_fingerprint: Fingerprint | None = None,
        partition_key: PartitionKey = PartitionKey(),
        view: View | None = None,
        snapshot: GraphSnapshot | None = None,
        connection: BackendConnection | None = None,
    ) -> StoragePartitionSnapshot:
        key = self.artifact_to_key[artifact]
        if snapshot is not None and artifact.producer_output is None:
            raise ValueError(
                f"Writing to a raw Artifact (`{key}`) with a GraphSnapshot is not supported."
            )
        if view is None:
            view = View.get_class_for(type(data))(
                artifact_class=type(artifact), type=artifact.type, mode="WRITE"
            )
        view.check_annotation_compatibility(type(data))
        view.check_artifact_compatibility(artifact)
        storage_partition_snapshot = io.write(
            data,
            type_=artifact.type,
            format=artifact.format,
            storage_partition=artifact.storage.generate_partition(
                input_fingerprint=input_fingerprint, partition_key=partition_key
            ),
            view=view,
        )
        # TODO: Should we only do this in bulk? We might want the backends to transparently batch
        # requests, but that's not so friendly with the transient ".connect".
        with (connection or self.backend).connect() as conn:
            conn.write_artifact_partitions(artifact, (storage_partition_snapshot,))
            # Skip linking this partition to the snapshot if it affects raw Artifacts (which would
            # trigger an id change).
            if snapshot is not None and artifact.producer_output is not None:
                conn.write_snapshot_partitions(
                    snapshot, key, artifact, (storage_partition_snapshot,)
                )
        return storage_partition_snapshot


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
    def from_graph(
        cls, graph: Graph, *, connection: BackendConnection | None = None
    ) -> GraphSnapshot:
        """Snapshot the Graph and all existing raw data.

        NOTE: There is currently a gap (and thus race condition) between when the Graph ID is
        computed and when we read raw Artifact data during Producer builds.
        """
        # TODO: Resolve and statically set all available fingerprints. Specifically, we should pin
        # the Producer.fingerprint, which may by dynamic (eg: version is a Timestamp). Unbuilt
        # Artifact (partitions) won't be fully resolved yet.
        snapshot_id = graph.fingerprint
        known_artifact_partitions = dict[str, StoragePartitionSnapshots]()
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
                    known_artifact_partitions[key] = node.storage.discover_partitions()
                    if not known_artifact_partitions[key]:
                        content_str = "partitions" if is_partitioned(node.type) else "data"
                        raise ValueError(f"No {content_str} found for `{key}`: {node}")
                    snapshot_id = snapshot_id.combine(
                        *[partition.fingerprint for partition in known_artifact_partitions[key]]
                    )
        snapshot = cls(graph=graph, id=snapshot_id)
        # Write the discovered partitions (if not already known) and link to this new snapshot.
        with (connection or snapshot.backend).connect() as conn:
            conn.write_graph(graph)
            conn.write_snapshot(snapshot)
            for key, partitions in known_artifact_partitions.items():
                conn.write_artifact_and_graph_partitions(
                    snapshot, key, snapshot.artifacts[key], partitions
                )
        return snapshot

    def build(self, executor: Executor | None = None) -> GraphSnapshot:
        if executor is None:
            from arti.executors.local import LocalExecutor

            executor = LocalExecutor()
        executor.build(self)
        return self

    def tag(
        self, tag: str, *, overwrite: bool = False, connection: BackendConnection | None = None
    ) -> None:
        with (connection or self.backend).connect() as conn:
            conn.write_snapshot_tag(self, tag, overwrite)

    @classmethod
    def from_tag(
        cls,
        name: str,
        tag: str,
        *,
        connectable: Backend | BackendConnection,
    ) -> GraphSnapshot:
        with connectable.connect() as conn:
            return conn.read_snapshot_tag(name, tag)

    def read(
        self,
        artifact: Artifact,
        *,
        annotation: Any | None = None,
        connection: BackendConnection | None = None,
        storage_partition_snapshots: StoragePartitionSnapshots | None = None,
        view: View | None = None,
    ) -> Any:
        return self.graph.read(
            artifact,
            annotation=annotation,
            storage_partition_snapshots=storage_partition_snapshots,
            view=view,
            snapshot=self,
            connection=connection,
        )

    def write(
        self,
        data: Any,
        *,
        artifact: Artifact,
        connection: BackendConnection | None = None,
        input_fingerprint: Fingerprint | None = None,
        partition_key: PartitionKey = PartitionKey(),
        view: View | None = None,
    ) -> StoragePartitionSnapshot:
        return self.graph.write(
            data,
            artifact=artifact,
            input_fingerprint=input_fingerprint,
            partition_key=partition_key,
            view=view,
            snapshot=self,
            connection=connection,
        )
