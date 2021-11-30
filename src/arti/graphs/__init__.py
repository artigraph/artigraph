__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

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
from arti.storage import StoragePartition
from arti.views import View

if TYPE_CHECKING:
    from arti.executors import Executor
else:
    from arti.internal.patches import patch_TopologicalSorter_class_getitem

    patch_TopologicalSorter_class_getitem()

# TODO: Add GraphMetadata model


SEALED: Literal[True] = True
OPEN: Literal[False] = False
BOX_KWARGS = {
    status: {
        "default_box": status is OPEN,
        "frozen_box": status is SEALED,
    }
    for status in (OPEN, SEALED)
}

_Return = TypeVar("_Return")


def requires_sealed(fn: Callable[..., _Return]) -> Callable[..., _Return]:
    @wraps(fn)
    def check_if_sealed(self: "Graph", *args: Any, **kwargs: Any) -> _Return:
        if self._status is not SEALED:
            raise ValueError(f"{fn.__name__} cannot be used while the Graph is still being defined")
        return fn(self, *args, **kwargs)

    return check_if_sealed


class ArtifactBox(TypedBox[str, Artifact]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        object.__setattr__(self, "_path", ())
        super().__init__(*args, **kwargs)

    def _TypedBox__cast_value(self, item: str, artifact: Any) -> Artifact:
        artifact = super()._TypedBox__cast_value(item, artifact)  # type: ignore
        if (graph := arti.context.graph) is not None:
            artifact = artifact.copy(
                update={
                    "storage": (
                        artifact.storage.resolve_graph_name(graph.name)
                        .resolve_names(self._path + (item,))
                        .resolve_path_tags(graph.path_tags)
                    )
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
            artifact = artifact.copy(
                update={"storage": artifact.storage.resolve_input_fingerprint(Fingerprint.empty())}
            )
        elif not artifact.storage.includes_input_fingerprint_template:
            raise ValueError(
                "Produced Artifacts must have a '{input_fingerprint}' template in their Storage"
            )
        return cast(Artifact, artifact)

    def _Box__convert_and_store(self, item: str, value: Artifact) -> None:
        if isinstance(value, dict):
            super()._Box__convert_and_store(item, value)  # pylint: disable=no-member
            # TODO: Test if this works with `Box({"some": {"nested": "thing"}})`.
            # Guessing not, may need to put in an empty dict/box first, set the path,
            # and then update it.
            object.__setattr__(self[item], "_path", self._path + (item,))
        else:
            super()._Box__convert_and_store(item, value)  # pylint: disable=no-member

    def _Box__get_default(self, item: str, attr: bool = False) -> Any:
        value = super()._Box__get_default(item, attr=attr)  # type: ignore
        object.__setattr__(value, "_path", self._path + (item,))
        return value


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

    def __enter__(self) -> "Graph":
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
    def build(self, executor: "Optional[Executor]" = None) -> None:
        if executor is None:
            from arti.executors.local import LocalExecutor

            executor = LocalExecutor()
        # TODO: Resolve and statically set all available fingerprints. Specifically, we
        # should pin the Producer.fingerprint, which may by dynamic (eg: version is a
        # Timestamp). Unbuilt Artifact (partitions) won't be fully resolved yet.
        return executor.build(self)

    @requires_sealed
    def compute_id(self) -> Fingerprint:
        """Identify a "unique" ID for this Graph at this point in time.

        The ID aims to encode the structure of the Graph plus a _snapshot_ of the raw Artifact data
        (partition kinds and contents). Any change that would affect data should prompt an ID
        change, however changes to this ID don't directly cause data to be reproduced.

        NOTE: There is currently a gap (and thus race condition) between when the Graph ID is
        computed and when we read raw Artifacts data during Producer builds.
        """
        id = self.fingerprint
        for node, _ in self.dependencies.items():
            id = id.combine(node.fingerprint)
            if isinstance(node, Artifact):
                key = self.artifact_to_key[node]
                id = id.combine(Fingerprint.from_string(key))
                # Include fingerprints (including content_fingerprint!) for all raw Artifact
                # partitions, triggering a graph ID change if these artifacts change out-of-band.
                if node.producer_output is None:
                    partitions = [
                        partition.with_content_fingerprint()
                        for partition in node.discover_storage_partitions()
                    ]
                    if not partitions:
                        raise ValueError(f"No data (partitions) found for {key}!")
                    for partition in partitions:
                        id = id.combine(partition.fingerprint)
        if id.is_empty or id.is_identity:  # pragme: no cover
            # NOTE: This shouldn't happen unless the logic above is faulty.
            raise ValueError("Fingerprint is empty!")
        return id

    @cached_property  # type: ignore # python/mypy#1362
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
        return NodeDependencies(artifact_deps | producer_deps)  # type: ignore

    @cached_property  # type: ignore # python/mypy#1362
    @requires_sealed
    def producers(self) -> frozenset[Producer]:
        return frozenset(self.producer_outputs)

    @cached_property  # type: ignore # python/mypy#1362
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

    # TODO: io.read/write probably need a bit of sanity checking (probably somewhere
    # else), eg: type ~= view. Doing validation on the data, etc. Should some of this
    # live on the View?

    def read(
        self,
        artifact: Artifact,
        *,
        storage_partitions: Optional[Sequence[StoragePartition]] = None,
        annotation: Optional[Any] = None,
        view: Optional[View] = None,
    ) -> Any:
        if annotation is None and view is None:
            raise ValueError("Either `annotation` or `view` must be passed")
        elif annotation is not None and view is not None:
            raise ValueError("Only one of `annotation` or `view` may be passed")
        elif annotation is not None:
            view = View.get_class_for(annotation)()
        if storage_partitions is None:
            with self.backend.connect() as backend:
                storage_partitions = backend.read_artifact_partitions(artifact)
        return io.read(
            type=artifact.type,
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
        if view is None:
            view = View.get_class_for(type(data))()
        storage_partition = artifact.storage.generate_partition(
            input_fingerprint=input_fingerprint, keys=keys, with_content_fingerprint=False
        )
        io.write(
            data,
            type=artifact.type,
            format=artifact.format,
            storage_partition=storage_partition,
            view=view,
        )
        storage_partition = storage_partition.with_content_fingerprint()
        # TODO: Should we only do this in bulk? We might want the backends to
        # transparently batch requests, but that's not so friendly with the transient
        # ".connect".
        with self.backend.connect() as backend:
            backend.write_artifact_partitions(artifact, (storage_partition,))
        return cast(StoragePartition, storage_partition)
