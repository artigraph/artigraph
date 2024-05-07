from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic.fields import ModelField

from arti.artifacts import Artifact
from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.internal.type_hints import Self
from arti.partitions import InputFingerprints
from arti.storage import StoragePartitions

if TYPE_CHECKING:
    from arti.graphs import Graph, GraphSnapshot

# TODO: Consider adding CRUD methods for "everything"?
#
# Likely worth making a distinction between lower level ("CRUD") methods vs higher level ("RPC" or
# "composing") methods. Executors should operate on the high level methods, but those may have
# defaults simply calling the lower level methods. If high level methods can be optimized (eg: not a
# bunch of low level calls, each own network call), Backend can override.


class BackendConnection:
    """BackendConnection is a wrapper around an active connection to a Backend resource.

    For example, a Backend connecting to a database might wrap up a SQLAlchemy connection in a
    BackendConnection subclass implementing the required methods.
    """

    # Artifact partitions - independent of a specific GraphSnapshot

    @abstractmethod
    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        """Read all known Partitions for this Storage spec.

        If `input_fingerprints` is provided, the returned partitions will be filtered accordingly.

        NOTE: The returned partitions may not be associated with any particular Graph, unless
        `input_fingerprints` is provided matching those for a GraphSnapshot.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        """Add more partitions for a Storage spec."""
        raise NotImplementedError()

    # Graph

    @abstractmethod
    def read_graph(self, name: str, fingerprint: Fingerprint) -> Graph:
        """Fetch an instance of the named Graph."""
        raise NotImplementedError()

    @abstractmethod
    def write_graph(self, graph: Graph) -> None:
        """Write the Graph and all linked Artifacts and Producers to the database."""
        raise NotImplementedError()

    # GraphSnapshot

    @abstractmethod
    def read_snapshot(self, name: str, fingerprint: Fingerprint) -> GraphSnapshot:
        """Fetch an instance of the named GraphSnapshot."""
        raise NotImplementedError()

    @abstractmethod
    def write_snapshot(self, snapshot: GraphSnapshot) -> None:
        """Write the GraphSnapshot to the database."""
        raise NotImplementedError()

    @abstractmethod
    def read_snapshot_tag(self, name: str, tag: str) -> GraphSnapshot:
        """Fetch the GraphSnapshot for the named tag."""
        raise NotImplementedError()

    @abstractmethod
    def write_snapshot_tag(
        self, snapshot: GraphSnapshot, tag: str, overwrite: bool = False
    ) -> None:
        """Stamp a GraphSnapshot with an arbitrary tag."""
        raise NotImplementedError()

    @abstractmethod
    def read_snapshot_partitions(
        self, snapshot: GraphSnapshot, artifact_key: str, artifact: Artifact
    ) -> StoragePartitions:
        """Read the known Partitions for the named Artifact in a specific GraphSnapshot."""
        raise NotImplementedError()

    @abstractmethod
    def write_snapshot_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        """Link the Partitions to the named Artifact in a specific GraphSnapshot."""
        raise NotImplementedError()

    # Helpers

    def write_artifact_and_graph_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        self.write_artifact_partitions(artifact, partitions)
        self.write_snapshot_partitions(snapshot, artifact_key, artifact, partitions)

    @contextmanager
    def connect(self) -> Iterator[Self]:
        """Return self

        This makes it easier to work with an optional connection, eg:
            with (connection or backend).connect() as conn:
                ...
        """
        yield self

    @classmethod
    def __get_validators__(cls) -> list[Callable[[Any, ModelField], Any]]:
        """Return an empty list of "validators".

        Allows using a BackendConnection (which is not a model) as a field in other models without
        setting `arbitrary_types_allowed` (which applies broadly). [1].

        1: https://docs.pydantic.dev/usage/types/#generic-classes-as-types
        """
        return []


BackendConnectionVar = TypeVar("BackendConnectionVar", bound=BackendConnection, covariant=True)


class Backend(Model, Generic[BackendConnectionVar]):
    """Backend represents a storage for internal Artigraph metadata.

    Backend storage is an addressable location (local path, database connection, etc) that
    tracks metadata for a collection of Graphs over time, including:
    - the Artifact(s)->Producer->Artifact(s) dependency graph
    - Artifact Annotations, Statistics, Partitions, and other metadata
    - Artifact and Producer Fingerprints
    - etc
    """

    @contextmanager
    @abstractmethod
    def connect(self) -> Iterator[BackendConnectionVar]:
        raise NotImplementedError()
