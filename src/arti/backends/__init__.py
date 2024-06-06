from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Self

from arti.artifacts import Artifact
from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.partitions import InputFingerprints
from arti.storage import StoragePartitionSnapshots

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
    ) -> StoragePartitionSnapshots:
        """Read all known Partitions for this Storage spec.

        If `input_fingerprints` is provided, the returned partitions will be filtered accordingly.

        NOTE: The returned partitions may not be associated with any particular Graph, unless
        `input_fingerprints` is provided matching those for a GraphSnapshot.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_artifact_partitions(
        self, artifact: Artifact, partitions: StoragePartitionSnapshots
    ) -> None:
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
    ) -> StoragePartitionSnapshots:
        """Read the known Partitions for the named Artifact in a specific GraphSnapshot."""
        raise NotImplementedError()

    @abstractmethod
    def write_snapshot_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitionSnapshots,
    ) -> None:
        """Link the Partitions to the named Artifact in a specific GraphSnapshot."""
        raise NotImplementedError()

    # Helpers

    def write_artifact_and_graph_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitionSnapshots,
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


class Backend[Connection: BackendConnection](Model):
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
    def connect(self) -> Iterator[Connection]:
        raise NotImplementedError()
