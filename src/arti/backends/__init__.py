from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Callable, Generic, TypeVar

from pydantic.fields import ModelField

from arti.artifacts import Artifact
from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.partitions import InputFingerprints
from arti.storage import StoragePartitions

# TODO: Consider adding CRUD methods for "everything"?
#
# Likely worth making a distinction between lower level ("CRUD") methods vs higher level ("RPC" or
# "composing") methods. Executors should operate on the high level methods, but those may have
# defaults simply calling the lower level methods. If high level methods can be optimized (eg: not a
# bunch of low level calls, each own network call), Backend can override.


class Connection:
    """Connection is a wrapper around an active connection to a Backend resource.

    For example, a Backend connecting to a database might wrap up a SQLAlchemy connection in a
    Connection subclass implementing the required methods.
    """

    # Artifact partitions - independent of a specific GraphSnapshot

    @abstractmethod
    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        """Read all known Partitions for this Storage spec.

        If `input_fingerprints` is provided, the returned partitions will be filtered accordingly.

        NOTE: The returned partitions may not be associated with any particular Graph, unless
        `input_fingerprints` is provided matching those for a Graph snapshot.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        """Add more partitions for a Storage spec."""
        raise NotImplementedError()

    # Artifact partitions for a specific GraphSnapshot

    @abstractmethod
    def read_graph_partitions(
        self, graph_name: str, graph_snapshot_id: Fingerprint, artifact_key: str, artifact: Artifact
    ) -> StoragePartitions:
        """Read the known Partitions for the named Artifact in a specific Graph snapshot."""
        raise NotImplementedError()

    @abstractmethod
    def write_graph_partitions(
        self,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        """Link the Partitions to the named Artifact in a specific Graph snapshot."""
        raise NotImplementedError()

    def write_artifact_and_graph_partitions(
        self,
        artifact: Artifact,
        partitions: StoragePartitions,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
    ) -> None:
        self.write_artifact_partitions(artifact, partitions)
        self.write_graph_partitions(
            graph_name, graph_snapshot_id, artifact_key, artifact, partitions
        )

    # GraphSnapshot Tagging

    @abstractmethod
    def read_graph_tag(self, graph_name: str, tag: str) -> Fingerprint:
        """Fetch the Snapshot ID for the named tag."""
        raise NotImplementedError()

    @abstractmethod
    def write_graph_tag(
        self, graph_name: str, graph_snapshot_id: Fingerprint, tag: str, overwrite: bool = False
    ) -> None:
        """Tag a Graph Snapshot ID with an arbitrary name."""
        raise NotImplementedError()

    @classmethod
    def __get_validators__(cls) -> list[Callable[[Any, ModelField], Any]]:
        """Return an empty list of "validators".

        Allows using a Connection (which is not a model) as a field in other models without setting
        `arbitrary_types_allowed` (which applies broadly). [1].

        1: https://docs.pydantic.dev/usage/types/#generic-classes-as-types
        """
        return []


ConnectionVar = TypeVar("ConnectionVar", bound=Connection, covariant=True)


class Backend(Model, Generic[ConnectionVar]):
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
    def connect(self) -> Iterator[ConnectionVar]:
        raise NotImplementedError()
