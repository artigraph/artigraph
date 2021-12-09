__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from abc import abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TypeVar

from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.storage import AnyStorage, InputFingerprints, StoragePartitions

# TODO: Consider adding CRUD methods for "everything"?
#
# Likely worth making a distinction between lower level ("CRUD") methods vs higher level ("RPC" or
# "composing") methods. Executors should operate on the high level methods, but those may have
# defaults simply calling the lower level methods. If high level methods can be optimized (eg: not a
# bunch of low level calls, each own network call), Backend can override.


_Backend = TypeVar("_Backend", bound="Backend")


class Backend(Model):
    """Backend represents a storage for internal Artigraph metadata.

    Backend storage is an addressable location (local path, database connection, etc) that
    tracks metadata for a collection of Graphs over time, including:
    - the Artifact(s)->Producer->Artifact(s) dependency graph
    - Artifact Annotations, Statistics, Partitions, and other metadata
    - Artifact and Producer Fingerprints
    - etc
    """

    @contextmanager
    def connect(self: _Backend) -> Iterator[_Backend]:
        raise NotImplementedError()

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

    @abstractmethod
    def read_graph_partitions(
        self, graph_name: str, graph_snapshot_id: Fingerprint, artifact_key: str
    ) -> StoragePartitions:
        """Read the known Partitions for the named Artifact in a specific Graph snapshot."""
        raise NotImplementedError()

    @abstractmethod
    def read_storage_partitions(
        self, storage: AnyStorage, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        """Read all known Partitions for this Storage spec.

        If `input_fingerprints` is provided, the returned partitions will be filtered accordingly.

        NOTE: The returned partitions may not be associated with any particular Graph, unless
        `input_fingerprints` is provided matching those for a Graph snapshot.
        """
        raise NotImplementedError()

    @abstractmethod
    def link_graph_partitions(
        self,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
        partitions: StoragePartitions,
    ) -> None:
        """Link the Partitions to the named Artifact in a specific Graph snapshot."""
        raise NotImplementedError()

    @abstractmethod
    def write_storage_partitions(self, storage: AnyStorage, partitions: StoragePartitions) -> None:
        """Add more partitions for a Storage spec."""
        raise NotImplementedError()

    def write_storage_partitions_and_link_to_graph(
        self,
        storage: AnyStorage,
        partitions: StoragePartitions,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
    ) -> None:
        self.write_storage_partitions(storage, partitions)
        self.link_graph_partitions(graph_name, graph_snapshot_id, artifact_key, partitions)
