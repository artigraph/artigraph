from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Self

from arti.artifacts import Artifact
from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.partitions import InputFingerprints
from arti.statistics import Statistic
from arti.storage import StoragePartitionSnapshot, StoragePartitionSnapshots
from arti.thresholds import Threshold

if TYPE_CHECKING:
    from arti.graphs import Graph, GraphSnapshot

# TODO: Consider adding CRUD methods for "everything"?
#
# Likely worth making a distinction between lower level ("CRUD") methods vs higher level ("RPC" or
# "composing") methods. Executors should operate on the high level methods, but those may have
# defaults simply calling the lower level methods. If high level methods can be optimized (eg: not a
# bunch of low level calls, each own network call), Backend can override.


class BackendConnection:
    """
    BackendConnection is a wrapper around an active connection to a Backend resource.

    For example, a Backend connecting to a database might wrap up a SQLAlchemy connection in a
    BackendConnection subclass implementing the required methods.

    Graphs and GraphSnapshots are identified by their name and fingerprint, so the first write of
    an identity is kept and later writes are ignored, even if they differ in metadata excluded from
    the fingerprint (eg: `Type.description`). Only snapshot tags can be overwritten.
    """

    # Artifact partitions - independent of a specific GraphSnapshot

    @abstractmethod
    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints | None = None
    ) -> StoragePartitionSnapshots:
        """
        Read all known Partitions for this Storage spec.

        If `input_fingerprints` is provided, only partitions whose partition key and input
        fingerprint match an entry are returned, so an empty `input_fingerprints` returns none.

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

    # Partition Statistics - independent of a specific GraphSnapshot

    @abstractmethod
    def read_partition_statistics(
        self, artifact: Artifact, partitions: StoragePartitionSnapshots
    ) -> dict[StoragePartitionSnapshot, dict[Statistic, Any]]:
        """
        Read the recorded values of the Artifact's `statistics` for each partition.

        Every partition is returned, mapped to the values recorded for it. Statistics without a
        recorded value are omitted, so a partition may map to only some (or none) of them.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_partition_statistics(
        self,
        artifact: Artifact,
        partition: StoragePartitionSnapshot,
        values: Mapping[Statistic, Any],
    ) -> None:
        """
        Record Statistic values for a partition, which must already be written for the Artifact.

        Statistics are facts about a partition's contents, so the first recorded value of each
        Statistic is kept and later writes are ignored.
        """
        raise NotImplementedError()

    # Threshold overrides - independent of a specific GraphSnapshot

    @abstractmethod
    def read_threshold_overrides(
        self, artifact: Artifact, partitions: StoragePartitionSnapshots
    ) -> dict[StoragePartitionSnapshot, dict[Threshold, str]]:
        """
        Read the overrides of the Artifact's `thresholds` for each partition, mapped to their reason.

        Every partition is returned, mapped to its (possibly no) overridden Thresholds.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_threshold_override(
        self,
        artifact: Artifact,
        partition: StoragePartitionSnapshot,
        threshold: Threshold,
        reason: str,
    ) -> None:
        """
        Record a (human) decision to accept the partition even if it fails the `threshold`.

        The override is specific to the partition's contents and the exact Threshold, so it no
        longer applies if either changes. The first recorded override (and reason) is kept.
        """
        raise NotImplementedError()

    def _check_threshold_override(
        self,
        artifact: Artifact,
        partition: StoragePartitionSnapshot,
        threshold: Threshold,
        reason: str,
    ) -> None:
        if threshold not in artifact.thresholds:
            raise ValueError(f"{threshold!r} is not one of the Artifact's Thresholds")
        if not reason.strip():
            raise ValueError("A Threshold override requires a reason")

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
    def connect(self) -> Generator[Self]:
        """
        Return self.

        This makes it easier to work with an optional connection, eg:
            with (connection or backend).connect() as conn:
                ...
        """
        yield self


class Backend[Connection: BackendConnection](Model):
    """
    Backend represents a storage for internal Artigraph metadata.

    Backend storage is an addressable location (local path, database connection, etc) that
    tracks metadata for a collection of Graphs over time, including:
    - the Artifact(s)->Producer->Artifact(s) dependency graph
    - Artifact Annotations, Partitions, Statistic values, Threshold overrides, and other metadata
    - Artifact and Producer Fingerprints
    - etc
    """

    @contextmanager
    @abstractmethod
    def connect(self) -> Generator[Connection]:
        raise NotImplementedError()

    def close(self) -> None:
        """Release any resources held by the backend."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
