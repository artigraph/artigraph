from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial

from pydantic import PrivateAttr

from arti import (
    Artifact,
    Backend,
    BackendConnection,
    Fingerprint,
    Graph,
    GraphSnapshot,
    InputFingerprints,
    Storage,
    StoragePartitionSnapshot,
    StoragePartitionSnapshots,
)
from arti.internal.utils import NoCopyMixin

_Graphs = dict[str, dict[Fingerprint, Graph]]
_GraphSnapshots = dict[str, dict[Fingerprint, GraphSnapshot]]
_GraphSnapshotPartitions = dict[
    GraphSnapshot, dict[str, set[StoragePartitionSnapshot]]
]  # ...[snapshot][artifact_key]
_SnapshotTags = dict[str, dict[str, GraphSnapshot]]  # ...[name][tag]
_StoragePartitions = dict[Storage, set[StoragePartitionSnapshot]]


# TODO: Determine if the "no copy" behavior is still necessary after the Pydantic v2 upgrade.
class _NoCopyContainer(NoCopyMixin):
    """Container for MemoryBackend data that bypasses (deep)copying.

    The MemoryBackend is *intended* to be stateful, like a connection to an external database in
    other backends. However, we usually prefer immutable data structures and Pydantic models, which
    (deep)copy often. If we were to (deep)copy these data structures, then we wouldn't be able to
    track metadata between steps. Instead, this container holds the state and skips (deep)copying.

    We may also add threading locks around access (with some slight usage changes).
    """

    def __init__(self) -> None:
        # NOTE: lambdas are not pickleable, so use partial for any nested defaultdicts.
        self.graphs: _Graphs = defaultdict(dict)
        self.snapshots: _GraphSnapshots = defaultdict(dict)
        # `container.snapshot_partitions` tracks all the partitions for a *specific* GraphSnapshot.
        # `container.storage_partitions` tracks all partitions, across all snapshots. This
        # separation is important to allow for Literals to be used even after a snapshot change.
        self.snapshot_partitions: _GraphSnapshotPartitions = defaultdict(
            partial(defaultdict, set[StoragePartitionSnapshot])
        )
        self.snapshot_tags: _SnapshotTags = defaultdict(dict)
        self.storage_partitions: _StoragePartitions = defaultdict(set[StoragePartitionSnapshot])

    def __eq__(self, other: object, /) -> bool:
        return isinstance(other, _NoCopyContainer) and vars(self) == vars(other)


class MemoryConnection(BackendConnection):
    def __init__(self, container: _NoCopyContainer) -> None:
        self.container = container

    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitionSnapshots:
        # The MemoryBackend is (obviously) not persistent, so there may be external data we don't
        # know about. If we haven't seen this storage before, we'll attempt to "warm" the cache.
        if artifact.storage not in self.container.storage_partitions:
            self.write_artifact_partitions(
                artifact, artifact.storage.discover_partitions(input_fingerprints)
            )
        partition_snapshots = self.container.storage_partitions[artifact.storage]
        if input_fingerprints:
            partition_snapshots = {
                snapshot
                for snapshot in partition_snapshots
                if input_fingerprints.get(snapshot.partition_key) == snapshot.input_fingerprint
            }
        return tuple(partition_snapshots)

    def write_artifact_partitions(
        self, artifact: Artifact, partitions: StoragePartitionSnapshots
    ) -> None:
        self.container.storage_partitions[artifact.storage].update(partitions)

    def read_graph(self, name: str, fingerprint: Fingerprint) -> Graph:
        return self.container.graphs[name][fingerprint]

    def write_graph(self, graph: Graph) -> None:
        self.container.graphs[graph.name][graph.fingerprint] = graph

    def read_snapshot(self, name: str, fingerprint: Fingerprint) -> GraphSnapshot:
        return self.container.snapshots[name][fingerprint]

    def write_snapshot(self, snapshot: GraphSnapshot) -> None:
        self.container.snapshots[snapshot.name][snapshot.fingerprint] = snapshot

    def read_snapshot_tag(self, name: str, tag: str) -> GraphSnapshot:
        if tag not in self.container.snapshot_tags[name]:
            raise ValueError(f"No known `{tag}` tag for GraphSnapshot `{name}`")
        return self.container.snapshot_tags[name][tag]

    def write_snapshot_tag(
        self, snapshot: GraphSnapshot, tag: str, overwrite: bool = False
    ) -> None:
        """Read the known Partitions for the named Artifact in a specific GraphSnapshot."""
        if (
            existing := self.container.snapshot_tags[snapshot.name].get(tag)
        ) is not None and not overwrite:
            raise ValueError(
                f"Existing `{tag}` tag for Graph `{snapshot.name}` points to {existing}"
            )
        self.container.snapshot_tags[snapshot.name][tag] = snapshot

    def read_snapshot_partitions(
        self, snapshot: GraphSnapshot, artifact_key: str, artifact: Artifact
    ) -> StoragePartitionSnapshots:
        return tuple(self.container.snapshot_partitions[snapshot][artifact_key])

    def write_snapshot_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitionSnapshots,
    ) -> None:
        self.container.snapshot_partitions[snapshot][artifact_key].update(partitions)


class MemoryBackend(Backend[MemoryConnection]):
    _container: _NoCopyContainer = PrivateAttr(default_factory=_NoCopyContainer)

    @contextmanager
    def connect(self) -> Iterator[MemoryConnection]:
        yield MemoryConnection(self._container)
