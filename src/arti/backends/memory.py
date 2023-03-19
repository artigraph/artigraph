from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from functools import partial

from pydantic import PrivateAttr

from arti.artifacts import Artifact
from arti.backends import Backend, Connection
from arti.fingerprints import Fingerprint
from arti.internal.utils import NoCopyMixin
from arti.partitions import InputFingerprints
from arti.storage import Storage, StoragePartition, StoragePartitions


def _ensure_fingerprinted(partitions: StoragePartitions) -> Iterator[StoragePartition]:
    for partition in partitions:
        yield partition.with_content_fingerprint(keep_existing=True)


_GraphSnapshotPartitions = dict[str, dict[Fingerprint, dict[str, set[StoragePartition]]]]
_StoragePartitions = dict[Storage[StoragePartition], set[StoragePartition]]


class _NoCopyContainer(NoCopyMixin):
    """Container for MemoryBackend data that bypasses (deep)copying.

    The MemoryBackend is *intended* to be stateful, like a connection to an external database in
    other backends. However, we usually prefer immutable data structures and Pydantic models, which
    (deep)copy often. If we were to (deep)copy these data structures, then we wouldn't be able to
    track metadata between steps. Instead, this container holds the state and skips (deep)copying.

    We may also add threading locks around access (with some slight usage changes).
    """

    def __init__(self) -> None:
        # `container.graph_snapshot_partitions` tracks all the partitions for a *specific* "run" of a graph.
        # `container.storage_partitions` tracks all partitions, across all graphs. This separation is important
        # to allow for Literals to be used even after a snapshot_id change.
        self.graph_snapshot_partitions: _GraphSnapshotPartitions = defaultdict(
            partial(defaultdict, partial(defaultdict, set[StoragePartition]))  # type: ignore[arg-type]
        )
        self.graph_tags: dict[str, dict[str, Fingerprint]] = defaultdict(dict)
        self.storage_partitions: _StoragePartitions = defaultdict(set[StoragePartition])


class MemoryConnection(Connection):
    def __init__(self, container: _NoCopyContainer) -> None:
        self.container = container

    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        # The MemoryBackend is (obviously) not persistent, so there may be external data we don't
        # know about. If we haven't seen this storage before, we'll attempt to "warm" the cache.
        if artifact.storage not in self.container.storage_partitions:
            self.write_artifact_partitions(
                artifact, artifact.storage.discover_partitions(input_fingerprints)
            )
        partitions = self.container.storage_partitions[artifact.storage]
        if input_fingerprints:
            partitions = {
                partition
                for partition in partitions
                if input_fingerprints.get(partition.keys) == partition.input_fingerprint
            }
        return tuple(partitions)

    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        self.container.storage_partitions[artifact.storage].update(
            _ensure_fingerprinted(partitions)
        )

    def read_graph_partitions(
        self, graph_name: str, graph_snapshot_id: Fingerprint, artifact_key: str, artifact: Artifact
    ) -> StoragePartitions:
        return tuple(
            self.container.graph_snapshot_partitions[graph_name][graph_snapshot_id][artifact_key]
        )

    def write_graph_partitions(
        self,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        self.container.graph_snapshot_partitions[graph_name][graph_snapshot_id][
            artifact_key
        ].update(_ensure_fingerprinted(partitions))

    def read_graph_tag(self, graph_name: str, tag: str) -> Fingerprint:
        if tag not in self.container.graph_tags[graph_name]:
            raise ValueError(f"No known `{tag}` tag for Graph `{graph_name}`")
        return self.container.graph_tags[graph_name][tag]

    def write_graph_tag(
        self, graph_name: str, graph_snapshot_id: Fingerprint, tag: str, overwrite: bool = False
    ) -> None:
        """Read the known Partitions for the named Artifact in a specific Graph snapshot."""
        if (
            existing := self.container.graph_tags[graph_name].get(tag)
        ) is not None and not overwrite:
            raise ValueError(f"Existing `{tag}` tag for Graph `{graph_name}` points to {existing}")
        self.container.graph_tags[graph_name][tag] = graph_snapshot_id


class MemoryBackend(Backend[MemoryConnection]):
    _container: _NoCopyContainer = PrivateAttr(default_factory=_NoCopyContainer)

    @contextmanager
    def connect(self) -> Iterator[MemoryConnection]:
        yield MemoryConnection(self._container)
