from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager

from pydantic import PrivateAttr

from arti.backends import Backend
from arti.fingerprints import Fingerprint
from arti.storage import AnyStorage, InputFingerprints, StoragePartition, StoragePartitions


def ensure_fingerprinted(partitions: StoragePartitions) -> Iterator[StoragePartition]:
    for partition in partitions:
        yield partition.with_content_fingerprint(keep_existing=True)


_GraphSnapshotPartitions = dict[str, dict[Fingerprint, dict[str, set[StoragePartition]]]]
_StoragePartitions = dict[AnyStorage, set[StoragePartition]]


class MemoryBackend(Backend):
    # Share the backing dicts across copies. This aligns with other Backends that have true
    # persistence (a copy would still connect to the same underlying database). This is required
    # because Graph.snapshot copies the Graph and thus the `.backend`, but we want the original
    # Graph (pre-snapshot) to still be able to see the newly built partitions.
    _share_private_attributes_across_copies_ = True

    # `_graph_snapshot_partitions` tracks all the partitions for a *specific* "run" of a graph.
    # `_storage_partitions` tracks all partitions, across all graphs. This separation is important
    # to allow for Literals to be used even after a snapshot_id change.
    _graph_snapshot_partitions: _GraphSnapshotPartitions = PrivateAttr(
        default_factory=lambda: defaultdict(
            lambda: defaultdict(lambda: defaultdict(set[StoragePartition]))
        )
    )
    _graph_tags: dict[str, dict[str, Fingerprint]] = PrivateAttr(
        default_factory=lambda: defaultdict(dict)
    )
    _storage_partitions: _StoragePartitions = PrivateAttr(
        default_factory=lambda: defaultdict(set[StoragePartition])
    )

    @contextmanager
    def connect(self) -> Iterator[MemoryBackend]:
        yield self

    def read_graph_tag(self, graph_name: str, tag: str) -> Fingerprint:
        if tag not in self._graph_tags[graph_name]:
            raise ValueError(f"No known `{tag}` tag for Graph `{graph_name}`")
        return self._graph_tags[graph_name][tag]

    def write_graph_tag(
        self, graph_name: str, graph_snapshot_id: Fingerprint, tag: str, overwrite: bool = False
    ) -> None:
        """Read the known Partitions for the named Artifact in a specific Graph snapshot."""
        if (existing := self._graph_tags[graph_name].get(tag)) is not None and not overwrite:
            raise ValueError(f"Existing `{tag}` tag for Graph `{graph_name}` points to {existing}")
        self._graph_tags[graph_name][tag] = graph_snapshot_id

    def read_graph_partitions(
        self, graph_name: str, graph_snapshot_id: Fingerprint, artifact_key: str
    ) -> StoragePartitions:
        return tuple(self._graph_snapshot_partitions[graph_name][graph_snapshot_id][artifact_key])

    def read_storage_partitions(
        self, storage: AnyStorage, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        partitions = self._storage_partitions[storage]
        if input_fingerprints:
            partitions = {
                partition
                for partition in partitions
                if input_fingerprints.get(partition.keys) == partition.input_fingerprint
            }
        return tuple(partitions)

    def link_graph_partitions(
        self,
        graph_name: str,
        graph_snapshot_id: Fingerprint,
        artifact_key: str,
        partitions: StoragePartitions,
    ) -> None:
        self._graph_snapshot_partitions[graph_name][graph_snapshot_id][artifact_key].update(
            ensure_fingerprinted(partitions)
        )

    def write_storage_partitions(self, storage: AnyStorage, partitions: StoragePartitions) -> None:
        self._storage_partitions[storage].update(ensure_fingerprinted(partitions))
