from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager

from pydantic import Field

from arti.backends import Backend
from arti.fingerprints import Fingerprint
from arti.storage import AnyStorage, InputFingerprints, StoragePartition, StoragePartitions


def ensure_fingerprinted(partitions: StoragePartitions) -> Iterator[StoragePartition]:
    for partition in partitions:
        yield partition.with_content_fingerprint(keep_existing=True)


class MemoryBackend(Backend):
    # `graph_snapshot_partitions` tracks all the partitions for a *specific* "run" of a graph.
    # `storage_partitions` tracks all partitions, across all graphs. This separation is important to
    # allow for Literals to be used even after a graph_id change.
    graph_snapshot_partitions: dict[Fingerprint, dict[str, set[StoragePartition]]] = Field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set[StoragePartition])), repr=False
    )
    storage_partitions: dict[AnyStorage, set[StoragePartition]] = Field(
        default_factory=lambda: defaultdict(set[StoragePartition]), repr=False
    )

    @contextmanager
    def connect(self) -> Iterator[MemoryBackend]:
        yield self

    def read_graph_partitions(self, graph_id: Fingerprint, name: str) -> StoragePartitions:
        return tuple(self.graph_snapshot_partitions[graph_id][name])

    def read_storage_partitions(
        self, storage: AnyStorage, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        partitions = self.storage_partitions[storage]
        if input_fingerprints:
            partitions = {
                partition
                for partition in partitions
                if input_fingerprints.get(partition.keys) == partition.input_fingerprint
            }
        return tuple(partitions)

    def link_graph_partitions(
        self, graph_id: Fingerprint, name: str, partitions: StoragePartitions
    ) -> None:
        self.graph_snapshot_partitions[graph_id][name].update(ensure_fingerprinted(partitions))

    def write_storage_partitions(self, storage: AnyStorage, partitions: StoragePartitions) -> None:
        self.storage_partitions[storage].update(ensure_fingerprinted(partitions))
