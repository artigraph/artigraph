from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager

from pydantic import Field

from arti.backends import Backend
from arti.fingerprints import Fingerprint
from arti.storage import StoragePartition, StoragePartitions


def ensure_fingerprinted(partitions: StoragePartitions) -> Iterator[StoragePartition]:
    for partition in partitions:
        yield partition.with_content_fingerprint(keep_existing=True)


class MemoryBackend(Backend):
    graph_partitions: dict[Fingerprint, dict[str, set[StoragePartition]]] = Field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set[StoragePartition])), repr=False
    )

    @contextmanager
    def connect(self) -> Iterator[MemoryBackend]:
        yield self

    def read_graph_partitions(self, graph_id: Fingerprint, name: str) -> StoragePartitions:
        return tuple(self.graph_partitions[graph_id][name])

    def write_graph_partitions(
        self, graph_id: Fingerprint, name: str, partitions: StoragePartitions
    ) -> None:
        # NOTE: Be careful about deduping, otherwise we might have dup reads.
        self.graph_partitions[graph_id][name].update(ensure_fingerprinted(partitions))
