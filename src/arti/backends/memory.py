from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import chain

from pydantic import Field

from arti.backends import Backend
from arti.fingerprints import Fingerprint
from arti.storage import StoragePartitions


class MemoryBackend(Backend):
    graph_partitions: dict[Fingerprint, dict[str, StoragePartitions]] = Field(
        default_factory=lambda: defaultdict(lambda: defaultdict(StoragePartitions)), repr=False
    )

    @contextmanager
    def connect(self) -> Iterator[MemoryBackend]:
        yield self

    def read_graph_partitions(self, graph_id: Fingerprint, name: str) -> StoragePartitions:
        return self.graph_partitions[graph_id][name]

    def write_graph_partitions(
        self, graph_id: Fingerprint, name: str, partitions: StoragePartitions
    ) -> None:
        # NOTE: Be careful about deduping, otherwise we might have dup reads.
        self.graph_partitions[graph_id][name] = StoragePartitions(
            set(
                chain(
                    self.graph_partitions[graph_id][name],
                    (
                        partition.with_content_fingerprint(keep_existing=True)
                        for partition in partitions
                    ),
                )
            )
        )
