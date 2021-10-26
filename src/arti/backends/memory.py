from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from itertools import chain

from pydantic import Field

from arti.artifacts import Artifact
from arti.backends import Backend
from arti.storage import StoragePartitions


class MemoryBackend(Backend):
    artifact_partitions: dict[Artifact, StoragePartitions] = Field(default_factory=dict, repr=False)

    @contextmanager
    def connect(self) -> Iterator[MemoryBackend]:
        yield self

    def read_artifact_partitions(self, artifact: Artifact) -> StoragePartitions:
        return self.artifact_partitions[artifact]

    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        # NOTE: Be careful about deduping, otherwise we might have dup reads.
        self.artifact_partitions[artifact] = tuple(
            set(
                chain(
                    self.artifact_partitions.get(artifact, ()),
                    (partition.with_fingerprint(keep_existing=True) for partition in partitions),
                )
            )
        )
