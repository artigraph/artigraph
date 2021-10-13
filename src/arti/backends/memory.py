from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

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
        self.artifact_partitions[artifact] = tuple(
            set(self.artifact_partitions.get(artifact, ()) + partitions)
        )
