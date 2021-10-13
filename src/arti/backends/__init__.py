from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol

from arti.artifacts import Artifact
from arti.internal.models import Model
from arti.storage import StoragePartitions


class BackendProtocol(Protocol):
    def read_artifact_partitions(self, artifact: Artifact) -> StoragePartitions:
        raise NotImplementedError()

    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        raise NotImplementedError()


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
    def connect(self) -> Iterator[BackendProtocol]:
        raise NotImplementedError()
