__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol

from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.storage import StoragePartitions


# TODO: Consider adding CRUD methods for "everything"?
#
# Likely worth making a distinction between lower level ("CRUD") methods vs higher level ("RPC" or
# "composing") methods. Executors should operate on the high level methods, but those may have
# defaults simply calling the lower level methods. If high level methods can be optimized (eg: not a
# bunch of low level calls, each own network call), Backend can override.
class BackendProtocol(Protocol):
    def read_graph_partitions(self, graph_id: Fingerprint, name: str) -> StoragePartitions:
        raise NotImplementedError()

    def write_graph_partitions(
        self, graph_id: Fingerprint, name: str, partitions: StoragePartitions
    ) -> None:
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
