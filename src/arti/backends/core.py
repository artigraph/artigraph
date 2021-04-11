from __future__ import annotations

from enum import Enum
from typing import Any

from arti.artifacts.core import Artifact


class BackendType(Enum):
    Memory = 1
    DGraph = 2


class Backend:
    """Backend represents a storage for internal Artigraph metadata.

    Backend storage is an addressable location (local path, database connection, etc) that
    tracks metadata for a collection of Graphs over time, including:
    - the Artifact(s)->Producer->Artifact(s) dependency graph
    - Artifact Annotations, Statistics, Partitions, and other metadata
    - Artifact and Producer Fingerprints
    - etc
    """

    def load_artifact(self, artifact_id: str) -> Artifact:
        raise NotImplementedError

    def write_artifact(self, artifact: Artifact) -> None:
        raise NotImplementedError

    # def get_producer(self, producer_id: str) -> Producer:
    #     raise NotImplementedError

    # def write_producer(self, producer: Producer) -> None:
    #     raise NotImplementedError

    # should graph have a "fingerprint" which is a hash of all its artifacts' fingerprints?
    def load_graph_dict(self, graph_id: str) -> dict[str, Any]:
        raise NotImplementedError

    def write_graph_from_dict(self, graph_dict: dict[str, Any]) -> None:
        raise NotImplementedError


class MemoryBackend(Backend):
    def __init__(self) -> None:
        # each store has serialized objects indexted by their id
        self.artifact_store: dict[str, dict[str, Any]] = {}
        self.graph_store: dict[str, dict[str, Any]] = {}
        # TODO: statistic_store?
        # figure out how to handle writing artifacts with statistics:
        # should there be a separate statistic_store that gets written to
        # alongside the artifact?

    def load_artifact(self, artifact_id: str) -> Artifact:
        if artifact_id not in self.artifact_store:
            raise ValueError(f"No artifact with id {artifact_id} in the artifact store.")
        artifact_dict = self.artifact_store[artifact_id]
        try:
            return Artifact.from_dict(artifact_dict)
        except Exception as e:
            raise ValueError(f"Error instantiating an Artifact from {artifact_dict}: {e}")

    def write_artifact(self, artifact: Artifact) -> None:
        # should we overwrite (in case there IS a fingerprint clash)? How will we know if there is a clash?
        artifact_id = artifact.id
        if artifact_id not in self.artifact_store:
            self.artifact_store[artifact_id] = artifact.to_dict()

    def load_graph_dict(self, graph_id: str) -> dict[str, Any]:
        if graph_id not in self.graph_store:
            raise ValueError(f"No graph with id {graph_id} in the graph store.")
        return self.graph_store[graph_id]

    def write_graph_from_dict(self, graph_dict: dict[str, Any]) -> None:
        graph_id = graph_dict.get("id")
        if not graph_id:
            raise ValueError(f"Serialized graph is missing a required 'id' key: {graph_dict}")
        if graph_id not in self.graph_store:
            self.graph_store[graph_id] = graph_dict
