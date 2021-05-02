from __future__ import annotations

from types import TracebackType
from typing import Any, Optional

from arti.artifacts.core import Artifact
from arti.backends.core import Backend
from arti.fingerprints.core import Fingerprint
from arti.internal.utils import TypedBox, int64

ArtifactBox = TypedBox[Artifact]


# TODO: When building out run logic, resolve and statically set all available fingerprints.
# Specifically, we should pin the Producer.fingerprint, which may by dynamic (eg: version is a
# Timestamp). Unbuilt Artifact (partitions) won't be fully resolved yet.
class Graph:
    """Graph stores a web of Artifacts connected by Producers."""

    name: str

    artifacts: ArtifactBox

    def __init__(self, name: str, *, backend: Optional[Backend] = None) -> None:
        self.name = name
        self.backend = backend or Backend()  # TODO: Fill in some default in-memory backend
        self.artifacts = ArtifactBox()
        self._sealed: bool = False
        # Seal the class and convert the Boxes
        self._toggle(sealed=True)

    def __enter__(self) -> Graph:
        self._toggle(sealed=False)
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> None:
        self._toggle(sealed=True)

    def _toggle(self, sealed: bool) -> None:
        self._sealed = sealed
        box_kwargs = {
            "default_box": not sealed,
            "frozen_box": sealed,  # Unfreeze the box while building the graph
        }
        self.artifacts = ArtifactBox(self.artifacts, **box_kwargs)

    @property
    def id(self) -> int64:
        # hash based on graph name and artifact fingerprints
        f = Fingerprint.from_string(self.name)
        for artifact in self.artifacts.to_dict().values():
            f = f.combine(artifact.fingerprint)
        return f.key or int64(0)

    def load(self, graph_id: int64) -> Graph:
        if not self.backend:
            raise AttributeError("Cannot load a Graph without setting a backend for it first.")
        graph_dict = self.backend.load_graph_dict(graph_id)

        # what to do if a graph name is defined but conflicts with name returned by backend?
        if not self.name:
            self.name = graph_dict["name"]
        artifacts = [self.backend.load_artifact(id) for id in graph_dict["artifact_ids"]]
        self.artifacts = ArtifactBox({a.key: a for a in artifacts})
        return self

    def write(self) -> None:
        if not self.backend:
            raise AttributeError("Cannot write graph without a backend set.")
        # write artifacts
        for artifact in self.artifacts.to_dict().values():
            self.backend.write_artifact(artifact)
        # write graph dict that references those artifacts
        self.backend.write_graph_from_dict(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "artifact_ids": [artifact.id for artifact in self.artifacts.to_dict().values()],
        }
