from __future__ import annotations

from types import TracebackType
from typing import Optional, Any
from hashlib import sha1

from arti.artifacts.core import Artifact
from arti.backends.core import Backend
from arti.internal.utils import TypedBox

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

    def extend(self, name: str, *, backend: Optional[Backend] = None) -> Graph:
        """ Return an extend copy of self.

            If passed, `artifacts` and `resources` will be deep merged.
        """
        with Graph(name, backend=(backend or self.backend)) as new:
            new.artifacts = deepcopy(self.artifacts)
        return new

    # should this be a @property? since we expect artifacts get added/removed dynamically,
    # perhaps it is unnecessary to get a "hash" until we need to write the Graph
    def calculate_hash(self) -> str:
        # hashes based on graph name and artifact fingerprints
        sha_hash = sha1()
        sha_hash.update((self.name or "").encode())
        for artifact in self.artifacts:
            sha_hash.update(artifact.fingerprint.encode())
        self.graph_hash = sha_hash.hexdigest()
        return self.graph_hash

    def load(self, graph_id: str) -> Graph:
        if not self.backend:
            raise AttributeError("Cannot load a Graph without setting a backend for it first.")
        graph_dict = self.backend.load_graph_dict(graph_id)

        # what to do if name defined but conflicts with name returned by backend?
        self.artifacts = Artifact.box(
            [Artifact.from_dict(artifact) for artifact in graph_dict["artifacts"]]
        )
        return self

    def write(self) -> None:
        if not self.backend:
            raise AttributeError("Cannot write graph without a backend set.")
        # write artifacts
        for artifact in self.artifacts:
            self.backend.write_artifact(artifact)
        # write graph dict that references those artifacts
        self.backend.write_graph_from_dict(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        def _get_edge_label(artifact: Artifact):
            # should there be an instance-level artifact -> edge_label map
            # which can be used to override these defaults?
            #
            # e.g. the default edge name is "google_transit.routes",
            # but the graph can override it as "transit_routes" if desired
            
            
            # punting on the following because we're not adding producers yet
            # return (
            #     f"{artifact.producer.key}.{artifact.key}"
            #     if artifact.producer is not None
            #     else f"{artifact.key}"  # or "raw.{artifact.key}" for raw sources?
            # )
            return f"{artifact.key}"

        return {
            "id": self.calculate_hash(),
            "name": self.name,
            "artifacts": [
                {"label": _get_edge_label(artifact), "id": artifact.id}
                for artifact in self.artifacts
            ],
        }
