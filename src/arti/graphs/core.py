from __future__ import annotations

from types import TracebackType
from typing import Optional

from arti.artifacts.core import Artifact
from arti.backends.core import Backend
from arti.internal.utils import TypedBox

ArtifactBox = TypedBox[Artifact]


class Graph:
    """ Graph stores a web of Artifacts connected by Producers.
    """

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
