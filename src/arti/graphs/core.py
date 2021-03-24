from __future__ import annotations

from copy import deepcopy
from types import TracebackType
from typing import Optional

from arti.artifacts.core import Artifact
from arti.backends.core import Backend
from arti.internal.pointers import PointerBox


class Graph:
    name: str

    # `Artifact` instances are pointers (arti.internal.pointer.Pointer), which allows us to easily
    # update all references Producers and other objects have to them when their path in the Graph is
    # updated (via PointerBox).
    artifacts: PointerBox[Artifact]

    # Let's make Graph own the Backend, not the other way around. The Backend can be used
    # multiple/many times, but we should be able to inspect a Graph to get fully addressable
    # references. Then again, does that mean a Backend *can't* use Artifacts to reference things? Or
    # just that those Artifacts must exist on a separate Graph or w/o one? Hmm, why do I want this
    # again?

    def __init__(self, name: str, *, backend: Optional[Backend] = None) -> None:
        self.name = name
        self.backend = backend or Backend()  # TODO: Fill in some default in-memory backend
        self.artifacts = Artifact.box()
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
        self.artifacts = Artifact.box(self.artifacts, **box_kwargs)

    def extend(self, name: str, *, backend: Optional[Backend] = None) -> Graph:
        """ Return an extend copy of self.

            If passed, `artifacts` and `resources` will be deep merged.
        """
        with Graph(name, backend=(backend or self.backend)) as new:
            new.artifacts = deepcopy(self.artifacts)
        return new
