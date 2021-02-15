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
            "default_box": True,
            "frozen_box": sealed,  # Unfreeze the box while building the graph
        }
        self.artifacts = Artifact.box(self.artifacts, **box_kwargs)
        # TODO: Sketch out cross graph references and boundary/visibility semantics.
        #
        # if sealed:
        #    self._artifact_index = self._compute_index(self.artifacts)
        #    for artifact in self._artifact_index:
        #        if artifact.graph is None or artifact.graph is self:
        #            artifact.graph = self
        #        elif artifact.graph is not self:
        #            # Graphs default to public, but if any Artifacts are published, those define the
        #            # boundary.
        #            is_public = (
        #                artifact.graph.public_artifacts
        #                and artifact not in artifact.graph.public_artifacts
        #            )
        #            if not is_public:
        #                raise ValueError(...)  # Error about internal artifact being referenced.
        #            # TODO: Maybe add to a list of "read only" artifacts? Or perhaps the Executor's build
        #            # logic just has to check that the Artifact's Graph is in one of the to-be-built
        #            # Graphs?
        #
        # TODO: Do we need to ensure Producers don't get called with Artifacts directly from another
        # graph so that we can validate publishing/visibility? ex:
        #     with Graph("G1") as g1:
        #         g1.artifacts.a = 5
        #     with Graph("G2") as g2:
        #         g2.artifacts.b = Producer(g1.artifacts.a) # We haven't check visiblity!

    def extend(self, name: str, *, backend: Optional[Backend] = None) -> Graph:
        """ Return an extend copy of self.

            If passed, `artifacts` and `resources` will be deep merged.
        """
        with Graph(name, backend=(backend or self.backend)) as new:
            new.artifacts = deepcopy(self.artifacts)
        return new
