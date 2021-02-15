from __future__ import annotations

from arti.artifacts.core import BaseArtifact


class Annotation(BaseArtifact):
    """ An Annotation is a piece of human knowledge associated with an Artifact.
    """

    # TODO: Set format/storage to some "system default" that can be used across backends.

    is_scalar = True
