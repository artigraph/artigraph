from __future__ import annotations

from arti.artifacts.core import BaseArtifact


class Annotation(BaseArtifact):
    """ An Annotation is a piece of human knowledge associated with an Artifact.
    """

    # TODO: Set format/storage to some "system default" that can be used across backends.
    # TODO: Have __init__ accept something matching the defined schema by default?

    is_scalar = True
