from __future__ import annotations

from arti.artifacts.core import BaseArtifact


class Annotation(BaseArtifact):
    """An Annotation is a piece of human knowledge associated with an Artifact."""

    # TODO: Set format/storage to some "system default" that can be used across backends.
    #
    # TODO: Derive the schema from the `__init__` method signature (or vice versa) - we expect users
    # to initialize these directly.

    is_scalar = True
