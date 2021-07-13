from __future__ import annotations

from arti.internal.models import Model, requires_subclass


@requires_subclass
class Annotation(Model):
    """An Annotation is a piece of human knowledge associated with an Artifact."""
