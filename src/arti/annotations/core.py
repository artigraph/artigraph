from __future__ import annotations

from arti.internal.models import Model


class Annotation(Model):
    """An Annotation is a piece of human knowledge associated with an Artifact."""

    __abstract__ = True
