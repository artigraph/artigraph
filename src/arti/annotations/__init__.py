from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from arti.internal.models import Model


class Annotation(Model):
    """An Annotation is a piece of human knowledge associated with an Artifact."""

    _abstract_ = True
