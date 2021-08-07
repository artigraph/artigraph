from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from arti.formats import Format
from arti.internal.models import Model
from arti.types import Type


class Storage(Model):
    _abstract_ = True

    def supports(self, type_: Type, format: Format) -> None:
        # TODO: Ensure the storage supports all of the specified types and partitioning on the
        # specified field(s).
        pass
