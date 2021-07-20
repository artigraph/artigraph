from __future__ import annotations

from arti.formats.core import Format
from arti.internal.models import Model
from arti.types.core import Type


class Storage(Model):
    _abstract_ = True

    def supports(self, type_: Type, format: Format) -> None:
        # TODO: Ensure the storage supports all of the specified types and partitioning on the
        # specified field(s).
        pass
