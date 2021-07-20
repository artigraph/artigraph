from __future__ import annotations

from typing import ClassVar

from arti.internal.models import Model
from arti.types.core import Type, TypeSystem


class Format(Model):
    """Format represents file formats such as CSV, Parquet, native (eg: databases), etc.

    Formats are associated with a type system that provides a bridge between the internal
    Artigraph types and any external type information.
    """

    _abstract_ = True
    type_system: ClassVar[TypeSystem]

    def supports(self, type_: Type) -> None:
        # TODO: Check self.type_system supports the type. We can likely add a TypeSystem method
        # that will check for matching TypeAdapters.
        pass
