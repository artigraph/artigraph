from __future__ import annotations

from typing import ClassVar, Optional

from arti.internal.models import Model
from arti.types.core import Type, TypeSystem


class Format(Model):
    """Format represents file formats such as CSV, Parquet, native (eg: databases), etc.

    Formats are associated with a type system that provides a bridge between the internal
    Artigraph types and any external type information.
    """

    __abstract__ = True
    type_system: ClassVar[TypeSystem]

    def validate_artifact(self, schema: Optional[Type]) -> None:
        # TODO: Check self.type_system supports the schema. We can likely add a TypeSystem method
        # that will check for matching TypeAdapters.
        pass
