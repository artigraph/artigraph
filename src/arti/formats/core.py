from __future__ import annotations

from typing import ClassVar, Optional

from arti.types.core import Type, TypeSystem


class Format:
    """ Format represents file formats such as CSV, Parquet, native (eg: databases), etc.

        Formats are associated with a type system that provides a bridge between the internal
        Artigraph types and any external type information.
    """

    type_system: ClassVar[TypeSystem]

    def validate(self, schema: Optional[Type]) -> None:
        # TODO: Check format.type_system supports the schema. We can likely add a
        # TypeSystem.validate(schema) method that will check for matching TypeAdaptors.
        pass
