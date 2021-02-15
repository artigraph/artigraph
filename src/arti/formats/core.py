from __future__ import annotations

from typing import ClassVar, Optional

from arti.types.core import Type, TypeSystem


class Format:
    """ Format
    """

    type_system: ClassVar[TypeSystem]

    def validate(self, schema: Optional[Type]) -> None:
        # TODO: Check format.type_system supports the schema. We can likely add a
        # TypeSystem.validate(schema) method that will check for matching TypeAdaptors.
        pass
