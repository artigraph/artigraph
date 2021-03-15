from __future__ import annotations

from typing import ClassVar, Optional, Union

from arti.types.core import Type, TypeSystem


class Format:
    """Format represents file formats such as CSV, Parquet, native (eg: databases), etc.

    Formats are associated with a type system that provides a bridge between the internal
    Artigraph types and any external type information.
    """

    type_system: ClassVar[TypeSystem]

    def validate(self, schema: Optional[Type]) -> None:
        # TODO: Check format.type_system supports the schema. We can likely add a
        # TypeSystem.validate(schema) method that will check for matching TypeAdaptors.
        pass

    def __init__(self, format_type: Optional[str] = None) -> None:
        self.type = format_type

    @classmethod
    def from_dict(cls, format_dict: dict[str, str]) -> Format:
        if "type" not in format_dict:
            raise ValueError(
                f'Missing a required "type" key in the Format dict. Available keys: {format_dict.keys()}'
            )
        return cls(format_dict["type"])

    def to_dict(self) -> dict[str, Union[str, None]]:
        return {"type": self.type}
