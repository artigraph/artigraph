__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from typing import ClassVar, Optional

from pydantic import Field, validator

from arti.internal.models import Model
from arti.types import Type, TypeSystem


class Format(Model):
    """Format represents file formats such as CSV, Parquet, native (eg: databases), etc.

    Formats are associated with a type system that provides a bridge between the internal
    Artigraph types and any external type information.
    """

    _abstract_ = True
    type_system: ClassVar[TypeSystem]

    extension: str = ""
    type: Optional[Type] = Field(None, repr=False)

    @validator("type")
    @classmethod
    def validate_type(cls, type_: Type) -> Type:
        # Ensure our type system can handle the provided type.
        cls.type_system.to_system(type_, hints={})
        return type_
