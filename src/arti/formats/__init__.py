__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from typing import ClassVar, Optional

from pydantic import validator

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
    type: Optional[Type] = None

    @validator("type")
    @classmethod
    def validate_type(cls, type_: Type) -> Type:
        # TODO: Check self.type_system supports the type. We can likely add a TypeSystem method
        # that will check for matching TypeAdapters.
        return type_
