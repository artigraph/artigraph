__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from typing import ClassVar

from arti.internal.models import Model
from arti.internal.type_hints import Self
from arti.types import Type, TypeSystem


class Format(Model):
    """Format represents file formats such as CSV, Parquet, native (eg: databases), etc.

    Formats are associated with a type system that provides a bridge between the internal
    Artigraph types and any external type information.
    """

    _abstract_ = True
    type_system: ClassVar[TypeSystem]

    extension: str = ""

    def _visit_type(self, type_: Type) -> Self:
        # Ensure our type system can handle the provided type.
        self.type_system.to_system(type_, hints={})
        return self

    @classmethod
    def get_default(cls) -> "Format":
        from arti.formats.json import JSON

        return JSON()  # TODO: Support some sort of configurable defaults.
