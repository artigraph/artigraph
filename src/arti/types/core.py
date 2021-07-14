from __future__ import annotations

from typing import Any, ClassVar, Literal, Optional, Union

from pydantic import PrivateAttr

from arti.internal.models import Model
from arti.internal.utils import class_name, register


class Type(Model):
    """Type represents a data type."""

    __abstract__ = True
    description: Optional[str]


# TODO: Expand the core types (and/or describe how to customize).


class Struct(Type):
    fields: dict[str, Type]


class Null(Type):
    pass


class String(Type):
    pass


class _Numeric(Type):
    pass


class _Float(_Numeric):
    pass


class Float16(_Float):
    pass


class Float32(_Float):
    pass


class Float64(_Float):
    pass


class _Int(_Numeric):
    pass


class Int32(_Int):
    pass


class Int64(_Int):
    pass


class Date(Type):
    pass


class Timestamp(Type):
    """UTC timestamp with configurable precision."""

    precision: Union[Literal["second"], Literal["millisecond"]]


class TypeAdapter:
    """TypeAdapter maps between Artigraph types and a foreign type system."""

    external: ClassVar[Optional[Any]] = None  # If available, the external type.
    internal: ClassVar[type[Type]]  # Mark which Artigraph Type this maps to.
    priority: ClassVar[int] = 0  # Set the priority of this mapping. Higher is better.

    key: ClassVar[str] = class_name()

    def to_external(self, type_: Type) -> Any:
        raise NotImplementedError()

    def to_internal(self, type_: Any) -> Type:
        raise NotImplementedError()


class TypeSystem(Model):
    key: str

    _adapter_by_key: dict[str, type[TypeAdapter]] = PrivateAttr(default_factory=dict)

    def register_adapter(self, adapter: type[TypeAdapter]) -> type[TypeAdapter]:
        return register(self._adapter_by_key, adapter.key, adapter)

    def from_core(self, type_: Type) -> Any:
        raise NotImplementedError()

    def to_core(self, type_: Any) -> Type:
        raise NotImplementedError()
