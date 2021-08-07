from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from collections.abc import Iterator
from operator import attrgetter
from typing import Any, ClassVar, Literal, Optional, Union

from pydantic import PrivateAttr

from arti.internal.models import Model
from arti.internal.utils import class_name, register


class Type(Model):
    """Type represents a data type."""

    _abstract_ = True
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

    precision: Union[Literal["second"], Literal["millisecond"], Literal["microsecond"]]


class TypeAdapter:
    """TypeAdapter maps between Artigraph types and a foreign type system."""

    key: ClassVar[str] = class_name()

    artigraph: ClassVar[type[Type]]  # The internal Artigraph Type
    system: ClassVar[Any]  # The external system's type

    priority: ClassVar[int] = 0  # Set the priority of this mapping. Higher is better.

    @classmethod
    def matches_artigraph(cls, type_: Type) -> bool:
        return isinstance(type_, cls.artigraph)

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        raise NotImplementedError()

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        raise NotImplementedError()

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        raise NotImplementedError()


class TypeSystem(Model):
    key: str

    _adapter_by_key: dict[str, type[TypeAdapter]] = PrivateAttr(default_factory=dict)

    def register_adapter(self, adapter: type[TypeAdapter]) -> type[TypeAdapter]:
        return register(self._adapter_by_key, adapter.key, adapter)

    @property
    def _priority_sorted_adapters(self) -> Iterator[type[TypeAdapter]]:
        return reversed(sorted(self._adapter_by_key.values(), key=attrgetter("priority")))

    def to_artigraph(self, type_: Any) -> Type:
        for adapter in self._priority_sorted_adapters:
            if adapter.matches_system(type_):
                return adapter.to_artigraph(type_)
        raise NotImplementedError(f"No {self} adapter for system type: {type_}.")

    def to_system(self, type_: Type) -> Any:
        for adapter in self._priority_sorted_adapters:
            if adapter.matches_artigraph(type_):
                return adapter.to_system(type_)
        raise NotImplementedError(f"No {self} adapter for Artigraph type: {type_}.")
