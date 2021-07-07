from __future__ import annotations

from collections.abc import Iterator
from itertools import groupby
from operator import attrgetter
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

    @classmethod
    def to_external(cls, type_: Type) -> Any:
        raise NotImplementedError()

    @classmethod
    def to_internal(cls, type_: Any) -> Type:
        raise NotImplementedError()


class TypeSystem(Model):
    key: str
    system_metaclass: bool = False  # whether system types are instances of metaclass (e.g., `type`)

    _adapter_by_key: dict[str, type[TypeAdapter]] = PrivateAttr(default_factory=dict)

    @property
    def _priority_sorted_adapters(self) -> Iterator[type[TypeAdapter]]:
        return reversed(sorted(self._adapter_by_key.values(), key=attrgetter("priority")))

    @property
    def adapter_by_internal_priority(self) -> dict[Any, type[TypeAdapter]]:
        return {
            k: next(v)
            for k, v in groupby(self._priority_sorted_adapters, key=attrgetter("internal"))
        }

    @property
    def adapter_by_external_priority(self) -> dict[Any, type[TypeAdapter]]:
        return {
            k: next(v)
            for k, v in groupby(self._priority_sorted_adapters, key=attrgetter("external"))
        }

    def register_adapter(self, adapter: type[TypeAdapter]) -> type[TypeAdapter]:
        return register(self._adapter_by_key, adapter.key, adapter)

    def _external_type_to_adapter_key(self, type_: Any) -> Any:
        if self.system_metaclass:
            return type_
        else:
            return type(type_)

    def from_core(self, type_: Type) -> Any:
        try:
            external = self.adapter_by_internal_priority[type(type_)].to_external(type_)
        except KeyError:
            raise NotImplementedError(f"No TypeAdapter for core type {type(type_)}.")
        return external

    def to_core(self, type_: Any) -> Type:
        try:
            internal = self.adapter_by_external_priority[
                self._external_type_to_adapter_key(type_)
            ].to_internal(type_)
        except KeyError:
            raise NotImplementedError(f"No TypeAdapter for external type {type(type_)}.")
        return internal
