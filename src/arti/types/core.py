from __future__ import annotations

from collections.abc import Iterator
from itertools import groupby
from operator import attrgetter
from typing import Any, ClassVar, Literal, Optional, Union

from arti.internal.utils import class_name, register


class Type:
    """Type represents a data type."""

    def __init__(self, *, description: Optional[str] = None) -> None:
        if type(self) is Type:
            raise ValueError(
                "Type cannot be instantiated directly, please use the appropriate subclass!"
            )
        self.description = description


# TODO: Expand the core types (and/or describe how to customize).


class Struct(Type):
    def __init__(self, fields: dict[str, Type], *, description: Optional[str] = None) -> None:
        self.fields = fields
        super().__init__(description=description)


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

    def __init__(
        self,
        precision: Union[Literal["second"], Literal["millisecond"]],
        *,
        description: Optional[str] = None,
    ) -> None:
        self.precision = precision
        super().__init__(description=description)


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


class TypeSystem:
    def __init__(self, key: str) -> None:
        self.key = key
        self.adapter_by_key: dict[str, type[TypeAdapter]] = {}
        super().__init__()

    @property
    def _priority_sorted_adapters(self) -> Iterator[type[TypeAdapter]]:
        return reversed(sorted(self.adapter_by_key.values(), key=attrgetter("priority")))

    @property
    def adapter_by_internal_priority(self) -> dict[Any, type[TypeAdapter]]:
        return {
            k: next(v)
            for k, v in groupby(self._priority_sorted_adapters, key=attrgetter("internal"))
        }

    @property
    def adapter_by_external_priority(self) -> Dict[Any, type[TypeAdapter]]:
        return {
            k: next(v)
            for k, v in groupby(self._priority_sorted_adapters, key=attrgetter("external"))
        }

    def register_adapter(self, adapter: type[TypeAdapter]) -> type[TypeAdapter]:
        return register(self.adapter_by_key, adapter.key, adapter)

    def from_core(self, type_: Type) -> Any:
        try:
            external = self.adapter_by_internal_priority[type(type_)].to_external(type_)
        except KeyError:
            raise NotImplementedError(f"No TypeAdapter for core type {type(type_)}.")
        return external

    def to_core(self, type_: Any) -> Type:
        try:
            internal = self.adapter_by_external_priority[type(type_)].to_internal(type_)
        except KeyError:
            raise NotImplementedError(f"No TypeAdapter for external type {type(type_)}.")
        return internal
