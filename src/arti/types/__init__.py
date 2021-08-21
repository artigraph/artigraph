from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from collections.abc import Iterator, Mapping
from operator import attrgetter
from typing import Any, ClassVar, Literal, Optional, Union, cast

from box.box import NO_DEFAULT as _NO_DEFAULT
from pydantic import PrivateAttr, validator

from arti.internal.models import Model
from arti.internal.utils import ObjectBox, class_name, register


class Type(Model):
    """Type represents a data type."""

    _abstract_ = True
    description: Optional[str]
    metadata: ObjectBox = ObjectBox(frozen_box=True)

    @validator("metadata", pre=True)
    @classmethod
    def _freeze_metadata(cls, metadata: Any) -> Any:
        # Convert existing mappings; otherwise let Model validate incorrect types.
        if isinstance(metadata, Mapping):
            return ObjectBox(metadata, frozen_box=True)
        return metadata

    def get_metadata(self, key: str, default: Any = _NO_DEFAULT) -> Any:
        *parts, tail = key.split(".")
        metadata = cast(dict[str, Any], self.metadata)
        for part in parts:
            if default is _NO_DEFAULT:
                metadata = metadata[part]
            else:
                metadata = metadata.get(part, {})
        if default is _NO_DEFAULT:
            return metadata[tail]
        return metadata.get(tail, default=default)


########################
# Core Artigraph Types #
########################


class _Numeric(Type):
    pass


class _Float(_Numeric):
    pass


class _Int(_Numeric):
    pass


class Binary(Type):
    byte_size: Optional[int]


class Boolean(Type):
    pass


class Date(Type):
    pass


class Float16(_Float):
    pass


class Float32(_Float):
    pass


class Float64(_Float):
    pass


class Int8(_Int):
    pass


class Int16(_Int):
    pass


class Int32(_Int):
    pass


class Int64(_Int):
    pass


class List(Type):
    value_type: Type


class Map(Type):
    key_type: Type
    value_type: Type


class Null(Type):
    pass


class String(Type):
    pass


class Struct(Type):
    name: str = "anon"
    fields: dict[str, Type]


class Timestamp(Type):
    """UTC timestamp with configurable precision."""

    precision: Union[
        Literal["second"], Literal["millisecond"], Literal["microsecond"], Literal["nanosecond"]
    ]


class UInt8(_Int):
    pass


class UInt16(_Int):
    pass


class UInt32(_Int):
    pass


class UInt64(_Int):
    pass


##############################
# Type conversion interfaces #
##############################


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
