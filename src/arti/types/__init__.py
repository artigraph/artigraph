from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from collections.abc import Iterable, Iterator, Mapping
from operator import attrgetter
from typing import Any, ClassVar, Literal, Optional, cast

from box.box import NO_DEFAULT as _NO_DEFAULT
from pydantic import PrivateAttr, validator

from arti.internal.models import Model
from arti.internal.type_hints import lenient_issubclass
from arti.internal.utils import ObjectBox, class_name, register


class Type(Model):
    """Type represents a data type."""

    _abstract_ = True
    description: Optional[str]
    metadata: ObjectBox = ObjectBox(frozen_box=True)
    nullable: bool = False

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

    # The metadata ObjectBox can't be converted to a python type (via the python_type_system), so
    # ignore it when converting into Struct instances (for loading into databases, eg: sgqlc).
    # Alternatively, we may consider converting to a JSON/unstructured Type of some sort.
    @classmethod
    def _pydantic_type_system_ignored_fields_hook_(cls) -> frozenset[str]:
        return frozenset(["metadata"]) | super()._pydantic_type_system_ignored_fields_hook_()


class _NamedMixin(Model):
    name: str = "anon"

    @classmethod
    def _pydantic_type_system_post_field_conversion_hook_(
        cls, type_: Type, *, name: str, required: bool
    ) -> Type:
        type_ = super()._pydantic_type_system_post_field_conversion_hook_(
            type_, name=name, required=required
        )
        if "name" not in type_.__fields_set__:
            type_ = type_.copy(update={"name": name})
        return type_


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


class Enum(Type, _NamedMixin):
    type: Type
    items: frozenset[Any]

    @validator("items", pre=True)
    @classmethod
    def _cast_values(cls, items: Any) -> Any:
        if isinstance(items, Iterable) and not isinstance(items, Mapping):
            return frozenset(items)
        return items

    @validator("items")
    @classmethod
    def _validate_values(cls, items: frozenset[Any], values: dict[str, Any]) -> frozenset[Any]:
        from arti.types.python import python_type_system

        if len(items) == 0:
            raise ValueError("cannot be empty.")
        # `type` will be missing if it doesn't pass validation.
        if (arti_type := values.get("type")) is None:
            return items
        py_type = python_type_system.to_system(arti_type)
        mismatched_items = [item for item in items if not lenient_issubclass(type(item), py_type)]
        if mismatched_items:
            raise ValueError(f"incompatible {arti_type} ({py_type}) item(s): {mismatched_items}")
        return items


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


class Struct(Type, _NamedMixin):
    fields: dict[str, Type]


class Timestamp(Type):
    """UTC timestamp with configurable precision."""

    precision: Literal["second", "millisecond", "microsecond", "nanosecond"]


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


# _ScalarClassTypeAdapter can be used for scalars defined as python types (eg: int or str for the
# python TypeSystem).
class _ScalarClassTypeAdapter(TypeAdapter):
    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        return cls.artigraph()

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        return lenient_issubclass(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        return cls.system

    @classmethod
    def generate(
        cls,
        *,
        artigraph: type[Type],
        system: Any,
        priority: int = 0,
        type_system: TypeSystem,
        name: Optional[str] = None,
    ) -> type[TypeAdapter]:
        """Generate a _ScalarClassTypeAdapter subclass for the scalar system type."""
        name = name or f"{type_system.key}{artigraph.__name__}"
        return type_system.register_adapter(
            type(
                name,
                (cls,),
                {"artigraph": artigraph, "system": system, "priority": priority},
            )
        )


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
