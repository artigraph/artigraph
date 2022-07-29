__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from operator import attrgetter
from typing import Any, ClassVar, Literal, Optional

from pydantic import PrivateAttr, validator

from arti.internal.models import Model
from arti.internal.type_hints import lenient_issubclass
from arti.internal.utils import NoCopyDict, class_name, frozendict, register

DEFAULT_ANONYMOUS_NAME = "anon"

_TimePrecision = Literal["second", "millisecond", "microsecond", "nanosecond"]


class Type(Model):
    """Type represents a data type."""

    _abstract_ = True
    # NOTE: Exclude the description to minimize fingerprint changes (and thus rebuilds).
    _fingerprint_excludes_ = frozenset(["description"])

    description: Optional[str]
    nullable: bool = False

    @property
    def friendly_key(self) -> str:
        """A human-readable class-name like key representing this Type.

        The key doesn't have to be unique, just a best effort, meaningful string.
        """
        return self._class_key_


class _NamedMixin(Model):
    name: str = DEFAULT_ANONYMOUS_NAME

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

    @property
    @abstractmethod
    def _default_friendly_key(self) -> str:
        raise NotImplementedError()

    @property
    def friendly_key(self) -> str:
        return self._default_friendly_key if self.name == DEFAULT_ANONYMOUS_NAME else self.name


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


class DateTime(Type):
    """A Date and Time as shown on a calendar and clock, independent of timezone."""

    precision: _TimePrecision


class Enum(_NamedMixin, Type):
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
        py_type = python_type_system.to_system(arti_type, hints={})
        mismatched_items = [item for item in items if not lenient_issubclass(type(item), py_type)]
        if mismatched_items:
            raise ValueError(f"incompatible {arti_type} ({py_type}) item(s): {mismatched_items}")
        return items

    @property
    def _default_friendly_key(self) -> str:
        return f"{self.type.friendly_key}{self._class_key_}"


class Float16(_Float):
    pass


class Float32(_Float):
    pass


class Float64(_Float):
    pass


class Geography(Type):
    format: Optional[str]  # "WKB", "WKT", etc
    srid: Optional[str]


class Int8(_Int):
    pass


class Int16(_Int):
    pass


class Int32(_Int):
    pass


class Int64(_Int):
    pass


class List(Type):
    element: Type

    @property
    def friendly_key(self) -> str:
        return f"{self.element.friendly_key}{self._class_key_}"


class Collection(_NamedMixin, List):
    """A collection of elements with partition and cluster metadata.

    Collections should not be nested in other types.
    """

    partition_by: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()

    @validator("partition_by", "cluster_by")
    @classmethod
    def _validate_field_ref(
        cls, references: tuple[str, ...], values: dict[str, Any]
    ) -> tuple[str, ...]:
        if (element := values.get("element")) is None:
            return references
        if references and not isinstance(element, Struct):
            raise ValueError("requires element to be a Struct")
        known, requested = set(element.fields), set(references)
        if unknown := requested - known:
            raise ValueError(f"unknown field(s): {unknown}")
        return references

    @validator("cluster_by")
    @classmethod
    def _validate_cluster_by(
        cls, cluster_by: tuple[str, ...], values: dict[str, Any]
    ) -> tuple[str, ...]:
        if (partition_by := values.get("partition_by")) is None:
            return cluster_by
        if overlapping := set(cluster_by) & set(partition_by):
            raise ValueError(f"clustering fields overlap with partition fields: {overlapping}")
        return cluster_by

    @property
    def _default_friendly_key(self) -> str:
        return f"{self.element.friendly_key}{self._class_key_}"

    @property
    def fields(self) -> frozendict[str, Type]:
        """Shorthand accessor to access Struct element fields.

        If the element is not a Struct, an AttributeError will be raised.
        """
        return self.element.fields  # type: ignore # We want the standard AttributeError

    @property
    def is_partitioned(self) -> bool:
        return bool(self.partition_fields)

    @property
    def partition_fields(self) -> frozendict[str, Type]:
        if not isinstance(self.element, Struct):
            return frozendict()
        return frozendict({name: self.element.fields[name] for name in self.partition_by})


class Map(Type):
    key: Type
    value: Type

    @property
    def friendly_key(self) -> str:
        return f"{self.key.friendly_key}To{self.value.friendly_key}"


class Null(Type):
    pass


class Set(Type):
    element: Type

    @property
    def friendly_key(self) -> str:
        return f"{self.element.friendly_key}{self._class_key_}"


class String(Type):
    pass


class Struct(_NamedMixin, Type):
    fields: frozendict[str, Type]

    @property
    def _default_friendly_key(self) -> str:
        return f"Custom{self._class_key_}"  # :shrug:


class Time(Type):
    precision: _TimePrecision


class Timestamp(Type):
    """UTC timestamp with configurable precision."""

    precision: _TimePrecision

    @property
    def friendly_key(self) -> str:
        return f"{self.precision.title()}{self._class_key_}"


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
    def matches_artigraph(cls, type_: Type, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, cls.artigraph)

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any], type_system: "TypeSystem") -> Type:
        raise NotImplementedError()

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        raise NotImplementedError()

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: "TypeSystem") -> Any:
        raise NotImplementedError()


# _ScalarClassTypeAdapter can be used for scalars defined as python types (eg: int or str for the
# python TypeSystem).
class _ScalarClassTypeAdapter(TypeAdapter):
    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any], type_system: "TypeSystem") -> Type:
        return cls.artigraph()

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return lenient_issubclass(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: "TypeSystem") -> Any:
        return cls.system

    @classmethod
    def generate(
        cls,
        *,
        artigraph: type[Type],
        system: Any,
        priority: int = 0,
        type_system: "TypeSystem",
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
    extends: "tuple[TypeSystem, ...]" = ()

    # NOTE: Use a NoCopyDict to avoid copies of the registry. Otherwise, TypeSystems that extend
    # this TypeSystem will only see the adapters registered *as of initialization* (as pydantic
    # would deepcopy the TypeSystems in the `extends` argument).
    _adapter_by_key: NoCopyDict[str, type[TypeAdapter]] = PrivateAttr(default_factory=NoCopyDict)

    def register_adapter(self, adapter: type[TypeAdapter]) -> type[TypeAdapter]:
        return register(self._adapter_by_key, adapter.key, adapter)

    @property
    def _priority_sorted_adapters(self) -> Iterator[type[TypeAdapter]]:
        return reversed(sorted(self._adapter_by_key.values(), key=attrgetter("priority")))

    def to_artigraph(
        self, type_: Any, *, hints: dict[str, Any], root_type_system: "Optional[TypeSystem]" = None
    ) -> Type:
        root_type_system = root_type_system or self
        for adapter in self._priority_sorted_adapters:
            if adapter.matches_system(type_, hints=hints):
                return adapter.to_artigraph(type_, hints=hints, type_system=root_type_system)
        for type_system in self.extends:
            try:
                return type_system.to_artigraph(
                    type_, hints=hints, root_type_system=root_type_system
                )
            except NotImplementedError:
                pass
        raise NotImplementedError(f"No {root_type_system} adapter for system type: {type_}.")

    def to_system(
        self, type_: Type, *, hints: dict[str, Any], root_type_system: "Optional[TypeSystem]" = None
    ) -> Any:
        root_type_system = root_type_system or self
        for adapter in self._priority_sorted_adapters:
            if adapter.matches_artigraph(type_, hints=hints):
                return adapter.to_system(type_, hints=hints, type_system=root_type_system)
        for type_system in self.extends:
            try:
                return type_system.to_system(type_, hints=hints, root_type_system=root_type_system)
            except NotImplementedError:
                pass
        raise NotImplementedError(f"No {root_type_system} adapter for Artigraph type: {type_}.")


# Fix ForwardRefs in outer_type_, pending: https://github.com/samuelcolvin/pydantic/pull/4249
TypeSystem.__fields__["extends"].outer_type_ = tuple[TypeSystem, ...]
