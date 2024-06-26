from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Iterable, Mapping
from operator import attrgetter
from typing import Annotated, Any, ClassVar, Literal

from pydantic import PrivateAttr, ValidationInfo, field_validator

from arti.fingerprints import SkipFingerprint
from arti.internal.mappings import FrozenMapping, frozendict
from arti.internal.models import Model
from arti.internal.type_hints import lenient_issubclass
from arti.internal.utils import NoCopyDict, class_name, register

DEFAULT_ANONYMOUS_NAME = "anon"


def is_partitioned(type_: Type) -> bool:
    """Helper function to determine whether the type is partitioned."""
    return isinstance(type_, Collection) and bool(type_.partition_fields)


class Type(Model):
    """Type represents a data type."""

    _abstract_ = True

    # NOTE: Skip fingerprinting the description to minimize changes (and thus rebuilds).
    description: Annotated[str | None, SkipFingerprint()] = None
    nullable: bool = False

    @property
    def friendly_key(self) -> str:
        """A human-readable class-name like key representing this Type.

        The key doesn't have to be unique, just a best effort, meaningful string.
        """
        return self._arti_type_key_


class _ContainerMixin(Model):
    element: Type


class _NamedMixin(Model):
    name: str = DEFAULT_ANONYMOUS_NAME

    @property
    @abstractmethod
    def _default_friendly_key(self) -> str:
        raise NotImplementedError()

    @property
    def friendly_key(self) -> str:
        return self._default_friendly_key if self.name == DEFAULT_ANONYMOUS_NAME else self.name


class _TimeMixin(Model):
    precision: Literal["second", "millisecond", "microsecond", "nanosecond"]


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
    byte_size: int | None = None


class Boolean(Type):
    pass


class Date(Type):
    pass


class DateTime(_TimeMixin, Type):
    """A Date and Time as shown on a calendar and clock, independent of timezone."""


class Enum(_NamedMixin, Type):
    type: Type
    items: frozenset[Any]

    @field_validator("items", mode="before")
    @classmethod
    def _cast_values(cls, items: Any) -> Any:
        if isinstance(items, Iterable) and not isinstance(items, Mapping):
            return frozenset(items)
        return items  # coverage: ignore

    @field_validator("items")
    @classmethod
    def _validate_values(cls, items: frozenset[Any], info: ValidationInfo) -> frozenset[Any]:
        from arti.types.python import python_type_system

        if len(items) == 0:
            raise ValueError("cannot be empty.")
        # `type` will be missing if it doesn't pass validation.
        if (arti_type := info.data.get("type")) is None:
            return items  # coverage: ignore
        py_type = python_type_system.to_system(arti_type, hints={})
        mismatched_items = [item for item in items if not lenient_issubclass(type(item), py_type)]
        if mismatched_items:
            raise ValueError(f"incompatible {arti_type} ({py_type}) item(s): {mismatched_items}")
        return items

    @property
    def _default_friendly_key(self) -> str:
        return f"{self.type.friendly_key}{self._arti_type_key_}"


class Float16(_Float):
    pass


class Float32(_Float):
    pass


class Float64(_Float):
    pass


class Geography(Type):
    format: str | None = None  # "WKB", "WKT", etc
    srid: str | None = None


class Int8(_Int):
    pass


class Int16(_Int):
    pass


class Int32(_Int):
    pass


class Int64(_Int):
    pass


class List(_ContainerMixin, Type):
    @property
    def friendly_key(self) -> str:
        return f"{self.element.friendly_key}{self._arti_type_key_}"


class Collection(_NamedMixin, List):
    """A collection of Structs with partition and cluster metadata.

    Collections should not be nested in other types.
    """

    element: Struct  # Partitioning requires fields, so constrain the element further than List.
    partition_by: tuple[str, ...] = ()
    cluster_by: tuple[str, ...] = ()

    @field_validator("partition_by", "cluster_by")
    @classmethod
    def _validate_field_ref(
        cls, references: tuple[str, ...], info: ValidationInfo
    ) -> tuple[str, ...]:
        if (element := info.data.get("element")) is None:
            return references  # coverage: ignore
        assert isinstance(element, Struct)
        known, requested = set(element.fields), set(references)
        if unknown := requested - known:
            raise ValueError(f"field '{unknown}' does not exist on {element}")
        return references

    @field_validator("cluster_by")
    @classmethod
    def _validate_cluster_by(
        cls, cluster_by: tuple[str, ...], info: ValidationInfo
    ) -> tuple[str, ...]:
        if (partition_by := info.data.get("partition_by")) is None:
            return cluster_by  # coverage: ignore
        if overlapping := set(cluster_by) & set(partition_by):
            raise ValueError(f"cluster_by overlaps with partition_by: {overlapping}")
        return cluster_by

    @property
    def _default_friendly_key(self) -> str:
        return f"{self.element.friendly_key}{self._arti_type_key_}"

    @property
    def fields(self) -> frozendict[str, Type]:
        return self.element.fields

    @property
    def partition_fields(self) -> frozendict[str, Type]:
        return frozendict({name: self.fields[name] for name in self.partition_by})

    @property
    def cluster_fields(self) -> frozendict[str, Type]:
        return frozendict({name: self.fields[name] for name in self.cluster_by})


class Map(Type):
    key: Type
    value: Type

    @property
    def friendly_key(self) -> str:
        return f"{self.key.friendly_key}To{self.value.friendly_key}"


class Null(Type):
    pass


class Set(_ContainerMixin, Type):
    @property
    def friendly_key(self) -> str:
        return f"{self.element.friendly_key}{self._arti_type_key_}"


class String(Type):
    pass


class Struct(_NamedMixin, Type):
    fields: FrozenMapping[str, Type]

    @property
    def _default_friendly_key(self) -> str:
        return f"Custom{self._arti_type_key_}"  # :shrug:


class Time(_TimeMixin, Type):
    pass


class Timestamp(_TimeMixin, Type):
    """UTC timestamp with configurable precision."""

    @property
    def friendly_key(self) -> str:
        return f"{self.precision.title()}{self._arti_type_key_}"


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
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any], type_system: TypeSystem) -> Type:
        raise NotImplementedError()

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        raise NotImplementedError()

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        raise NotImplementedError()


# _ScalarClassTypeAdapter can be used for scalars defined as python types (eg: int or str for the
# python TypeSystem).
class _ScalarClassTypeAdapter(TypeAdapter):
    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any], type_system: TypeSystem) -> Type:
        return cls.artigraph()

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return lenient_issubclass(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem) -> Any:
        return cls.system

    @classmethod
    def generate(
        cls,
        *,
        artigraph: type[Type],
        system: Any,
        priority: int = 0,
        type_system: TypeSystem,
        name: str | None = None,
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
    extends: tuple[TypeSystem, ...] = ()

    # NOTE: Use a NoCopyDict to avoid copies of the registry. Otherwise, TypeSystems that extend
    # this TypeSystem will only see the adapters registered *as of initialization* (as pydantic
    # would deepcopy the TypeSystems in the `extends` argument).
    _adapter_by_key: NoCopyDict[str, type[TypeAdapter]] = PrivateAttr(default_factory=NoCopyDict)

    def register_adapter(self, adapter: type[TypeAdapter]) -> type[TypeAdapter]:
        return register(self._adapter_by_key, adapter.key, adapter)

    @property
    def _priority_sorted_adapters(self) -> list[type[TypeAdapter]]:
        return sorted(self._adapter_by_key.values(), key=attrgetter("priority"), reverse=True)

    def to_artigraph(
        self, type_: Any, *, hints: dict[str, Any], root_type_system: TypeSystem | None = None
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
        self, type_: Type, *, hints: dict[str, Any], root_type_system: TypeSystem | None = None
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
