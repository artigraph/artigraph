from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
from datetime import date
from inspect import getattr_static
from typing import Any, ClassVar

from arti.fingerprints import Fingerprint
from arti.internal.mappings import frozendict
from arti.internal.models import Model
from arti.internal.utils import classproperty, register
from arti.types import Collection, Date, Int8, Int16, Int32, Int64, Null, Type


class field_component(property):
    pass


class PartitionField(Model):
    _abstract_ = True
    _by_type_: ClassVar[dict[type[Type], type[PartitionField]]] = {}

    default_components: ClassVar[frozendict[str, str]]
    matching_type: ClassVar[type[Type]]

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if cls._abstract_:
            return
        for attr in ("default_components", "matching_type"):
            if not hasattr(cls, attr):
                raise TypeError(f"{cls.__name__} must set `{attr}`")
        if unknown := set(cls.default_components) - cls.components:
            raise TypeError(f"Unknown components in {cls.__name__}.default_components: {unknown}")
        register(cls._by_type_, cls.matching_type, cls)

    @classproperty
    def components(cls) -> frozenset[str]:
        return frozenset(cls.model_fields) | frozenset(
            name for name in dir(cls) if isinstance(getattr_static(cls, name), field_component)
        )

    @classmethod
    @abc.abstractmethod
    def from_components(cls, **components: str) -> PartitionField:
        raise NotImplementedError(f"Unable to parse '{cls.__name__}' from: {components}")

    @classmethod
    def get_class_for(cls, type_: Type) -> type[PartitionField]:
        return cls._by_type_[type(type_)]


PartitionKeyTypes = frozendict[str, type[PartitionField]]


class PartitionKey(frozendict[str, PartitionField]):
    """The set of named PartitionFields that uniquely identify a single partition."""

    @classmethod
    def types_from(cls, type_: Type) -> PartitionKeyTypes:
        if not isinstance(type_, Collection):
            return frozendict()
        return frozendict(
            {
                name: PartitionField.get_class_for(field)
                for name, field in type_.partition_fields.items()
            }
        )


NotPartitioned = PartitionKey()


class DateField(PartitionField):
    default_components: ClassVar[frozendict[str, str]] = frozendict(Y="Y", m="m:02", d="d:02")
    matching_type = Date

    value: date

    @field_component
    def Y(self) -> int:
        return self.value.year

    @field_component
    def m(self) -> int:
        return self.value.month

    @field_component
    def d(self) -> int:
        return self.value.day

    @field_component
    def iso(self) -> str:
        return self.value.isoformat()

    @classmethod
    def from_components(cls, **components: str) -> PartitionField:
        names = set(components)
        if names == {"value"}:
            return cls(value=date.fromisoformat(components["value"]))
        if names == {"iso"}:
            return cls(value=date.fromisoformat(components["iso"]))
        if names == {"Y", "m", "d"}:
            return cls(value=date(*[int(components[k]) for k in ("Y", "m", "d")]))
        return super().from_components(**components)


class _IntField(PartitionField):
    _abstract_ = True
    default_components: ClassVar[frozendict[str, str]] = frozendict(value="value")

    value: int

    @field_component
    def hex(self) -> str:
        return hex(self.value)

    @classmethod
    def from_components(cls, **components: str) -> PartitionField:
        names = set(components)
        if names == {"value"}:
            return cls(value=int(components["value"]))
        if names == {"hex"}:
            return cls(value=int(components["hex"], base=16))
        return super().from_components(**components)


class Int8Field(_IntField):
    matching_type = Int8


class Int16Field(_IntField):
    matching_type = Int16


class Int32Field(_IntField):
    matching_type = Int32


class Int64Field(_IntField):
    matching_type = Int64


class NullField(PartitionField):
    default_components: ClassVar[frozendict[str, str]] = frozendict(value="value")
    matching_type = Null

    value: None = None

    @classmethod
    def from_components(cls, **components: str) -> PartitionField:
        if set(components) == {"value"}:
            if components["value"] != "None":
                raise ValueError(f"'{cls.__name__}' can only be used with 'None'!")
            return cls()
        return super().from_components(**components)


InputFingerprints = frozendict[PartitionKey, Fingerprint | None]
