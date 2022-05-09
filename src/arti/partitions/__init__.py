__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc
from datetime import date
from inspect import getattr_static
from typing import Any, ClassVar

from arti.internal.models import Model
from arti.internal.utils import classproperty, frozendict, register
from arti.types import Collection, Date, Int8, Int16, Int32, Int64, Null, Type


class key_component(property):
    pass


class PartitionKey(Model):
    _abstract_ = True
    _by_type_: "ClassVar[dict[type[Type], type[PartitionKey]]]" = {}

    default_key_components: ClassVar[frozendict[str, str]]
    matching_type: ClassVar[type[Type]]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls._abstract_:
            return
        for attr in ("default_key_components", "matching_type"):
            if not hasattr(cls, attr):
                raise TypeError(f"{cls.__name__} must set `{attr}`")
        if unknown := (set(cls.default_key_components) - cls.key_components):
            raise TypeError(
                f"Unknown key_components in {cls.__name__}.default_key_components: {unknown}"
            )
        register(cls._by_type_, cls.matching_type, cls)

    @classproperty
    def key_components(cls) -> frozenset[str]:
        return frozenset(cls.__fields__) | frozenset(
            name for name in dir(cls) if isinstance(getattr_static(cls, name), key_component)
        )

    @classmethod
    @abc.abstractmethod
    def from_key_components(cls, **key_components: str) -> "PartitionKey":
        raise NotImplementedError(f"Unable to parse '{cls.__name__}' from: {key_components}")

    @classmethod
    def get_class_for(cls, type_: Type) -> type["PartitionKey"]:
        return cls._by_type_[type(type_)]

    @classmethod
    def types_from(cls, type_: Type) -> "CompositeKeyTypes":
        if not isinstance(type_, Collection):
            return frozendict()
        return frozendict(
            {name: cls.get_class_for(field) for name, field in type_.partition_fields.items()}
        )


# CompositeKey is the set of named PartitionKeys that uniquely identify a single partition.
CompositeKey = frozendict[str, PartitionKey]
CompositeKeyTypes = frozendict[str, type[PartitionKey]]

NotPartitioned = CompositeKey()


class DateKey(PartitionKey):
    default_key_components: ClassVar[frozendict[str, str]] = frozendict(Y="Y", m="m:02", d="d:02")
    matching_type = Date

    key: date

    @key_component
    def Y(self) -> int:
        return self.key.year

    @key_component
    def m(self) -> int:
        return self.key.month

    @key_component
    def d(self) -> int:
        return self.key.day

    @key_component
    def iso(self) -> str:
        return self.key.isoformat()

    @classmethod
    def from_key_components(cls, **key_components: str) -> PartitionKey:
        names = set(key_components)
        if names == {"key"}:
            return cls(key=date.fromisoformat(key_components["key"]))
        if names == {"iso"}:
            return cls(key=date.fromisoformat(key_components["iso"]))
        if names == {"Y", "m", "d"}:
            return cls(key=date(*[int(key_components[k]) for k in ("Y", "m", "d")]))
        return super().from_key_components(**key_components)


class _IntKey(PartitionKey):
    _abstract_ = True
    default_key_components: ClassVar[frozendict[str, str]] = frozendict(key="key")

    key: int

    @key_component
    def hex(self) -> str:
        return hex(self.key)

    @classmethod
    def from_key_components(cls, **key_components: str) -> PartitionKey:
        names = set(key_components)
        if names == {"key"}:
            return cls(key=int(key_components["key"]))
        if names == {"hex"}:
            return cls(key=int(key_components["hex"], base=16))
        return super().from_key_components(**key_components)


class Int8Key(_IntKey):
    matching_type = Int8


class Int16Key(_IntKey):
    matching_type = Int16


class Int32Key(_IntKey):
    matching_type = Int32


class Int64Key(_IntKey):
    matching_type = Int64


class NullKey(PartitionKey):
    default_key_components: ClassVar[frozendict[str, str]] = frozendict(key="key")
    matching_type = Null

    key: None = None

    @classmethod
    def from_key_components(cls, **key_components: str) -> PartitionKey:
        if set(key_components) == {"key"}:
            if key_components["key"] != "None":
                raise ValueError(f"'{cls.__name__}' can only be used with 'None'!")
            return cls()
        return super().from_key_components(**key_components)
