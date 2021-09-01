from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

import abc
from datetime import date

from arti.internal.models import Model
from arti.internal.utils import classproperty


class key_component(property):
    pass


class PartitionKey(Model):
    @classproperty
    @classmethod
    def key_components(cls) -> frozenset[str]:
        return frozenset(
            {name for name, attr in cls.__dict__.items() if isinstance(attr, key_component)}
        )

    @classmethod
    @abc.abstractmethod
    def from_key_components(cls, **key_components: str) -> PartitionKey:
        raise NotImplementedError(f"Unable to parse '{cls.__name__}' from: {key_components}")


class DateKey(PartitionKey):
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


class IntKey(PartitionKey):
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


class NullKey(PartitionKey):
    key: None = None

    @classmethod
    def from_key_components(cls, **key_components: str) -> PartitionKey:
        if set(key_components) == {"key"}:
            if key_components["key"] != "None":
                raise ValueError(f"'{cls.__name__}' can only be used with 'None'!")
            return cls()
        return super().from_key_components(**key_components)
