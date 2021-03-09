from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, cast


class ClassName:
    def __get__(self, obj: Any, type_: type[Any]) -> str:
        return type_.__name__


class_name = cast(Callable[[], str], ClassName)


PropReturn = TypeVar("PropReturn")


class classproperty:
    def __init__(self, f: Callable[..., PropReturn]) -> None:
        self.f = f

    def __get__(self, obj: Any, type_: type[Any]) -> PropReturn:
        return self.f(type_)


def ordinal(n: int) -> str:
    """ Convert an integer into its ordinal representation.
    """
    n = int(n)
    suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    return str(n) + suffix


K = TypeVar("K", bound=str)
V = TypeVar("V")


def register(registry: dict[K, V], key: K, value: V) -> V:
    if key in registry:
        existing = registry[key]
        raise ValueError(f"{key} is already registered with: {existing}!")
    registry[key] = value
    return value
