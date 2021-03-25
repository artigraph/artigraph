from __future__ import annotations

from collections.abc import Callable, MutableMapping
from typing import Any, ClassVar, TypeVar, Union, cast

from box import Box


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


RegisterK = TypeVar("RegisterK", bound=str)
RegisterV = TypeVar("RegisterV")


def register(registry: dict[RegisterK, RegisterV], key: RegisterK, value: RegisterV) -> RegisterV:
    if key in registry:
        existing = registry[key]
        raise ValueError(f"{key} is already registered with: {existing}!")
    registry[key] = value
    return value


T = TypeVar("T")


class TypedBox(Box, MutableMapping[str, Union[T, MutableMapping[str, T]]]):
    """ TypedBox holds a collection of typed values.

        Subclasses must set the __target_type__ to a base class for the contained values.
    """

    __target_type__: ClassVar[type[T]]

    @classmethod
    def __class_getitem__(cls, target_type: type[T]) -> TypedBox[T]:
        return cast(
            "TypedBox[T]",
            type(f"{target_type.__name__}Box", (cls,), {"__target_type__": target_type}),
        )

    def __cast_value(self, value: Any) -> T:
        if isinstance(value, self.__target_type__):
            return value
        tgt_name = self.__target_type__.__name__
        if hasattr(self.__target_type__, "cast"):
            casted = cast(Any, self.__target_type__).cast(value)
            if isinstance(casted, self.__target_type__):
                return casted
            raise TypeError(
                f"Expected {tgt_name}.cast({value}) to return an instance of {tgt_name}, got: {casted}"
            )
        raise TypeError(f"Expected an instance of {tgt_name}, got: {value}")

    # NOTE: Box uses name mangling (double __) to prevent conflicts with contained values.
    def _Box__convert_and_store(self, item: str, value: T) -> None:
        if isinstance(value, dict):
            super()._Box__convert_and_store(item, value)  # pylint: disable=no-member
        elif item in self:
            raise ValueError(f"{item} is already set!")
        else:
            super()._Box__convert_and_store(item, self.__cast_value(value))
