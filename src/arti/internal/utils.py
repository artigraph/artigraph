from __future__ import annotations

from collections.abc import Callable, MutableMapping
from typing import Any, ClassVar, Generic, Optional, TypeVar, Union, cast

from box import Box


class ClassName:
    def __get__(self, obj: Any, type_: type[Any]) -> str:
        return type_.__name__


class_name = cast(Callable[[], str], ClassName)


PropReturn = TypeVar("PropReturn")


class classproperty(Generic[PropReturn]):
    """Access a @classmethod like a @property.

    Can be stacked above @classmethod (to satisfy pylint, mypy, etc).
    """

    def __init__(self, f: Callable[..., PropReturn]) -> None:
        self.f = f
        if isinstance(self.f, classmethod):
            self.f = lambda type_: f.__get__(None, type_)()

    def __get__(self, obj: Any, type_: Any) -> PropReturn:
        return self.f(type_)


_int_sub = TypeVar("_int_sub", bound="_int")


class _int(int):
    def __repr__(self) -> str:
        return f"{qname(self)}({int(self)})"

    # Stock magics.
    #
    # Using "self: TypeVar" so mypy will detect the returned subclass (rather than _int).

    def __add__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__add__(x))

    def __and__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__and__(n))

    def __ceil__(self: _int_sub) -> _int_sub:
        return type(self)(super().__ceil__())

    def __floor__(self: _int_sub) -> _int_sub:
        return type(self)(super().__floor__())

    def __floordiv__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__floordiv__(x))

    def __invert__(self: _int_sub) -> _int_sub:
        return type(self)(super().__invert__())

    def __lshift__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__lshift__(n))

    def __mod__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__mod__(x))

    def __mul__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__mul__(x))

    def __neg__(self: _int_sub) -> _int_sub:
        return type(self)(super().__neg__())

    def __or__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__or__(n))

    def __pos__(self: _int_sub) -> _int_sub:
        return type(self)(super().__pos__())

    def __radd__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__radd__(x))

    def __rand__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__rand__(n))

    def __rfloordiv__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__rfloordiv__(x))

    def __rlshift__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__rlshift__(n))

    def __rmod__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__rmod__(x))

    def __rmul__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__rmul__(x))

    def __ror__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__ror__(n))

    def __round__(self: _int_sub, ndigits: Optional[int] = 0) -> _int_sub:
        return type(self)(super().__round__(ndigits))

    def __rrshift__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__rrshift__(n))

    def __rshift__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__rshift__(n))

    def __rsub__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__rsub__(x))

    def __rxor__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__rxor__(n))

    def __sub__(self: _int_sub, x: int) -> _int_sub:
        return type(self)(super().__sub__(x))

    def __trunc__(self: _int_sub) -> _int_sub:
        return type(self)(super().__trunc__())

    def __xor__(self: _int_sub, n: int) -> _int_sub:
        return type(self)(super().__xor__(n))


class int64(_int):
    _min, _max = -(2 ** 63), (2 ** 63) - 1

    def __new__(cls, i: Union[int, int64, uint64]) -> int64:
        if i > cls._max:
            if isinstance(i, uint64):
                i = int(i) - uint64._max - 1
            else:
                raise ValueError(f"{i} is too large for int64. Hint: cast to uint64 first.")
        if i < cls._min:
            raise ValueError(f"{i} is too small for int64.")
        return super().__new__(cls, i)


class uint64(_int):
    _min, _max = 0, (2 ** 64) - 1

    def __new__(cls, i: Union[int, int64, uint64]) -> uint64:
        if i > cls._max:
            raise ValueError(f"{i} is too large for uint64.")
        if i < cls._min:
            if isinstance(i, int64):
                i = int(i) + cls._max + 1
            else:
                raise ValueError(f"{i} is negative. Hint: cast to int64 first.")
        return super().__new__(cls, i)


def ordinal(n: int) -> str:
    """Convert an integer into its ordinal representation."""
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


def qname(val: Union[object, type]) -> str:
    if isinstance(val, type):
        return val.__qualname__
    return type(val).__qualname__


T = TypeVar("T")


# TODO: what's the type on this?
def all_subclasses(cls: Any) -> set[Any]:
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )


class TypedBox(Box, MutableMapping[str, Union[T, MutableMapping[str, T]]]):
    """TypedBox holds a collection of typed values.

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
