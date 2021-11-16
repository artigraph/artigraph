import inspect
import os.path
from collections.abc import Callable, Generator, MutableMapping
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from types import GenericAlias
from typing import IO, Any, ClassVar, Generic, Optional, TypeVar, Union, cast, overload

from box import Box
from frozendict.core import frozendict as _frozendict
from multimethod import multidispatch

from arti.internal.type_hints import lenient_issubclass, tidy_signature

_K = TypeVar("_K")
_V = TypeVar("_V")


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


RETURN = TypeVar("RETURN")
REGISTERED = TypeVar("REGISTERED", bound=Callable[..., Any])


# This may be less useful once mypy supports ParamSpecs - after that, we might be able to define
# multidispatch with a ParamSpec and have mypy check the handlers' arguments are covariant.
class dispatch(multidispatch[RETURN]):
    """Multiple dispatch for a set of functions based on parameter type.

    Usage is similar to `@functools.singledispatch`. The original definition defines the "spec" that
    subsequent handlers must follow, namely the name and (base)class of parameters.
    """

    def __init__(self, func: Callable[..., RETURN]) -> None:
        super().__init__(func)
        self.clean_signature = tidy_signature(func, self.signature)
        if self.clean_signature.return_annotation not in (Any, type(None)):
            raise NotImplementedError("Return type checking is not implemented yet!")

    @overload
    def register(self, __func: REGISTERED) -> REGISTERED:
        ...

    @overload
    def register(self, *args: type) -> Callable[[REGISTERED], REGISTERED]:
        ...

    def register(self, *args: Any) -> Callable[[REGISTERED], REGISTERED]:
        if len(args) == 1 and hasattr(args[0], "__annotations__"):
            func = args[0]
            sig = tidy_signature(func, inspect.signature(func))
            spec = self.clean_signature
            if set(sig.parameters) != set(spec.parameters):
                raise TypeError(
                    f"Expected `{func.__name__}` to have {sorted(set(spec.parameters))} parameters, got {sorted(set(sig.parameters))}"
                )
            for name in sig.parameters:
                sig_param, spec_param = sig.parameters[name], spec.parameters[name]
                if sig_param.kind != spec_param.kind:
                    raise TypeError(
                        f"Expected the `{func.__name__}.{name}` parameter to be {spec_param.kind}, got {sig_param.kind}"
                    )
                if sig_param.annotation is not Any and not lenient_issubclass(
                    sig_param.annotation, spec_param.annotation
                ):
                    raise TypeError(
                        f"Expected the `{func.__name__}.{name}` parameter to be a subclass of {spec_param.annotation}, got {sig_param.annotation}"
                    )
        return super().register(*args)  # type: ignore


# frozendict is useful for models to preserve hashability (eg: key in a dict). Unfortunately, there
# is 1 small pydantic issue[1] and an upstream issue with types.GenericAlias + deepcopy[2,3]. We can
# work around the blocking deepcopy issue by replacing types.GenericAlias with typing.Generic (which
# returns a typing._GenericAlias). The pydantic bug (which returns a dict instead of a frozendict)
# is worked around in arti.internal.patches.patch_pydantic_ModelField__type_analysis.
#
# 1: https://github.com/samuelcolvin/pydantic/pull/3138
# 2: https://github.com/Marco-Sulla/python-frozendict/issues/29
# 3: https://bugs.python.org/issue45167
class frozendict(Generic[_K, _V], _frozendict[_K, _V]):
    pass


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

    def __new__(cls, i: Union[int, "int64", "uint64"]) -> "int64":
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

    def __new__(cls, i: Union[int, int64, "uint64"]) -> "uint64":
        if i > cls._max:
            raise ValueError(f"{i} is too large for uint64.")
        if i < cls._min:
            if isinstance(i, int64):
                i = int(i) + cls._max + 1
            else:
                raise ValueError(f"{i} is negative. Hint: cast to int64 first.")
        return super().__new__(cls, i)


@contextmanager
def named_temporary_file(mode: str = "w+b") -> Generator[IO[Any], None, None]:
    """Minimal alternative to tempfile.NamedTemporaryFile that can be re-opened on Windows."""
    with TemporaryDirectory() as d:
        with open(os.path.join(d, "contents"), mode=mode) as f:
            yield f


def ordinal(n: int) -> str:
    """Convert an integer into its ordinal representation."""
    n = int(n)
    suffix = ["th", "st", "nd", "rd", "th"][min(n % 10, 4)]
    if 11 <= (n % 100) <= 13:
        suffix = "th"
    return str(n) + suffix


def register(
    registry: dict[_K, _V],
    key: _K,
    value: _V,
    get_priority: Optional[Callable[[_V], int]] = None,
) -> _V:
    if key in registry:
        existing = registry[key]
        if get_priority is None:
            raise ValueError(f"{key} is already registered with: {existing}!")
        existing_priority, new_priority = get_priority(existing), get_priority(value)
        if existing_priority > new_priority:
            return value
        if existing_priority == new_priority:
            raise ValueError(
                f"{key} with matching priority ({existing_priority}) is already registered with: {existing}!"
            )
    registry[key] = value
    return value


def qname(val: Union[object, type]) -> str:
    if isinstance(val, type):
        return val.__qualname__
    return type(val).__qualname__


class TypedBox(Box, MutableMapping[_K, Union[_V, MutableMapping[_K, _V]]]):
    """TypedBox holds a collection of typed values.

    Subclasses must set the __target_type__ to a base class for the contained values.
    """

    __target_type__: ClassVar[type[_V]]

    @classmethod
    def __class_getitem__(cls, item: tuple[type[_K], type[_V]]) -> GenericAlias:
        if not isinstance(item, tuple) or len(item) != 2:
            raise TypeError(f"{cls.__name__} expects a key and value type")
        key_type, value_type = item
        if key_type is not str:
            raise TypeError(f"{cls.__name__} key must be `str`")
        return GenericAlias(
            type(
                cls.__name__,
                (cls,),
                {
                    "__module__": __name__,
                    "__target_type__": value_type,
                },
            ),
            item,
        )

    def __setattr__(self, key: str, value: Any) -> None:
        # GenericAlias sets __orig_class__ after __init__, so preempt Box from storing that (or
        # erroring if frozen).
        if key == "__orig_class__":
            return object.__setattr__(self, key, value)
        super().__setattr__(key, value)

    def __cast_value(self, value: Any) -> _V:
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
    def _Box__convert_and_store(self, item: str, value: _V) -> None:
        if isinstance(value, dict):
            super()._Box__convert_and_store(item, value)  # pylint: disable=no-member
        elif item in self:
            raise ValueError(f"{item} is already set!")
        else:
            super()._Box__convert_and_store(item, self.__cast_value(value))
