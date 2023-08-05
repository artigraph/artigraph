from __future__ import annotations

import importlib
import inspect
import pkgutil
import threading
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import GenericAlias, ModuleType
from typing import IO, Any, ClassVar, Optional, SupportsIndex, TypeVar, Union, cast

from box import Box

from arti.internal.type_hints import Self
from arti.internal.vendored.setuptools import find_namespace_packages

_K = TypeVar("_K")
_V = TypeVar("_V")


class ClassName:
    def __get__(self, obj: Any, type_: type[Any]) -> str:
        return type_.__name__


class_name = cast(Callable[[], str], ClassName)


PropReturn = TypeVar("PropReturn")


def classproperty(meth: Callable[..., PropReturn]) -> PropReturn:
    """Access a @classmethod like a @property."""
    # mypy doesn't understand class properties yet: https://github.com/python/mypy/issues/2563
    return classmethod(property(meth))  # type: ignore[arg-type,return-value]


# NOTE: When subclassing `frozendict`, use a private name with unbound TypeVars (ie: `class
# _X(frozendict[_K, _V])`) and then bind the desired types with an alias (eg: `X = _X[str, int]`).
# See arti.partitions.PartitionKey for an example.
#
# This is required for Pydantic v1 to identify it as Mapping (and thus validate key/value types)
# rather than a custom type. The reason is that binding type vars returns a `types.GenericAlias`
# instance (with the class reference and bound args), but subclassing that directly obscures this
# from pydantic. A custom type requires `arbitrary_types_allowed` or a validator. However, adding a
# mapping-like validator requires reimplementing a lot of the key/value validation until v2[1].
# Hence, we'll work around this by keeping the original class generic (with type vars) and using the
# subscripted alias within models.
#
# When upgrading to pydantic v2, we can replace these patterns with a `__get_pydantic_core_schema__`
# method on `frozendict` that will cover all subclasses. The method can do something similar to [1],
# but instead use our `get_class_type_vars` helper to identify the bound type vars from a base
# class.
#
#     @classmethod
#     def __get_pydantic_core_schema__(
#         cls, source_type: Any, handler: GetCoreSchemaHandler
#     ) -> CoreSchema:
#         return core_schema.no_info_after_validator_function(
#             cls, handler(Mapping[get_class_type_vars(source_type)])  # type: ignore[misc]
#         )
#
# 1: https://github.com/pydantic/pydantic/issues/1457#issuecomment-1566287896
class frozendict(Mapping[_K, _V]):
    def __init__(
        self, arg: Union[Mapping[_K, _V], Iterable[tuple[_K, _V]]] = (), **kwargs: _V
    ) -> None:
        self._data = dict[_K, _V](arg, **kwargs)
        # Eagerly evaluate the hash to confirm elements are also frozen (via frozenset) at
        # creation time, not just when hashed.
        self._hash = hash(frozenset(self._data.items()))

    def __getitem__(self, key: _K) -> _V:
        return self._data[key]

    def __hash__(self) -> int:
        return self._hash

    def __iter__(self) -> Iterator[_K]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __or__(self, other: Mapping[_K, _V]) -> frozendict[_K, _V]:
        return type(self)({**self, **other})

    __ror__ = __or__

    def __repr__(self) -> str:
        return repr(self._data)


def get_module_name(depth: int = 1) -> Optional[str]:
    """Return the module name of a specific level in the stack.

    Depth describes how many levels to traverse, for example:
    - depth=0: return get_module_name's module
    - depth=1 (default): return the caller's module
    - depth=2: return the caller's calling module
    - ...
    """
    frame = inspect.currentframe()
    if frame is None:  # the interpreter doesn't support frame inspection
        return None  # pragma: no cover
    for _ in range(depth):
        frame = frame.f_back
        if frame is None:
            return None
    return frame.f_globals.get("__name__", "__main__")


def import_submodules(
    path: list[str],  # module.__path__ is a list[str]
    name: str,
    *,
    lock: threading.Lock = threading.Lock(),
) -> dict[str, ModuleType]:
    """Recursively import submodules.

    This can be useful with registry patterns to automatically discover and import submodules
    defining additional implementations.

    `path` and `name` are usually provided from an existing module's `__path__` and `__name__`.

    This function is thread-safe and supports namespace modules.

    NOTE: This inherently triggers eager imports, which has performance impacts and may cause import
    cycles. To reduce these issues, avoid calling during module definition.
    """
    # pkgutil.iter_modules is not recursive and pkgutil.walk_packages does not handle namespace
    # packages... however we can leverage setuptools.find_namespace_packages, which was built for
    # exactly this.
    path_names = {p: name for p in path}
    path_names.update(
        {
            str(Path(path).joinpath(*name.split("."))): f"{root_name}.{name}"
            for path, root_name in path_names.items()
            for name in find_namespace_packages(path)
        }
    )
    with lock:
        return {
            name: importlib.import_module(name)
            for path, name in path_names.items()
            for _, name, _ in pkgutil.iter_modules([path], prefix=f"{name}.")
        }


class _int(int):
    def __repr__(self) -> str:
        return f"{qname(self)}({int(self)})"

    def __str__(self) -> str:
        return str(int(self))

    # Stock magics.

    def __add__(self, x: int) -> Self:
        return type(self)(super().__add__(x))

    def __and__(self, n: int) -> Self:
        return type(self)(super().__and__(n))

    def __ceil__(self) -> Self:
        return type(self)(super().__ceil__())

    def __floor__(self) -> Self:
        return type(self)(super().__floor__())

    def __floordiv__(self, x: int) -> Self:
        return type(self)(super().__floordiv__(x))

    def __invert__(self) -> Self:
        return type(self)(super().__invert__())

    def __lshift__(self, n: int) -> Self:
        return type(self)(super().__lshift__(n))

    def __mod__(self, x: int) -> Self:
        return type(self)(super().__mod__(x))

    def __mul__(self, x: int) -> Self:
        return type(self)(super().__mul__(x))

    def __neg__(self) -> Self:
        return type(self)(super().__neg__())

    def __or__(self, n: int) -> Self:
        return type(self)(super().__or__(n))

    def __pos__(self) -> Self:
        return type(self)(super().__pos__())

    def __radd__(self, x: int) -> Self:
        return type(self)(super().__radd__(x))

    def __rand__(self, n: int) -> Self:
        return type(self)(super().__rand__(n))

    def __rfloordiv__(self, x: int) -> Self:
        return type(self)(super().__rfloordiv__(x))

    def __rlshift__(self, n: int) -> Self:
        return type(self)(super().__rlshift__(n))

    def __rmod__(self, x: int) -> Self:
        return type(self)(super().__rmod__(x))

    def __rmul__(self, x: int) -> Self:
        return type(self)(super().__rmul__(x))

    def __ror__(self, n: int) -> Self:
        return type(self)(super().__ror__(n))

    def __round__(self, ndigits: SupportsIndex = 0) -> Self:
        return type(self)(super().__round__(ndigits))

    def __rrshift__(self, n: int) -> Self:
        return type(self)(super().__rrshift__(n))

    def __rshift__(self, n: int) -> Self:
        return type(self)(super().__rshift__(n))

    def __rsub__(self, x: int) -> Self:
        return type(self)(super().__rsub__(x))

    def __rxor__(self, n: int) -> Self:
        return type(self)(super().__rxor__(n))

    def __sub__(self, x: int) -> Self:
        return type(self)(super().__sub__(x))

    def __trunc__(self) -> Self:
        return type(self)(super().__trunc__())

    def __xor__(self, n: int) -> Self:
        return type(self)(super().__xor__(n))


class int64(_int):
    _min, _max = -(2**63), (2**63) - 1

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
    _min, _max = 0, (2**64) - 1

    def __new__(cls, i: Union[int, int64, uint64]) -> uint64:
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
    with TemporaryDirectory() as d, (Path(d) / "contents").open(mode=mode) as f:
        yield f


def one_or_none(values: Optional[list[_V]], *, item_name: str) -> Optional[_V]:
    if values is None or len(values) == 0:
        return None
    if len(values) > 1:
        raise ValueError(f"multiple {item_name} values found: {values}")
    return values[0]


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


class NoCopyMixin:
    """Mixin to bypass (deep)copying.

    This is useful for objects that are *intended* to be stateful and preserved, despite usually
    preferring immutable data structures and Pydantic models, which (deep)copy often.
    """

    def __copy__(self) -> Self:
        return self  # pragma: no cover

    def __deepcopy__(self, memo: Any) -> Self:
        return self  # pragma: no cover


class NoCopyDict(dict[_K, _V], NoCopyMixin):
    pass


class TypedBox(Box, MutableMapping[str, Union[_V, MutableMapping[str, _V]]]):
    """TypedBox holds a collection of typed values.

    Subclasses must set the __target_type__ to a base class for the contained values.
    """

    __target_type__: ClassVar[type[_V]]  # type: ignore[misc]

    @classmethod
    def __class_getitem__(cls, item: type[_V]) -> GenericAlias:
        if isinstance(item, tuple):
            raise TypeError(f"{cls.__name__} expects a single value type")
        value_type = item
        return GenericAlias(
            type(
                cls.__name__,
                (cls,),
                {
                    "__module__": get_module_name(depth=2),  # Set to our caller's module
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
        return None

    def __cast_value(self, item: str, value: Any) -> _V:
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
            super()._Box__convert_and_store(item, self.__cast_value(item, value))

    def walk(self, root: tuple[str, ...] = ()) -> Iterator[tuple[str, _V]]:
        for k, v in self.items():
            subroot = (*root, k)
            if isinstance(v, TypedBox):
                yield from v.walk(root=subroot)
            else:
                yield ".".join(subroot), v  # type: ignore[misc]
