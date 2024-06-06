from __future__ import annotations

import importlib
import inspect
import pkgutil
import threading
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import IO, Any, Self, SupportsIndex, cast

from arti.internal.vendored.setuptools import find_namespace_packages


class ClassName:
    def __get__(self, obj: Any, type_: type[Any]) -> str:
        return type_.__name__


class_name = cast(Callable[[], str], ClassName)


def classproperty[Ret](meth: Callable[..., Ret]) -> Ret:
    """Access a @classmethod like a @property."""
    # mypy doesn't understand class properties yet: https://github.com/python/mypy/issues/2563
    return classmethod(property(meth))  # type: ignore[arg-type,return-value]


def get_module_name(depth: int = 1) -> str | None:
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
    return cast(str, frame.f_globals.get("__name__"))


def import_submodules(
    path: Iterable[str],  # module.__path__ is a list[str]
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
        return f"{type(self).__name__}({int(self)})"

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

    def __new__(cls, i: int | int64 | uint64) -> int64:
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

    def __new__(cls, i: int | int64 | uint64) -> uint64:
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


def one_or_none[V, D](values: list[V] | None, *, default: D = None, item_name: str) -> V | D:
    if values is None or len(values) == 0:
        return default
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


def register[K, V](
    registry: dict[K, V], key: K, value: V, get_priority: Callable[[V], int] | None = None
) -> V:
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


class NoCopyMixin:
    """Mixin to bypass (deep)copying.

    This is useful for objects that are *intended* to be stateful and preserved, despite usually
    preferring immutable data structures and Pydantic models, which (deep)copy often.
    """

    def __copy__(self) -> Self:
        return self  # pragma: no cover

    def __deepcopy__(self, memo: Any) -> Self:
        return self  # pragma: no cover


class NoCopyDict[K, V](dict[K, V], NoCopyMixin):
    pass
