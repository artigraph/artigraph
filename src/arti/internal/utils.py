from __future__ import annotations

import importlib
import inspect
import pkgutil
import threading
from collections.abc import Callable, Iterable
from types import ModuleType
from typing import Any, Self, cast, overload


class ClassName:
    def __get__(self, obj: Any, type_: type[Any]) -> str:
        return type_.__name__


class_name = cast(Callable[[], str], ClassName)


class ClassProperty[Ret]:
    def __init__(self, meth: Callable[[Any], Ret]) -> None:
        self.meth = meth

    def __get__(self, obj: Any, type_: type[Any] | None = None) -> Ret:
        if type_ is None:
            type_ = cast(type[Any], type(obj))
        return self.meth(type_)


def classproperty[Ret](meth: Callable[[Any], Ret]) -> Ret:
    """Access a class-level method like a property."""
    return cast(Ret, ClassProperty(meth))


def get_module_name(depth: int = 1) -> str | None:
    """
    Return the module name of a specific level in the stack.

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
    """
    Import the direct submodules of a package.

    This can be useful with registry patterns to automatically discover and import submodules
    defining additional implementations.

    `path` and `name` are usually provided from an existing module's `__path__` and `__name__`.

    This function is thread-safe and supports namespace packages. Subpackages are not recursed into.

    NOTE: This inherently triggers eager imports, which has performance impacts and may cause import
    cycles. To reduce these issues, avoid calling during module definition.
    """
    with lock:
        return {
            module.name: importlib.import_module(module.name)
            for module in pkgutil.iter_modules(path, prefix=f"{name}.")
        }


@overload
def one_or_none[V](values: list[V] | None, *, default: None = None, item_name: str) -> V | None: ...


@overload
def one_or_none[V, D](values: list[V] | None, *, default: D, item_name: str) -> V | D: ...


def one_or_none[V, D](
    values: list[V] | None, *, default: D | None = None, item_name: str
) -> V | D | None:
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
    """
    Mixin to bypass (deep)copying.

    This is useful for objects that are *intended* to be stateful and preserved, despite usually
    preferring immutable data structures and Pydantic models, which (deep)copy often.
    """

    def __copy__(self) -> Self:
        return self  # pragma: no cover

    def __deepcopy__(self, memo: Any) -> Self:
        return self  # pragma: no cover


class NoCopyDict[K, V](dict[K, V], NoCopyMixin):
    pass
