from __future__ import annotations

import inspect
import sys
import types
from collections.abc import Callable
from typing import _GenericAlias  # type: ignore
from typing import Any, Union, cast, get_args, get_origin

NoneType = cast(type, type(None))  # mypy otherwise treats type(None) as an object


def lenient_issubclass(klass: Any, class_or_tuple: Union[type, tuple[type, ...]]) -> bool:
    if not isinstance(klass, type):
        return False
    if isinstance(class_or_tuple, tuple):
        return any(lenient_issubclass(klass, subtype) for subtype in class_or_tuple)
    check_type = class_or_tuple
    if is_union_hint(check_type):
        return any(lenient_issubclass(klass, subtype) for subtype in get_args(check_type))
    try:
        return issubclass(klass, check_type)
    except TypeError:
        if isinstance(klass, (types.GenericAlias, _GenericAlias)):
            return False
        raise


def signature(fn: Callable[..., Any], *, follow_wrapped: bool = True) -> inspect.Signature:
    """Convenience wrapper around `inspect.signature`.

    The returned Signature will have `cls`/`self` parameters removed and
    `tuple[...]` converted to `tuple(...)` in the `return_annotation`.
    """
    sig = inspect.signature(fn, follow_wrapped=follow_wrapped)
    sig = sig.replace(
        parameters=[p for p in sig.parameters.values() if p.name not in ("cls", "self")],
        return_annotation=(
            get_args(sig.return_annotation)
            if lenient_issubclass(get_origin(sig.return_annotation), tuple)
            else (
                sig.return_annotation
                if sig.return_annotation is sig.empty
                else (sig.return_annotation,)
            )
        ),
    )
    return sig


#############################################
# Helpers for typing across python versions #
#############################################
#
# Focusing on  3.9+ (for now)


if sys.version_info < (3, 10):

    def is_union(type_: Any) -> bool:
        return type_ is Union


else:  # pragma: no cover

    def is_union(type_: Any) -> bool:
        # `Union[int, str]` or `int | str`
        return type_ is Union or type_ is types.Union


def is_optional_hint(type_: Any) -> bool:
    # Optional[x] is represented as Union[x, NoneType]
    return is_union(get_origin(type_)) and NoneType in get_args(type_)


def is_union_hint(type_: Any) -> bool:
    return get_origin(type_) is Union
