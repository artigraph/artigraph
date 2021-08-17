from __future__ import annotations

import inspect
import sys
import types
from collections.abc import Callable
from typing import _GenericAlias  # type: ignore
from typing import Any, Union, get_args, get_origin


def lenient_issubclass(klass: Any, class_or_tuple: Union[type[Any], tuple[type[Any], ...]]) -> bool:
    try:
        return isinstance(klass, type) and issubclass(klass, class_or_tuple)
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

    def is_union(tp: Any) -> bool:
        return tp is Union


else:  # pragma: no cover

    def is_union(tp: Any) -> bool:
        # `Union[int, str]` or `int | str`
        return tp is Union or tp is types.Union
