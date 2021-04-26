from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, get_args, get_origin


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
            if get_origin(sig.return_annotation) is tuple
            else (
                sig.return_annotation
                if sig.return_annotation is sig.empty
                else (sig.return_annotation,)
            )
        ),
    )
    return sig
