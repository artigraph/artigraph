from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Optional, TypeVar, cast, overload

import multimethod as _multimethod  # Minimize name confusion

from arti.internal.type_hints import lenient_issubclass, tidy_signature

RETURN = TypeVar("RETURN")
REGISTERED = TypeVar("REGISTERED", bound=Callable[..., Any])


# This may be less useful once mypy supports ParamSpecs - after that, we might be able to define
# multidispatch with a ParamSpec and have mypy check the handlers' arguments are covariant.
class _multipledispatch(_multimethod.multidispatch[RETURN]):
    """Multiple dispatch for a set of functions based on parameter type.

    Usage is similar to `@functools.singledispatch`. The original definition defines the "spec" that
    subsequent handlers must follow, namely the name and (base)class of parameters.
    """

    # NOTE: We can't add extra (kw)args without also overriding __new__. However, `__new__` is
    # called for each *registered* func in the multimethod  internals (a bit confusing). Instead, we
    # can just set attrs in a helper func below.
    def __init__(self, func: Callable[..., RETURN]) -> None:
        super().__init__(func)
        self.canonical_name: Optional[str] = None
        self.discovery_func: Optional[Callable[[], None]] = None
        assert self.signature is not None
        self.clean_signature = tidy_signature(func, self.signature)

    def __missing__(self, types: tuple[Any, ...]) -> Callable[..., RETURN]:
        if self.discovery_func is not None:
            self.discovery_func()
        return super().__missing__(types)

    def lookup(self, *args: Optional[type[Any]]) -> Callable[..., Any]:
        # multimethod wraps Generics (eg: `list[int]`) with an internal helper. We must do the same
        # before looking up. Non-Generics pass through as is.
        args = tuple(_multimethod.subtype(arg) for arg in args)  # type: ignore[no-untyped-call]
        # NOTE: multimethod doesn't override __contains__ (likely so __missing__ will still run), so
        # "args in self" will be False when using subclasses of any arg.
        missing_error = ValueError(f"No `{self.canonical_name}` implementation found for: {args}")
        try:
            handler = cast(Callable[..., Any], self[args])
        # multimethod raises a TypeError instead of KeyError, as __call__.
        except TypeError as e:  # pragma: no cover
            raise missing_error from e
        # Filter out the base "NotImplementedError" handler.
        if getattr(handler, "_abstract_", False):
            raise missing_error
        return handler

    @overload
    def register(self, __func: REGISTERED) -> REGISTERED: ...

    @overload
    def register(self, *args: type) -> Callable[[REGISTERED], REGISTERED]: ...

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
            if not lenient_issubclass(sig.return_annotation, spec.return_annotation):
                raise TypeError(
                    f"Expected the `{func.__name__}` return to match {spec.return_annotation}, got {sig.return_annotation}"
                )
        return cast(Callable[..., Any], super().register(*args))


def multipledispatch(
    canonical_name: str, *, discovery_func: Optional[Callable[[], None]] = None
) -> Callable[[Callable[..., RETURN]], _multipledispatch[RETURN]]:
    def wrap(func: Callable[..., RETURN]) -> _multipledispatch[RETURN]:
        # The base handler is expected to `raise NotImplementedError`
        func._abstract_ = True  # type: ignore[attr-defined]
        dispatch = _multipledispatch(func)
        dispatch.canonical_name = canonical_name
        dispatch.discovery_func = discovery_func
        return dispatch

    return wrap
