from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, TypeVar, cast, overload

import multimethod as _multimethod  # Minimize name confusion

from arti.internal.type_hints import lenient_issubclass, tidy_signature

RETURN = TypeVar("RETURN")
REGISTERED = TypeVar("REGISTERED", bound=Callable[..., Any])


# The dynamic multimethod implementation cannot currently express the handlers' covariance.
class _multipledispatch(_multimethod.multidispatch[RETURN]):
    """
    Multiple dispatch for a set of functions based on parameter type.

    Usage is similar to `@functools.singledispatch`. The original definition defines the "spec" that
    subsequent handlers must follow, namely the name and (base)class of parameters.
    """

    # NOTE: We can't add extra (kw)args without also overriding __new__. However, `__new__` is
    # called for each *registered* func in the multimethod  internals (a bit confusing). Instead, we
    # can just set attrs in a helper func below.
    def __init__(self, func: Callable[..., RETURN]) -> None:
        # multimethod 2 registers the initial multidispatch function as an untyped fallback.
        # Register it through the typed base class to preserve our strict annotated dispatch.
        self.pending = set()
        self.generics = []
        self.signatures = {}
        base_init = cast(
            Callable[[Any, Callable[..., RETURN]], None],
            getattr(_multimethod.multimethod, "__init__"),  # noqa: B009
        )
        base_init(self, func)
        self.canonical_name: str | None = None
        self.discovery_func: Callable[[], None] | None = None
        self.clean_signature = tidy_signature(func, inspect.signature(func))

    def __missing__(self, types: tuple[Any, ...]) -> Callable[..., RETURN]:
        if self.discovery_func is not None:
            self.discovery_func()
        missing = cast(
            Callable[[tuple[Any, ...]], Callable[..., RETURN]],
            getattr(super(), "__missing__"),  # noqa: B009
        )
        return missing(types)

    def dispatch(self, *args: Any) -> Callable[..., RETURN]:
        # multimethod 2 resolves generic parameters (eg: `Sequence[...]`) with instance checks that
        # bypass `__missing__`, so discover implementations before every dispatch.
        if self.discovery_func is not None:
            self.discovery_func()
        dispatch = cast(
            Callable[..., Callable[..., RETURN]],
            getattr(super(), "dispatch"),  # noqa: B009
        )
        return dispatch(*args)

    def lookup(self, *args: type[Any] | None) -> Callable[..., Any]:
        # multimethod wraps Generics (eg: `list[int]`) with an internal helper. We must do the same
        # before looking up. Non-Generics pass through as is.
        subtype = cast(Callable[[type[Any] | None], Any], _multimethod.subtype)
        args = tuple(subtype(arg) for arg in args)
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
    canonical_name: str, *, discovery_func: Callable[[], None] | None = None
) -> Callable[[Callable[..., RETURN]], _multipledispatch[RETURN]]:
    def wrap(func: Callable[..., RETURN]) -> _multipledispatch[RETURN]:
        # The base handler is expected to `raise NotImplementedError`
        object.__setattr__(func, "_abstract_", True)
        dispatch = cast(_multipledispatch[RETURN], _multipledispatch(func))
        dispatch.canonical_name = canonical_name
        dispatch.discovery_func = discovery_func
        return dispatch

    return wrap
