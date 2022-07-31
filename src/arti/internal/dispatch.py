import inspect
from collections.abc import Callable
from typing import Any, TypeVar, overload

from multimethod import multidispatch as _multidispatch  # Minimize name confusion

from arti.internal.type_hints import lenient_issubclass, tidy_signature

RETURN = TypeVar("RETURN")
REGISTERED = TypeVar("REGISTERED", bound=Callable[..., Any])


# This may be less useful once mypy supports ParamSpecs - after that, we might be able to define
# multidispatch with a ParamSpec and have mypy check the handlers' arguments are covariant.
class multipledispatch(_multidispatch[RETURN]):
    """Multiple dispatch for a set of functions based on parameter type.

    Usage is similar to `@functools.singledispatch`. The original definition defines the "spec" that
    subsequent handlers must follow, namely the name and (base)class of parameters.
    """

    def __init__(self, func: Callable[..., RETURN]) -> None:
        super().__init__(func)
        self.clean_signature = tidy_signature(func, self.signature)

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
            if not lenient_issubclass(sig.return_annotation, spec.return_annotation):
                raise TypeError(
                    f"Expected the `{func.__name__}` return to match {spec.return_annotation}, got {sig.return_annotation}"
                )
        return super().register(*args)  # type: ignore
