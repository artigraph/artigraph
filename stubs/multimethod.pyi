# Pending https://github.com/coady/multimethod/pull/37 release

from inspect import Signature
from typing import Any, Callable, Tuple, TypeVar, overload

RETURN = TypeVar("RETURN")
REGISTERED = TypeVar("REGISTERED", bound=Callable[..., Any])

class multimethod(dict[Tuple[type, ...], Callable[..., Any]]):
    signature: Signature

    # def register(self, *args: REGISTERED) -> REGISTERED: ...
    @overload
    def register(self, __func: REGISTERED) -> REGISTERED: ...
    @overload
    def register(self, *args: type) -> Callable[[REGISTERED], REGISTERED]: ...

class multidispatch(multimethod, dict[Tuple[type, ...], Callable[..., RETURN]]):
    def __call__(self, *args: Any, **kwargs: Any) -> RETURN: ...
    def __init__(self, func: Callable[..., RETURN]) -> None: ...
    def __new__(cls, func: Callable[..., RETURN]) -> multidispatch[RETURN]: ...
