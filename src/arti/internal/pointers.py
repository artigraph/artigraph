from __future__ import annotations

import copy
from collections.abc import MutableMapping
from typing import Any, ClassVar, Generic, TypeVar, Union, cast

from box import Box  # type: ignore
from wrapt import ObjectProxy  # type: ignore

PointerType = TypeVar("PointerType", bound="Pointer")


class TypedProxy(ObjectProxy, Generic[PointerType]):  # type: ignore
    """ TypedProxy transparently proxies typed values, allowing stored references to be updated.

        When initialized, the sole argument must an instance of `__target_type__`, unless
        `__target_type__` has a `cast` staticmethod or classmethod that can be used to convert the
        value.
    """

    __target_type__: ClassVar[type[PointerType]]
    __wrapped__: PointerType

    @staticmethod
    def __unwrap(value: Union[PointerType, TypedProxy[PointerType]]) -> PointerType:
        while hasattr(value, "__wrapped__"):
            value = cast(Any, value).__wrapped__
        return value

    def __cast_value(self, value: Any) -> PointerType:
        # If __wrapped__ has already been assigned, we'll be more strict to ensure any newly
        # assigned instance are of the same sub-type.
        try:
            target_type = type(self.__wrapped__)
        except ValueError:
            target_type = self.__target_type__
        if isinstance(value, target_type):
            return self.__unwrap(value)
        tgt_name = target_type.__name__
        if hasattr(target_type, "cast"):
            casted = cast(Any, target_type).cast(value)
            if isinstance(casted, target_type):
                return self.__unwrap(casted)
            raise TypeError(
                f"Expected {tgt_name}.cast({value}) to return an instance of {tgt_name}, got: {casted}"
            )
        raise TypeError(f"Expected an instance of {tgt_name}, got: {value}")

    def __copy__(self) -> TypedProxy[PointerType]:
        # Normal `__copy__` doesn't copy the args, but the proxy intends to shadow direct usage of
        # the wrapped object.
        return type(self)(copy.copy(self.__wrapped__))

    def __deepcopy__(self, memo: dict[int, Any]) -> TypedProxy[PointerType]:
        return type(self)(copy.deepcopy(self.__wrapped__, memo))

    def __init__(self, value: Any):
        super().__init__(self.__cast_value(value))
        self._self_initialized = True

    def __reduce__(self) -> tuple[type[TypedProxy[PointerType]], tuple[PointerType]]:
        return type(self), (self.__wrapped__,)

    def __reduce_ex__(
        self, protocol: int
    ) -> tuple[type[TypedProxy[PointerType]], tuple[PointerType]]:
        return self.__reduce__()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.__wrapped__})"

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "__wrapped__":
            value = self.__cast_value(value)
        super().__setattr__(name, value)


_PointerProxy = TypedProxy["Pointer"]


class PointerMeta(type):
    def __call__(cls, *args: Any, **kwargs: Any) -> _PointerProxy:
        obj = super().__call__(*args, **kwargs)
        return cast(_PointerProxy, obj.__proxy_type__(obj))


class Pointer(metaclass=PointerMeta):
    __configure_subclass_proxy__: bool = True
    __proxy_type__: ClassVar[type[_PointerProxy]]
    box: ClassVar[type[PointerBox[_PointerProxy]]]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        if cls.__configure_subclass_proxy__:
            cls.__configure_subclass_proxy__ = False
            cls.__proxy_type__ = type(
                f"{cls.__name__}Proxy", (TypedProxy,), {"__target_type__": cls}
            )
            cls.box = type(
                f"{cls.__name__}Box", (PointerBox,), {"__proxy_type__": cls.__proxy_type__}
            )


class PointerBox(Box, MutableMapping[str, TypedProxy[PointerType]]):  # type: ignore
    """ PointerBox holds a collection of typed Pointers.

        Subclasses must set the __proxy_type__ to a Pointer subclass.

        Assignment to an existing key will update the Pointer's reference in place.
    """

    __proxy_type__: ClassVar[type[TypedProxy[PointerType]]]

    # NOTE: Box uses name mangling (double __) to prevent conflicts with contained values.
    def _Box__convert_and_store(self, item: str, value: TypedProxy[PointerType]) -> None:
        # TODO: Might need to handle list/tuple/... here.
        if isinstance(value, dict):
            super()._Box__convert_and_store(item, value)  # pylint: disable=no-member
        elif item in self:
            self[item].__wrapped__ = value
        else:
            super()._Box__convert_and_store(  # pylint: disable=no-member
                item,
                value if isinstance(value, self.__proxy_type__) else self.__proxy_type__(value),
            )
