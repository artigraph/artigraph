from __future__ import annotations

from functools import partial
from typing import Any, cast

import numpy as np

import arti.types
from arti.types import (
    Binary,
    Boolean,
    List,
    String,
    Type,
    TypeAdapter,
    TypeSystem,
    _ScalarClassTypeAdapter,
)

# NOTE: TypeAdapters for some types may still be missing. Please open an issue or PR if you find
# anything missing.
#
# TODO: Handle compound/structured dtypes and recarray
# - This page will likely be helpful: https://numpy.org/doc/stable/reference/arrays.dtypes.html#arrays-dtypes

numpy_type_system = TypeSystem(key="numpy")


class _NumpyScalarTypeAdapter(_ScalarClassTypeAdapter):
    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        if isinstance(type_, np.ndarray):
            return False
        # NOTE: this works for both direct type and np.dtype comparison, eg:
        # - np.bool_ == np.bool_
        # - np.dtype("bool") == np.bool_
        return cast(bool, type_ == cls.system)


_generate = partial(_NumpyScalarTypeAdapter.generate, type_system=numpy_type_system)

_generate(artigraph=Binary, system=np.bytes_)
_generate(artigraph=Boolean, system=np.bool_)
_generate(artigraph=String, system=np.str_)
for _precision in (16, 32, 64):
    _generate(
        artigraph=getattr(arti.types, f"Float{_precision}"),
        system=getattr(np, f"float{_precision}"),
        priority=_precision,
    )
for _precision in (8, 16, 32, 64):
    _generate(
        artigraph=getattr(arti.types, f"Int{_precision}"),
        system=getattr(np, f"int{_precision}"),
        priority=_precision,
    )
    _generate(
        artigraph=getattr(arti.types, f"UInt{_precision}"),
        system=getattr(np, f"uint{_precision}"),
        priority=_precision,
    )


@numpy_type_system.register_adapter
class ArrayAdapter(TypeAdapter):
    artigraph = List
    system = np.ndarray

    # NOTE: np.ndarray now supports TypeVars, eg: `np.ndarray[Any, np.dtype[np.float64]]`
    #
    # We may consider supporting that form in addition to an empty value, but it doesn't yet support
    # specifying the shape/ndim.

    @classmethod
    def to_artigraph(
        cls, type_: np.ndarray[Any, Any], *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        value = type_[0] if isinstance(type_[0], np.ndarray) else type(type_[0])
        return cls.artigraph(element=type_system.to_artigraph(value, hints=hints))

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return isinstance(type_, np.ndarray)

    @classmethod
    def to_system(
        cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> np.ndarray[Any, Any]:
        assert isinstance(type_, List)
        element_type = type_system.to_system(type_.element, hints=hints)
        # scalar numpy dtypes can be instantiated to return a zero value, like the python types
        value = element_type if isinstance(element_type, np.ndarray) else element_type()
        return np.array([value])
