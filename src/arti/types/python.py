from __future__ import annotations

import datetime
from functools import partial
from itertools import chain
from typing import _TypedDictMeta  # type: ignore
from typing import Any, Literal, Optional, TypedDict, Union, get_args, get_origin, get_type_hints

import arti.types
from arti.internal.type_hints import NoneType, is_optional_hint, is_union
from arti.types import Type, TypeAdapter, TypeSystem, _ScalarClassTypeAdapter

python_type_system = TypeSystem(key="python")
_generate = partial(_ScalarClassTypeAdapter.generate, type_system=python_type_system)

# NOTE: issubclass(bool, int) is True, so set higher priority
_generate(artigraph=arti.types.Boolean, system=bool, priority=int(1e9))
_generate(artigraph=arti.types.Date, system=datetime.date)
_generate(artigraph=arti.types.Null, system=NoneType)
_generate(artigraph=arti.types.String, system=str)
for _precision in (16, 32, 64):
    _generate(
        artigraph=getattr(arti.types, f"Float{_precision}"),
        system=float,
        priority=_precision,
    )
for _precision in (8, 16, 32, 64):
    _generate(
        artigraph=getattr(arti.types, f"Int{_precision}"),
        system=int,
        priority=_precision,
    )


@python_type_system.register_adapter
class PyDatetime(_ScalarClassTypeAdapter):
    artigraph = arti.types.Timestamp
    system = datetime.datetime

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        return cls.artigraph(precision="microsecond")


@python_type_system.register_adapter
class PyList(TypeAdapter):
    artigraph = arti.types.List
    system = list

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        (value_type,) = get_args(type_)
        return cls.artigraph(
            value_type=python_type_system.to_artigraph(value_type),
        )

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        return get_origin(type_) is cls.system

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        assert isinstance(type_, cls.artigraph)
        return cls.system[
            python_type_system.to_system(type_.value_type),
        ]  # type: ignore


@python_type_system.register_adapter
class PyLiteral(TypeAdapter):
    artigraph = arti.types.Enum
    system = Literal

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        origin, items = get_origin(type_), get_args(type_)
        if is_union(origin):
            assert not is_optional_hint(type_)  # Should be handled by PyOptional
            # We only support Enums currently, so all subtypes must be Literal
            if non_literals := [sub for sub in items if not get_origin(sub) is Literal]:
                raise NotImplementedError(
                    f"Only Union[Literal[...], ...] (enums) are currently supported, got invalid subtypes: {non_literals}"
                )
            # Flatten Union[Literal[1], Literal[1,2,3]]
            origin, items = Literal, tuple(chain.from_iterable(get_args(sub) for sub in items))
        assert origin is Literal
        assert isinstance(items, tuple)
        if len(items) == 0:
            raise NotImplementedError(f"Invalid Literal with no values: {type_}")
        py_type, *other_types = [type(v) for v in items]
        if not all(t is py_type for t in other_types):
            raise ValueError("All Literals must be the same type, got: {(py_type, *other_types)}")
        return cls.artigraph(
            type=python_type_system.to_artigraph(py_type),
            items=items,
        )

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        # We don't (currently) support arbitrary Unions, but can map Union[Literal[1], Literal[2]]
        # to an Enum. Python's Optional is also represented as a Union, but we handle that with the
        # high priority PyOptional.
        origin, items = get_origin(type_), get_args(type_)
        return origin is Literal or (
            is_union(origin) and all(get_origin(sub) is Literal for sub in items)
        )

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        assert isinstance(type_, cls.artigraph)
        return cls.system[tuple(type_.items)]


@python_type_system.register_adapter
class PyMap(TypeAdapter):
    artigraph = arti.types.Map
    system = dict

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        key_type, value_type = get_args(type_)
        return cls.artigraph(
            key_type=python_type_system.to_artigraph(key_type),
            value_type=python_type_system.to_artigraph(value_type),
        )

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        return get_origin(type_) is cls.system

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        assert isinstance(type_, cls.artigraph)
        return cls.system[
            python_type_system.to_system(type_.key_type),
            python_type_system.to_system(type_.value_type),
        ]  # type: ignore


@python_type_system.register_adapter
class PyOptional(TypeAdapter):
    artigraph = arti.types.Type  # Check against isinstance *and* .nullable
    system = Optional
    # Set very high priority to intercept other matching arti.types.Types/py Union in order to set .nullable
    priority = int(1e9)

    @classmethod
    def matches_artigraph(cls, type_: Type) -> bool:
        return super().matches_artigraph(type_) and type_.nullable

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        # Optional is represented as a Union; strip out NoneType before dispatching
        type_ = Union[tuple(subtype for subtype in get_args(type_) if subtype is not NoneType)]
        return python_type_system.to_artigraph(type_).copy(update={"nullable": True})

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        return is_optional_hint(type_)

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        return cls.system[python_type_system.to_system(type_.copy(update={"nullable": False}))]


@python_type_system.register_adapter
class PyStruct(TypeAdapter):
    artigraph = arti.types.Struct
    system = TypedDict

    # TODO: Support and inspect TypedDict's '__optional_keys__', '__required_keys__', '__total__'

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        return arti.types.Struct(
            name=type_.__name__,
            fields={
                field_name: python_type_system.to_artigraph(field_type)
                for field_name, field_type in get_type_hints(type_).items()
            },
        )

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        # NOTE: This check is probably a little shaky, particularly across python versions. Consider
        # using the typing_inspect package.
        return isinstance(type_, _TypedDictMeta)

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        assert isinstance(type_, cls.artigraph)
        return TypedDict(
            type_.name,
            {
                field_name: python_type_system.to_system(field_type)
                for field_name, field_type in type_.fields.items()
            },
        )  # type: ignore
