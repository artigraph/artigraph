from __future__ import annotations

import datetime
from typing import _TypedDictMeta  # type: ignore
from typing import Any, TypedDict, get_args, get_origin, get_type_hints

import arti.types
from arti.internal.type_hints import NoneType
from arti.types import Type, TypeAdapter, TypeSystem

python_type_system = TypeSystem(key="python")


# Python types are all instances of `type` - there is no "meaningful" metaclass. Hence, we must
# match on and return the type identity.
class _SingletonTypeAdapter(TypeAdapter):
    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        return cls.artigraph()

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        return type_ is cls.system

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        return cls.system


def _gen_adapter(*, artigraph: type[Type], system: Any, priority: int = 0) -> type[TypeAdapter]:
    return python_type_system.register_adapter(
        type(
            f"Py{artigraph.__name__}",
            (_SingletonTypeAdapter,),
            {"artigraph": artigraph, "system": system, "priority": priority},
        )
    )


_gen_adapter(artigraph=arti.types.Boolean, system=bool)
_gen_adapter(artigraph=arti.types.Date, system=datetime.date)
_gen_adapter(artigraph=arti.types.Null, system=NoneType)
_gen_adapter(artigraph=arti.types.String, system=str)
for _precision in (16, 32, 64):
    _gen_adapter(
        artigraph=getattr(arti.types, f"Float{_precision}"),
        system=float,
        priority=_precision,
    )
for _precision in (8, 16, 32, 64):
    _gen_adapter(
        artigraph=getattr(arti.types, f"Int{_precision}"),
        system=int,
        priority=_precision,
    )


@python_type_system.register_adapter
class PyDatetime(_SingletonTypeAdapter):
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
