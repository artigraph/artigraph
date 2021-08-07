from __future__ import annotations

import datetime
from typing import Any, cast

import arti.types
from arti.types import Struct, Type, TypeAdapter, TypeSystem

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


for precision in (16, 32, 64):
    _gen_adapter(
        artigraph=getattr(arti.types, f"Float{precision}"),
        system=float,
        priority=precision,
    )


for precision in (32, 64):
    _gen_adapter(
        artigraph=getattr(arti.types, f"Int{precision}"),
        system=int,
        priority=precision,
    )

_gen_adapter(artigraph=arti.types.Null, system=type(None))
_gen_adapter(artigraph=arti.types.String, system=str)
_gen_adapter(artigraph=arti.types.Date, system=datetime.date)


@python_type_system.register_adapter
class PyDatetime(_SingletonTypeAdapter):
    artigraph = arti.types.Timestamp
    system = datetime.datetime

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        return cls.artigraph(precision="microsecond")


@python_type_system.register_adapter
class PyStruct(TypeAdapter):
    artigraph = arti.types.Struct
    system = dict[str, type]

    @classmethod
    def to_artigraph(cls, type_: Any) -> Type:
        return arti.types.Struct(
            fields={
                field_name: python_type_system.to_artigraph(field_type)
                for field_name, field_type in type_.items()
            }
        )

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        return isinstance(type_, dict) and all(
            [
                isinstance(field_name, str) and isinstance(field_type, type)
                for field_name, field_type in type_.items()
            ]
        )

    @classmethod
    def to_system(cls, type_: Type) -> Any:
        type_ = cast(Struct, type_)
        return {
            field_name: python_type_system.to_system(field_type)
            for field_name, field_type in type_.fields.items()
        }
