from __future__ import annotations

from typing import Any

from arti.types.core import (
    Float16,
    Float32,
    Float64,
    Int32,
    Int64,
    String,
    Type,
    TypeAdapter,
    TypeSystem,
)

python = TypeSystem(key="python")


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


def _gen_adapter(*, artigraph: type[Type], system: Any, precision: int = 0) -> type[TypeAdapter]:
    return python.register_adapter(
        type(
            f"Py{artigraph.__name__}",
            (_SingletonTypeAdapter,),
            {"artigraph": artigraph, "system": system, "priority": precision},
        )
    )


_gen_adapter(artigraph=Float64, system=float, precision=64)
_gen_adapter(artigraph=Float32, system=float, precision=32)
_gen_adapter(artigraph=Float16, system=float, precision=16)
_gen_adapter(artigraph=Int64, system=int, precision=64)
_gen_adapter(artigraph=Int32, system=int, precision=32)
_gen_adapter(artigraph=String, system=str)
