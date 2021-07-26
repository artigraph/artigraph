from __future__ import annotations

from typing import Any

import arti.types.core
from arti.types.core import Type, TypeAdapter, TypeSystem

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


for precision in (16, 32, 64):
    _gen_adapter(
        artigraph=getattr(arti.types.core, f"Float{precision}"),
        system=float,
        precision=precision,
    )


for precision in (32, 64):
    _gen_adapter(
        artigraph=getattr(arti.types.core, f"Int{precision}"),
        system=int,
        precision=precision,
    )

_gen_adapter(artigraph=arti.types.core.String, system=str)
