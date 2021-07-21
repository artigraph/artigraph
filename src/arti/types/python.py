from __future__ import annotations

from typing import Any

from arti.types.core import Int64, Type, TypeAdapter, TypeSystem

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


def _gen_adapter(name: str, *, artigraph: type[Type], system: Any) -> type[TypeAdapter]:
    return python.register_adapter(
        type(name, (_SingletonTypeAdapter,), {"artigraph": artigraph, "system": system})
    )


_gen_adapter("PyInt64", artigraph=Int64, system=int)
