from __future__ import annotations

from typing import Any

from arti.types.core import Int64, Type, TypeAdapter, TypeSystem

python = TypeSystem(key="python", system_metaclass=True)


@python.register_adapter
class PyInt64(TypeAdapter):
    external = int
    internal = Int64

    @classmethod
    def to_external(cls, type_: Type) -> Any:
        return cls.external

    @classmethod
    def to_internal(cls, type_: Any) -> Type:
        return cls.internal()
