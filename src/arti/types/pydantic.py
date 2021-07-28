from typing import Any

import pydantic
from pydantic.fields import ModelField

from arti.internal.type_hints import lenient_issubclass
from arti.types import Struct, Type, TypeAdapter, TypeSystem
from arti.types.python import python_type_system

pydantic_type_system = TypeSystem(key="pydantic")


@pydantic_type_system.register_adapter
class BaseModelAdapter(TypeAdapter):
    artigraph = Struct
    system = pydantic.BaseModel

    @classmethod
    def to_artigraph(cls, type_: type[pydantic.BaseModel]) -> Type:
        def _field_type_to_artigraph(field: ModelField) -> Type:
            if lenient_issubclass(field.type_, pydantic.BaseModel):
                return pydantic_type_system.to_artigraph(field.type_)
            return python_type_system.to_artigraph(field.type_)

        return Struct(
            name=type_.__name__,
            fields={
                field.name: _field_type_to_artigraph(field) for field in type_.__fields__.values()
            },
        )

    @classmethod
    def matches_system(cls, type_: Any) -> bool:
        return issubclass(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type) -> type[pydantic.BaseModel]:
        assert isinstance(type_, Struct)
        pydantic_type = type(
            f"{type_.name}",
            (pydantic.BaseModel,),
            {
                "__annotations__": {
                    k: (
                        pydantic_type_system.to_system(v)
                        if isinstance(v, Struct)
                        else python_type_system.to_system(v)
                    )
                    for k, v in type_.fields.items()
                }
            },
        )
        return pydantic_type
