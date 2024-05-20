from __future__ import annotations

from typing import Any

from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo

from arti.internal.type_hints import lenient_issubclass
from arti.types import Struct, Type, TypeAdapter, TypeSystem, _NamedMixin
from arti.types.python import python_type_system

pydantic_type_system = TypeSystem(key="pydantic", extends=(python_type_system,))


@pydantic_type_system.register_adapter
class BaseModelAdapter(TypeAdapter):
    artigraph = Struct
    system = BaseModel

    @staticmethod
    def _field_to_artigraph(
        name: str, field: FieldInfo, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        subtype = type_system.to_artigraph(field.annotation, hints=hints)
        if isinstance(subtype, _NamedMixin):
            subtype = subtype.model_copy(update={"name": name})
        return subtype

    @classmethod
    def to_artigraph(
        cls, type_: type[BaseModel], *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        return Struct(
            name=type_.__name__,
            fields={
                name: cls._field_to_artigraph(name, field, hints=hints, type_system=type_system)
                for name, field in type_.model_fields.items()
            },
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return lenient_issubclass(type_, cls.system)

    @classmethod
    def to_system(
        cls, type_: Type, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> type[BaseModel]:
        assert isinstance(type_, Struct)
        annotations = {k: type_system.to_system(v, hints=hints) for k, v in type_.fields.items()}
        return create_model(type_.name, **{name: (hint, ...) for name, hint in annotations.items()})
