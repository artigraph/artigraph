from typing import Any, Protocol

from pydantic import BaseModel
from pydantic.fields import ModelField
from pydantic.fields import UndefinedType as _PydanticUndefinedType

from arti.internal.type_hints import lenient_issubclass
from arti.types import Struct, Type, TypeAdapter, TypeSystem, _ScalarClassTypeAdapter
from arti.types.python import python_type_system

pydantic_type_system = TypeSystem(key="pydantic")


class _PostFieldConversionHook(Protocol):
    def __call__(self, type_: Type, *, name: str, required: bool) -> Type:
        raise NotImplementedError()


def get_post_field_conversion_hook(type_: Any) -> _PostFieldConversionHook:
    if hasattr(type_, "_pydantic_type_system_post_field_conversion_hook_"):
        return type_._pydantic_type_system_post_field_conversion_hook_  # type: ignore
    return lambda type_, *, name, required: type_


@pydantic_type_system.register_adapter
class BaseModelAdapter(TypeAdapter):
    artigraph = Struct
    system = BaseModel

    @staticmethod
    def _field_to_artigraph(field: ModelField, *, hints: dict[str, Any]) -> Type:
        subtype = python_type_system.to_artigraph(field.outer_type_, hints=hints)
        return get_post_field_conversion_hook(subtype)(
            subtype,
            name=field.name,
            required=(
                True if isinstance(field.required, _PydanticUndefinedType) else field.required
            ),
        )

    @classmethod
    def to_artigraph(cls, type_: type[BaseModel], *, hints: dict[str, Any]) -> Type:
        return Struct(
            name=type_.__name__,
            fields={
                field.name: cls._field_to_artigraph(field, hints=hints)
                for field in type_.__fields__.values()
            },
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return lenient_issubclass(type_, cls.system)

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any]) -> type[BaseModel]:
        assert isinstance(type_, Struct)
        return type(
            f"{type_.name}",
            (BaseModel,),
            {
                "__annotations__": {
                    k: (
                        pydantic_type_system.to_system(v, hints=hints)
                        if isinstance(v, Struct)
                        else python_type_system.to_system(v, hints=hints)
                    )
                    for k, v in type_.fields.items()
                }
            },
        )


# Extend the python_type_system to handle BaseModel. This simplifies conversion of nested models


@python_type_system.register_adapter
class _PythonBaseModelAdapter(_ScalarClassTypeAdapter):
    artigraph = Struct
    system = BaseModel
    priority = int(1e8)  # Beneath the Optional Adapter

    @classmethod
    def matches_artigraph(cls, type_: Type, *, hints: dict[str, Any]) -> bool:
        # Avoid converting a python type to a BaseModel unless explicit annotated.
        return super().matches_artigraph(type_, hints=hints) and hints.get(
            f"{pydantic_type_system.key}.is_model", False
        )

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> Type:
        return BaseModelAdapter.to_artigraph(type_, hints=hints)

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return BaseModelAdapter.matches_system(type_, hints=hints)

    @classmethod
    def to_system(cls, type_: Type, *, hints: dict[str, Any]) -> Any:
        return BaseModelAdapter.to_system(type_, hints=hints)
