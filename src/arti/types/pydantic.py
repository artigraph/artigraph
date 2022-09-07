from typing import Any, Protocol

from pydantic import BaseModel
from pydantic.fields import ModelField
from pydantic.fields import UndefinedType as _PydanticUndefinedType

from arti.internal.type_hints import lenient_issubclass
from arti.types import Struct, Type, TypeAdapter, TypeSystem
from arti.types.python import python_type_system

pydantic_type_system = TypeSystem(key="pydantic", extends=(python_type_system,))


class _PostFieldConversionHook(Protocol):
    def __call__(self, type_: Type, *, name: str, required: bool) -> Type:
        raise NotImplementedError()


def get_post_field_conversion_hook(type_: Any) -> _PostFieldConversionHook:
    if hasattr(type_, "_pydantic_type_system_post_field_conversion_hook_"):
        return type_._pydantic_type_system_post_field_conversion_hook_  # type: ignore[no-any-return]
    return lambda type_, *, name, required: type_


@pydantic_type_system.register_adapter
class BaseModelAdapter(TypeAdapter):
    artigraph = Struct
    system = BaseModel

    @staticmethod
    def _field_to_artigraph(
        field: ModelField, *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        subtype = type_system.to_artigraph(field.outer_type_, hints=hints)
        return get_post_field_conversion_hook(subtype)(
            subtype,
            name=field.name,
            required=(
                True if isinstance(field.required, _PydanticUndefinedType) else field.required
            ),
        )

    @classmethod
    def to_artigraph(
        cls, type_: type[BaseModel], *, hints: dict[str, Any], type_system: TypeSystem
    ) -> Type:
        return Struct(
            name=type_.__name__,
            fields={
                field.name: cls._field_to_artigraph(field, hints=hints, type_system=type_system)
                for field in type_.__fields__.values()
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
        return type(
            f"{type_.name}",
            (BaseModel,),
            {
                "__annotations__": {
                    k: type_system.to_system(v, hints=hints) for k, v in type_.fields.items()
                }
            },
        )
