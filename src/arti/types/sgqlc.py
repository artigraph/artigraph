from functools import partial
from typing import Any, ClassVar, Literal

import sgqlc.types

import arti.types
from arti.types import TypeAdapter, TypeSystem, _ScalarClassTypeAdapter


def is_nullable(type_: type[sgqlc.types.BaseType]) -> bool:
    return not type_.__name__.endswith("!")


def is_list(type_: type[sgqlc.types.BaseType]) -> bool:
    return type_.__name__.startswith("[")


# sgqlc types are implicitly nullable, which is the opposite of the arti Type default. Rather than
# handle nullable in every TypeAdapter below, we'll handle in the base dispatch.
#
# This differs from the python_type_system, where not-null is the default AND there's an Optional
# type we can easily match against.
class _SgqlcTypeSystem(TypeSystem):
    def to_artigraph(self, type_: Any, *, hints: dict[str, Any]) -> arti.types.Type:
        if not (nullable := is_nullable(type_)):
            # non_null subclasses the input type (hence, we can fetch it with .mro)
            type_ = type_.mro()[1]
        ret = super().to_artigraph(type_, hints=hints)
        if ret.nullable != nullable:
            ret = ret.copy(update={"nullable": nullable})
        return ret

    def to_system(self, type_: arti.types.Type, *, hints: dict[str, Any]) -> Any:
        ret = super().to_system(type_.copy(update={"nullable": True}), hints=hints)
        assert is_nullable(ret)  # sgqlc default
        if not type_.nullable:
            ret = sgqlc.types.non_null(ret)
        return ret


sgqlc_type_system = _SgqlcTypeSystem(key="sgqlc")

_generate = partial(_ScalarClassTypeAdapter.generate, type_system=sgqlc_type_system)

_generate(artigraph=arti.types.Boolean, system=sgqlc.types.Boolean)
for _precision in (16, 32, 64):
    _generate(
        artigraph=getattr(arti.types, f"Float{_precision}"),
        system=sgqlc.types.Float,
        priority=_precision,
    )
for _precision in (8, 16, 32, 64):
    _generate(
        artigraph=getattr(arti.types, f"Int{_precision}"),
        system=sgqlc.types.Int,
        priority=_precision,
    )
# Register sgqlc.types.String with higher priority to avoid arti.types.String->ID by default
_generate(artigraph=arti.types.String, system=sgqlc.types.String, priority=1)
_generate(artigraph=arti.types.String, system=sgqlc.types.ID, name="sgqlcID")


# NOTE: GraphQL only supports string enums (a series of "names")
@sgqlc_type_system.register_adapter
class SgqlcEnumAdapter(TypeAdapter):
    artigraph = arti.types.Enum
    system = sgqlc.types.Enum

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> arti.types.Type:
        assert issubclass(type_, cls.system)
        return cls.artigraph(
            name=type_.__name__,
            type=arti.types.String(),
            items=type_.__choices__,
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return issubclass(type_, cls.system)

    @classmethod
    def to_system(cls, type_: arti.types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        assert isinstance(type_.type, arti.types.String)
        return type(
            type_.name,
            (cls.system,),
            {
                "__choices__": tuple(type_.items),
                "__schema__": sgqlc.types.Schema(),  # Don't reference the global schema
                f"_{type_.name}__auto_register": False,  # Disable registering with the global schema
            },
        )


@sgqlc_type_system.register_adapter
class SgqlcList(TypeAdapter):
    artigraph = arti.types.List
    system = sgqlc.types.BaseType
    priority = int(1e9)  # May wrap other types, so must be highest priority

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> arti.types.Type:
        assert issubclass(type_, cls.system)
        # list_of subclasses the input type (hence, we can fetch it with .mro)
        return cls.artigraph(value_type=sgqlc_type_system.to_artigraph(type_.mro()[1], hints=hints))

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return issubclass(type_, cls.system) and is_list(type_)

    @classmethod
    def to_system(cls, type_: arti.types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return sgqlc.types.list_of(sgqlc_type_system.to_system(type_.value_type, hints=hints))


class _StructAdapter(TypeAdapter):
    artigraph = arti.types.Struct
    kind: ClassVar[Literal["interface", "type"]]

    @classmethod
    def matches_artigraph(cls, type_: arti.types.Type, *, hints: dict[str, Any]) -> bool:
        abstract = hints.get(f"{sgqlc_type_system.key}.abstract", False)
        # Interfaces should be abstract, Types not.
        return isinstance(type_, cls.artigraph) and (
            abstract if cls.kind == "interface" else not abstract
        )

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> arti.types.Type:
        assert issubclass(type_, cls.system)
        return cls.artigraph(
            name=type_.__name__,
            fields={
                field.name: sgqlc_type_system.to_artigraph(field.type, hints=hints)
                for field in type_
            },
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return issubclass(type_, cls.system) and type_.__kind__ == cls.kind

    @classmethod
    def to_system(cls, type_: arti.types.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return type(
            type_.name,
            (
                cls.system,
                *hints.get(f"{sgqlc_type_system.key}.interfaces", ()),
            ),
            {
                "__schema__": sgqlc.types.Schema(),  # Don't reference the global schema
                f"_{type_.name}__auto_register": False,  # Disable registering with the global schema
                **{k: sgqlc_type_system.to_system(v, hints=hints) for k, v in type_.fields.items()},
            },
        )


@sgqlc_type_system.register_adapter
class SgqlcInterfaceAdapter(_StructAdapter):
    system = sgqlc.types.Interface
    kind: ClassVar[Literal["interface"]] = "interface"


@sgqlc_type_system.register_adapter
class SgqlcTypeAdapter(_StructAdapter):
    system = sgqlc.types.Type
    kind: ClassVar[Literal["type"]] = "type"
