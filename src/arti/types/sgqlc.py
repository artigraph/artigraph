from functools import partial
from typing import Any, Callable, ClassVar, Literal, Optional, cast

import sgqlc.types as st

import arti.types as at
from arti.types import TypeAdapter, TypeSystem, _ScalarClassTypeAdapter


def is_nullable(type_: type[st.BaseType]) -> bool:
    return not type_.__name__.endswith("!")


def is_list(type_: type[st.BaseType]) -> bool:
    return type_.__name__.startswith("[")


def get_schema(hints: dict[str, Any]) -> st.Schema:
    if (schema := hints.get(SGQLC_HINT_SCHEMA)) is not None:
        return cast(st.Schema, schema)
    # Avoid referencing (and thus polluting) the global schema.
    return st.Schema()


def get_existing(
    type_: at.Type,
    schema: st.Schema,
    equals: Callable[[at.Type, type[st.BaseType]], bool],
) -> Optional[type[st.BaseType]]:
    assert isinstance(type_, at._NamedMixin)
    if (existing := getattr(schema, type_.name, None)) is not None:
        if not equals(type_, existing):
            raise ValueError(
                f"Detected duplicate but mismatched sgqlc {existing.__name__} type when converting {type_}: {repr(existing)}"
            )
        return cast(type[st.BaseType], existing)
    return None


# sgqlc types are implicitly nullable, which is the opposite of the arti Type default. Rather than
# handle nullable in every TypeAdapter below, we'll handle in the base dispatch.
#
# This differs from the python_type_system, where not-null is the default AND there's an Optional
# type we can easily match against.
class _SgqlcTypeSystem(TypeSystem):
    def to_artigraph(self, type_: Any, *, hints: dict[str, Any]) -> at.Type:
        if not (nullable := is_nullable(type_)):
            # non_null subclasses the input type (hence, we can fetch it with .mro)
            type_ = type_.mro()[1]
        ret = super().to_artigraph(type_, hints=hints)
        if ret.nullable != nullable:
            ret = ret.copy(update={"nullable": nullable})
        return ret

    def to_system(self, type_: at.Type, *, hints: dict[str, Any]) -> Any:
        ret = super().to_system(type_.copy(update={"nullable": True}), hints=hints)
        assert is_nullable(ret)  # sgqlc default
        if not type_.nullable:
            ret = st.non_null(ret)
        return ret


sgqlc_type_system = _SgqlcTypeSystem(key="sgqlc")

SGQLC_HINT_ABSTRACT = f"{sgqlc_type_system.key}.abstract"
SGQLC_HINT_INTERFACES = f"{sgqlc_type_system.key}.interfaces"
SGQLC_HINT_SCHEMA = f"{sgqlc_type_system.key}.schema"

_generate = partial(_ScalarClassTypeAdapter.generate, type_system=sgqlc_type_system)

_generate(artigraph=at.Boolean, system=st.Boolean)
for _precision in (16, 32, 64):
    _generate(
        artigraph=getattr(at, f"Float{_precision}"),
        system=st.Float,
        priority=_precision,
    )
for _precision in (8, 16, 32, 64):
    _generate(
        artigraph=getattr(at, f"Int{_precision}"),
        system=st.Int,
        priority=_precision,
    )
# Register st.String with higher priority to avoid at.String->ID by default
_generate(artigraph=at.String, system=st.String, priority=1)
_generate(artigraph=at.String, system=st.ID, name="sgqlcID")


# NOTE: GraphQL only supports string enums (a series of "names")
@sgqlc_type_system.register_adapter
class SgqlcEnumAdapter(TypeAdapter):
    artigraph = at.Enum
    system = st.Enum

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> at.Type:
        assert issubclass(type_, cls.system)
        return cls.artigraph(
            name=type_.__name__,
            type=at.String(),
            items=type_.__choices__,
        )

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return issubclass(type_, cls.system)

    @classmethod
    def _compare_existing(cls, type_: at.Type, existing: type[st.BaseType]) -> bool:
        return cls.matches_system(existing, hints={}) and cast(at.Enum, type_).items == set(
            existing  # type: ignore
        )

    @classmethod
    def to_system(cls, type_: at.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        assert isinstance(type_.type, at.String)
        schema = get_schema(hints)
        if (existing := get_existing(type_, schema, equals=cls._compare_existing)) is not None:
            return existing
        return type(
            type_.name,
            (cls.system,),
            {
                "__choices__": tuple(type_.items),
                "__schema__": schema,
            },
        )


@sgqlc_type_system.register_adapter
class SgqlcList(TypeAdapter):
    artigraph = at.List
    system = st.BaseType
    priority = int(1e9)  # May wrap other types, so must be highest priority

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> at.Type:
        assert issubclass(type_, cls.system)
        # list_of subclasses the input type (hence, we can fetch it with .mro)
        return cls.artigraph(element=sgqlc_type_system.to_artigraph(type_.mro()[1], hints=hints))

    @classmethod
    def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
        return issubclass(type_, cls.system) and is_list(type_)

    @classmethod
    def to_system(cls, type_: at.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        return st.list_of(sgqlc_type_system.to_system(type_.element, hints=hints))


class _StructAdapter(TypeAdapter):
    artigraph = at.Struct
    kind: ClassVar[Literal["interface", "type"]]

    @classmethod
    def matches_artigraph(cls, type_: at.Type, *, hints: dict[str, Any]) -> bool:
        abstract = hints.get(SGQLC_HINT_ABSTRACT, False)
        # Interfaces should be abstract, Types not.
        return isinstance(type_, cls.artigraph) and (
            abstract if cls.kind == "interface" else not abstract
        )

    @classmethod
    def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> at.Type:
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
    def _compare_existing(
        cls, type_: at.Type, existing: type[st.BaseType], field_types: dict[str, type[st.BaseType]]
    ) -> bool:
        assert set(cast(at.Struct, type_).fields) == set(field_types)
        return cls.matches_system(existing, hints={}) and field_types == {
            field.name: field.type for field in cast(st.Type, existing)
        }

    @classmethod
    def to_system(cls, type_: at.Type, *, hints: dict[str, Any]) -> Any:
        assert isinstance(type_, cls.artigraph)
        schema = get_schema(hints)
        fields = {k: sgqlc_type_system.to_system(v, hints=hints) for k, v in type_.fields.items()}
        compare_existing = partial(cls._compare_existing, field_types=fields)
        if (existing := get_existing(type_, schema, equals=compare_existing)) is not None:
            return existing
        return type(
            type_.name,
            (
                cls.system,
                *hints.get(SGQLC_HINT_INTERFACES, ()),
            ),
            {
                "__schema__": schema,
                **fields,
            },
        )


@sgqlc_type_system.register_adapter
class SgqlcInterfaceAdapter(_StructAdapter):
    system = st.Interface
    kind: ClassVar[Literal["interface"]] = "interface"


@sgqlc_type_system.register_adapter
class SgqlcTypeAdapter(_StructAdapter):
    system = st.Type
    kind: ClassVar[Literal["type"]] = "type"
