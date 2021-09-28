from typing import Any, Optional

import pytest
import sgqlc.types as st

import arti.types as at
from arti.types.sgqlc import SGQLC_HINT_ABSTRACT, SGQLC_HINT_INTERFACES, SGQLC_HINT_SCHEMA
from arti.types.sgqlc import is_list as is_sgqlc_list
from arti.types.sgqlc import is_nullable as is_sgqlc_nullable
from arti.types.sgqlc import sgqlc_type_system

_test_schema = st.Schema()


class MyEnum(st.Enum):
    __schema__ = _test_schema

    __choices__ = ("RED", "GREEN", "BLUE")


class MyInterface(st.Interface):
    __schema__ = _test_schema

    a = st.Int
    b = st.String


class MyType(st.Type, MyInterface):
    __schema__ = _test_schema

    c = st.Int


class MySuperType(st.Type):
    __schema__ = _test_schema

    id = st.non_null(int)
    value = st.list_of(st.non_null(MyType))


def get_schema_hint(schema: Optional[st.Schema]) -> dict[str, Any]:
    if schema is None:
        return {}
    return {SGQLC_HINT_SCHEMA: schema}


def check_schema(type_: type[st.BaseType], schema: Optional[st.Schema]) -> None:
    if schema is not None:
        assert type_.__name__ in schema
        assert schema[type_.__name__] is type_
        assert type_.__schema__ is schema


@pytest.mark.parametrize(
    ["sgqlc_type", "arti_type"],
    (
        (st.String, at.String),
        (st.Boolean, at.Boolean),
        (st.Int, at.Int64),
        (st.Float, at.Float64),
        # (st.ID, at.String),
    ),
)
def test_sgqlc_scalars(sgqlc_type: type[st.Scalar], arti_type: type[at.Type]) -> None:
    a = sgqlc_type_system.to_artigraph(sgqlc_type, hints={})
    assert isinstance(a, arti_type)
    assert sgqlc_type_system.to_system(a, hints={}) is sgqlc_type


@pytest.mark.parametrize(
    ["schema"],
    (
        (st.Schema(),),
        (None,),
    ),
)
def test_sgqlc_interface_adapter(schema: Optional[st.Schema]) -> None:
    arti_iface = sgqlc_type_system.to_artigraph(MyInterface, hints={})
    assert isinstance(arti_iface, at.Struct)

    sgqlc_iface = sgqlc_type_system.to_system(
        arti_iface, hints={SGQLC_HINT_ABSTRACT: True} | get_schema_hint(schema)
    )
    assert issubclass(sgqlc_iface, st.Interface)
    check_schema(sgqlc_iface, schema)
    for k, v in MyInterface._ContainerTypeMeta__fields.items():
        assert k in sgqlc_iface._ContainerTypeMeta__fields
        # these are st.BaseMeta types
        assert v.type == sgqlc_iface._ContainerTypeMeta__fields[k].type


@pytest.mark.parametrize(
    ["schema"],
    (
        (st.Schema(),),
        (None,),
    ),
)
def test_sgqlc_type_adapter(schema: Optional[st.Schema]) -> None:
    arti_type = sgqlc_type_system.to_artigraph(MyType, hints={})
    assert isinstance(arti_type, at.Struct)
    assert list(arti_type.fields.keys()) == [field.name for field in MyType]

    sgqlc_type = sgqlc_type_system.to_system(
        arti_type, hints={SGQLC_HINT_INTERFACES: (MyInterface,)} | get_schema_hint(schema)
    )
    assert issubclass(sgqlc_type, st.Type)
    # check that the MyType interface(s) got passed around
    assert sgqlc_type.__interfaces__ == (MyInterface,)
    check_schema(sgqlc_type, schema)


def test_nested_sgqlc_type() -> None:
    a = sgqlc_type_system.to_artigraph(MySuperType, hints={})
    assert isinstance(a, at.Struct)
    s = sgqlc_type_system.to_system(a, hints={})
    assert issubclass(s, st.Type)

    # root arti
    assert a.name == MySuperType.__name__
    assert a.nullable
    assert set(a.fields) == {"id", "value"}
    assert isinstance(a.fields["id"], at._Int)
    assert not a.fields["id"].nullable
    assert isinstance(a.fields["value"], at.List)
    assert isinstance(a.fields["value"].element, at.Struct)
    # root sgqlc
    assert s.__name__ == MySuperType.__name__
    assert is_sgqlc_nullable(s)
    assert set(f.name for f in s) == {"id", "value"}
    assert issubclass(s.id.type, st.Int)
    assert not is_sgqlc_nullable(s.id.type)
    assert is_sgqlc_list(s.value.type)
    assert issubclass(s.value.type, st.Type)

    # value arti
    a_container = a.fields["value"]
    assert isinstance(a_container, at.List)
    assert a_container.nullable
    a_value = a_container.element
    assert isinstance(a_value, at.Struct)
    assert a_value.name == MyType.__name__
    assert not a_value.nullable
    assert set(a_value.fields) == {"a", "b", "c"}
    assert isinstance(a_value.fields["a"], at._Int)
    assert isinstance(a_value.fields["b"], at.String)
    assert isinstance(a_value.fields["c"], at._Int)
    # value sgqlc
    s_container = s.value.type
    assert s_container.__name__ == f"[{MyType.__name__}!]"
    assert is_sgqlc_nullable(s_container)
    s_value = s_container.mro()[1]
    assert not is_sgqlc_nullable(s_value)
    assert set(f.name for f in s_container) == {"a", "b", "c"}
    assert issubclass(s_container.a.type, st.Int)
    assert issubclass(s_container.b.type, st.String)
    assert issubclass(s_container.c.type, st.Int)


@pytest.mark.parametrize(
    ["schema"],
    (
        (st.Schema(),),
        # (None,),
    ),
)
def test_sgqlc_enum_adapter(schema: Optional[st.Schema]) -> None:
    arti_enum = sgqlc_type_system.to_artigraph(MyEnum, hints={})
    assert isinstance(arti_enum, at.Enum)
    assert isinstance(arti_enum.type, at.String)
    assert arti_enum.items == set(MyEnum.__choices__)
    assert arti_enum.name == "MyEnum"

    sgqlc_enum = sgqlc_type_system.to_system(arti_enum, hints=get_schema_hint(schema))
    assert issubclass(sgqlc_enum, st.Enum)
    assert sgqlc_enum.__name__ == "MyEnum"
    assert set(sgqlc_enum.__choices__) == arti_enum.items
    check_schema(sgqlc_enum, schema)


def test_sgqlc_list_adapter() -> None:
    base = st.list_of(st.Int)

    arti_t = sgqlc_type_system.to_artigraph(base, hints={})
    assert isinstance(arti_t, at.List)
    assert isinstance(arti_t.element, at.Int64)

    sgqlc_t = sgqlc_type_system.to_system(arti_t, hints={})
    assert issubclass(sgqlc_t, st.Int)
    assert sgqlc_t.__name__.startswith("[")
    assert sgqlc_t.__name__.endswith("]")


def test_sgqlc_non_null_adapting() -> None:
    base = st.non_null(MyEnum)

    arti_t = sgqlc_type_system.to_artigraph(base, hints={})
    assert isinstance(arti_t, at.Enum)
    assert not arti_t.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t, hints={})
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.endswith("!")


def test_sgqlc_list_of_non_null_adapting() -> None:
    base = st.list_of(st.non_null(MyEnum))

    arti_t = sgqlc_type_system.to_artigraph(base, hints={})
    assert isinstance(arti_t, at.List)
    assert arti_t.nullable
    assert isinstance(arti_t.element, at.Enum)
    assert not arti_t.element.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t, hints={})
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.startswith("[")
    assert sgqlc_t.__name__.endswith("]")
    assert sgqlc_t.mro()[1].__name__.endswith("!")


def test_sgqlc_non_null_list_of_adapting() -> None:
    base = st.non_null(st.list_of(MyEnum))

    arti_t = sgqlc_type_system.to_artigraph(base, hints={})
    assert isinstance(arti_t, at.List)
    assert not arti_t.nullable
    assert isinstance(arti_t.element, at.Enum)
    assert arti_t.element.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t, hints={})
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.endswith("!")
    assert sgqlc_t.mro()[1].__name__.startswith("[")
    assert sgqlc_t.mro()[1].__name__.endswith("]")


def test_sgqlc_non_null_list_of_non_null_adapting() -> None:
    base = st.non_null(st.list_of(st.non_null(MyEnum)))

    arti_t = sgqlc_type_system.to_artigraph(base, hints={})
    assert isinstance(arti_t, at.List)
    assert not arti_t.nullable
    assert isinstance(arti_t.element, at.Enum)
    assert not arti_t.element.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t, hints={})
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.endswith("!")
    assert sgqlc_t.mro()[1].__name__.startswith("[")
    assert sgqlc_t.mro()[1].__name__.endswith("!]")


@pytest.mark.parametrize(
    ["a", "b"],
    (
        (
            at.Struct(name="Test", fields={"a": at.String(), "b": at.String()}),
            at.Struct(name="Test", fields={"c": at.String(), "d": at.String()}),
        ),
        (
            at.Enum(name="Test", type=at.String(), items={"a", "b"}),
            at.Enum(name="Test", type=at.String(), items={"c", "d"}),
        ),
    ),
)
def test_sgqlc_schema_duplicate(a: at.Type, b: at.Type) -> None:
    hints = get_schema_hint(st.Schema())

    m1 = sgqlc_type_system.to_system(a, hints=hints)
    assert sgqlc_type_system.to_system(a, hints=hints) is m1
    with pytest.raises(
        ValueError, match="Detected duplicate but mismatched sgqlc Test type when converting"
    ):
        sgqlc_type_system.to_system(b, hints=hints)
