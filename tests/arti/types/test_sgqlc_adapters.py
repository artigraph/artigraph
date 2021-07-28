import pytest
import sgqlc.types as st

import arti.types as at
from arti.types.sgqlc import is_list as is_sgqlc_list
from arti.types.sgqlc import is_nullable as is_sgqlc_nullable
from arti.types.sgqlc import sgqlc_type_system


class MyEnum(st.Enum):
    __choices__ = ("RED", "GREEN", "BLUE")


class MyInterface(st.Interface):
    a = st.Int
    b = st.String


class MyType(st.Type, MyInterface):
    c = st.Int


class MySuperType(st.Type):
    id = st.non_null(int)
    value = st.list_of(MyType)


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
    a = sgqlc_type_system.to_artigraph(sgqlc_type)
    assert isinstance(a, arti_type)
    assert sgqlc_type_system.to_system(a) is sgqlc_type


def test_nested_sgqlc_type() -> None:
    a = sgqlc_type_system.to_artigraph(MySuperType)
    assert isinstance(a, at.Struct)
    s = sgqlc_type_system.to_system(a)
    assert issubclass(s, st.Type)

    # root arti
    assert a.name == MySuperType.__name__
    assert a.nullable
    assert set(a.fields) == {"id", "value"}
    assert isinstance(a.fields["id"], at._Int)
    assert not a.fields["id"].nullable
    assert isinstance(a.fields["value"], at.List)
    assert isinstance(a.fields["value"].value_type, at.Struct)
    # root sgqlc
    assert s.__name__ == MySuperType.__name__
    assert is_sgqlc_nullable(s)
    assert set(f.name for f in s) == {"id", "value"}
    assert issubclass(s.id.type, st.Int)
    assert not is_sgqlc_nullable(s.id.type)
    assert is_sgqlc_list(s.value.type)
    assert issubclass(s.value.type, st.Type)

    # value arti
    a_sub = a.fields["value"].value_type
    assert a_sub.name == MyType.__name__
    assert a_sub.nullable
    assert set(a_sub.fields) == {"a", "b", "c"}
    assert isinstance(a_sub.fields["a"], at._Int)
    assert isinstance(a_sub.fields["b"], at.String)
    assert isinstance(a_sub.fields["c"], at._Int)
    # value sgqlc
    s_sub = s.value.type
    assert s_sub.__name__ == f"[{MyType.__name__}]"
    assert is_sgqlc_nullable(s_sub)
    assert set(f.name for f in s_sub) == {"a", "b", "c"}
    assert issubclass(s_sub.a.type, st.Int)
    assert issubclass(s_sub.b.type, st.String)
    assert issubclass(s_sub.c.type, st.Int)


def test_sgqlc_enum_adapter() -> None:
    arti_enum = sgqlc_type_system.to_artigraph(MyEnum)
    assert isinstance(arti_enum, at.Enum)
    assert isinstance(arti_enum.type, at.String)
    assert arti_enum.items == set(MyEnum.__choices__)
    assert arti_enum.name == "MyEnum"

    sgqlc_enum = sgqlc_type_system.to_system(arti_enum)
    assert issubclass(sgqlc_enum, st.Enum)
    assert sgqlc_enum.__name__ == "MyEnum"
    assert set(sgqlc_enum.__choices__) == arti_enum.items


def test_sgqlc_list_adapter() -> None:
    base = st.list_of(st.Int)

    arti_t = sgqlc_type_system.to_artigraph(base)
    assert isinstance(arti_t, at.List)
    assert isinstance(arti_t.value_type, at.Int64)

    sgqlc_t = sgqlc_type_system.to_system(arti_t)
    assert issubclass(sgqlc_t, st.Int)
    assert sgqlc_t.__name__.startswith("[")
    assert sgqlc_t.__name__.endswith("]")


def test_sgqlc_non_null_adapting() -> None:
    base = st.non_null(MyEnum)

    arti_t = sgqlc_type_system.to_artigraph(base)
    assert isinstance(arti_t, at.Enum)
    assert not arti_t.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t)
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.endswith("!")


def test_sgqlc_list_of_non_null_adapting() -> None:
    base = st.list_of(st.non_null(MyEnum))

    arti_t = sgqlc_type_system.to_artigraph(base)
    assert isinstance(arti_t, at.List)
    assert arti_t.nullable
    assert isinstance(arti_t.value_type, at.Enum)
    assert not arti_t.value_type.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t)
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.startswith("[")
    assert sgqlc_t.__name__.endswith("]")
    assert sgqlc_t.mro()[1].__name__.endswith("!")


def test_sgqlc_non_null_list_of_adapting() -> None:
    base = st.non_null(st.list_of(MyEnum))

    arti_t = sgqlc_type_system.to_artigraph(base)
    assert isinstance(arti_t, at.List)
    assert not arti_t.nullable
    assert isinstance(arti_t.value_type, at.Enum)
    assert arti_t.value_type.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t)
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.endswith("!")
    assert sgqlc_t.mro()[1].__name__.startswith("[")
    assert sgqlc_t.mro()[1].__name__.endswith("]")


def test_sgqlc_non_null_list_of_non_null_adapting() -> None:
    base = st.non_null(st.list_of(st.non_null(MyEnum)))

    arti_t = sgqlc_type_system.to_artigraph(base)
    assert isinstance(arti_t, at.List)
    assert not arti_t.nullable
    assert isinstance(arti_t.value_type, at.Enum)
    assert not arti_t.value_type.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t)
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.endswith("!")
    assert sgqlc_t.mro()[1].__name__.startswith("[")
    assert sgqlc_t.mro()[1].__name__.endswith("!]")


def test_sgqlc_interface_adapter() -> None:
    arti_iface = sgqlc_type_system.to_artigraph(MyInterface)
    sgqlc_iface = sgqlc_type_system.to_system(arti_iface)

    assert isinstance(arti_iface, at.Struct)
    assert arti_iface.get_metadata("sgqlc.abstract") is True

    for k, v in MyInterface._ContainerTypeMeta__fields.items():
        assert k in sgqlc_iface._ContainerTypeMeta__fields

        # these are st.BaseMeta types
        assert v.type == sgqlc_iface._ContainerTypeMeta__fields[k].type


def test_sgqlc_type_adapter() -> None:
    arti_type = sgqlc_type_system.to_artigraph(MyType)
    sgqlc_type = sgqlc_type_system.to_system(arti_type)

    assert isinstance(arti_type, at.Struct)
    assert list(arti_type.fields.keys()) == [field.name for field in MyType]
    assert arti_type.get_metadata("sgqlc.abstract") is False

    # check that the MyType interface(s) got passed around
    assert arti_type.get_metadata("sgqlc.interfaces") == (MyInterface,)
    assert sgqlc_type.__interfaces__ == (MyInterface,)
