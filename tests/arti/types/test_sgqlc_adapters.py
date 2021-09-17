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
    value = st.list_of(st.non_null(MyType))


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


def test_sgqlc_interface_adapter() -> None:
    arti_iface = sgqlc_type_system.to_artigraph(MyInterface, hints={})
    assert isinstance(arti_iface, at.Struct)
    sgqlc_iface = sgqlc_type_system.to_system(arti_iface, hints={"sgqlc.abstract": True})
    assert issubclass(sgqlc_iface, st.Interface)

    for k, v in MyInterface._ContainerTypeMeta__fields.items():
        assert k in sgqlc_iface._ContainerTypeMeta__fields
        # these are st.BaseMeta types
        assert v.type == sgqlc_iface._ContainerTypeMeta__fields[k].type


def test_sgqlc_type_adapter() -> None:
    arti_type = sgqlc_type_system.to_artigraph(MyType, hints={})
    assert isinstance(arti_type, at.Struct)
    assert list(arti_type.fields.keys()) == [field.name for field in MyType]
    sgqlc_type = sgqlc_type_system.to_system(arti_type, hints={"sgqlc.interfaces": (MyInterface,)})
    assert issubclass(sgqlc_type, st.Type)
    # check that the MyType interface(s) got passed around
    assert sgqlc_type.__interfaces__ == (MyInterface,)


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
    a_container = a.fields["value"]
    assert isinstance(a_container, at.List)
    assert a_container.nullable
    a_value = a_container.value_type
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


def test_sgqlc_enum_adapter() -> None:
    arti_enum = sgqlc_type_system.to_artigraph(MyEnum, hints={})
    assert isinstance(arti_enum, at.Enum)
    assert isinstance(arti_enum.type, at.String)
    assert arti_enum.items == set(MyEnum.__choices__)
    assert arti_enum.name == "MyEnum"

    sgqlc_enum = sgqlc_type_system.to_system(arti_enum, hints={})
    assert issubclass(sgqlc_enum, st.Enum)
    assert sgqlc_enum.__name__ == "MyEnum"
    assert set(sgqlc_enum.__choices__) == arti_enum.items


def test_sgqlc_list_adapter() -> None:
    base = st.list_of(st.Int)

    arti_t = sgqlc_type_system.to_artigraph(base, hints={})
    assert isinstance(arti_t, at.List)
    assert isinstance(arti_t.value_type, at.Int64)

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
    assert isinstance(arti_t.value_type, at.Enum)
    assert not arti_t.value_type.nullable

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
    assert isinstance(arti_t.value_type, at.Enum)
    assert arti_t.value_type.nullable

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
    assert isinstance(arti_t.value_type, at.Enum)
    assert not arti_t.value_type.nullable

    sgqlc_t = sgqlc_type_system.to_system(arti_t, hints={})
    assert issubclass(sgqlc_t, st.Enum)
    assert sgqlc_t.__name__.endswith("!")
    assert sgqlc_t.mro()[1].__name__.startswith("[")
    assert sgqlc_t.mro()[1].__name__.endswith("!]")
