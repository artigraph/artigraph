import pytest

from arti.types.core import Int32, String, Struct, Timestamp, Type, TypeAdapter, TypeSystem


def test_Type() -> None:
    with pytest.raises(ValueError, match="Type cannot be instantiated directly"):
        Type()

    class MyType(Type):
        pass

    my_type = MyType(description="Hi")
    assert my_type.description == "Hi"


def test_Struct() -> None:
    fields: dict[str, Type] = {"x": Int32()}
    assert Struct(fields).fields == fields


def test_Timestamp() -> None:
    assert Timestamp("second").precision == "second"
    assert Timestamp("millisecond").precision == "millisecond"


def test_TypeSystem() -> None:
    python = TypeSystem("python")
    assert python.key == "python"

    @python.register_adapter
    class PyInt32(TypeAdapter):
        external = int
        internal = Int32

    assert PyInt32.key == "PyInt32"


def test_scalar_to_from_dict() -> None:
    # scalar types without init params
    int_dict = {"type": "Int32"}
    int_obj = Type.from_dict(int_dict)
    assert isinstance(int_obj, Int32)
    assert int_obj.to_dict() == int_dict

    # scalar type with init params
    timestamp_dict = {"type": "Timestamp", "params": {"precision": "second"}}
    timestamp_obj = Type.from_dict(timestamp_dict)
    assert isinstance(timestamp_obj, Timestamp)
    assert timestamp_obj.precision == "second"
    assert timestamp_obj.to_dict() == timestamp_dict


def test_Struct_to_from_dict() -> None:
    struct_dict = {
        "type": "Struct",
        "params": {"fields": {"a": {"type": "Int32"}, "b": {"type": "String"}}},
    }
    struct_obj = Type.from_dict(struct_dict)
    assert isinstance(struct_obj, Struct)
    assert isinstance(struct_obj.fields["a"], Int32)
    assert isinstance(struct_obj.fields["b"], String)
    assert struct_obj.to_dict() == struct_dict

    # check struct within a struct
    parent_struct_dict = {"type": "Struct", "params": {"fields": {"child": struct_dict}}}
    parent_struct_obj = Type.from_dict(parent_struct_dict)
    assert isinstance(parent_struct_obj, Struct)
    assert isinstance(parent_struct_obj.fields["child"], Struct)
