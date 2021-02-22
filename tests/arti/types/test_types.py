import pytest

from arti.types.core import Int32, Struct, Timestamp, Type, TypeSystem


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
    assert TypeSystem("tests").key == "tests"
