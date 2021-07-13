import pytest
from pydantic import ValidationError

from arti.types.core import Int32, Struct, Timestamp, Type, TypeAdapter, TypeSystem


def test_Type() -> None:
    with pytest.raises(ValidationError, match="cannot be instantiated directly"):
        Type()

    class MyType(Type):
        pass

    my_type = MyType(description="Hi")
    assert my_type.description == "Hi"


def test_Struct() -> None:
    fields: dict[str, Type] = {"x": Int32()}
    assert Struct(fields=fields).fields == fields


def test_Timestamp() -> None:
    assert Timestamp(precision="second").precision == "second"
    assert Timestamp(precision="millisecond").precision == "millisecond"


def test_TypeSystem() -> None:
    python = TypeSystem(key="python")
    assert python.key == "python"

    @python.register_adapter
    class PyInt32(TypeAdapter):
        external = int
        internal = Int32

    assert PyInt32.key == "PyInt32"
