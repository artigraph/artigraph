import re
from typing import Any

import pytest
from box import BoxError
from pydantic import ValidationError

from arti.internal.utils import ObjectBox
from arti.types import (
    Enum,
    Float16,
    Float32,
    Float64,
    Int32,
    Struct,
    Timestamp,
    Type,
    TypeAdapter,
    TypeSystem,
)


class MyFloat(float):
    pass


class MyInt(int):
    pass


def _gen_numeric_adapter(
    artigraph_type: type[Type], system_type: Any, precision: int
) -> type[TypeAdapter]:
    class Adapter(TypeAdapter):
        key = f"{artigraph_type._class_key_}Adapter"
        artigraph = artigraph_type
        system = system_type

        priority = precision

        @classmethod
        def to_artigraph(cls, type_: Any) -> Type:
            return cls.artigraph()

        @classmethod
        def matches_system(cls, type_: Any) -> bool:
            return type_ is cls.system

        @classmethod
        def to_system(cls, type_: Type) -> Any:
            return cls.system

    return Adapter


@pytest.fixture(scope="session")
def Float16Adapter() -> type[TypeAdapter]:
    return _gen_numeric_adapter(artigraph_type=Float16, system_type=MyFloat, precision=16)


@pytest.fixture(scope="session")
def Float32Adapter() -> type[TypeAdapter]:
    return _gen_numeric_adapter(artigraph_type=Float32, system_type=MyFloat, precision=32)


@pytest.fixture(scope="session")
def Float64Adapter() -> type[TypeAdapter]:
    return _gen_numeric_adapter(artigraph_type=Float64, system_type=MyFloat, precision=64)


@pytest.fixture(scope="session")
def Int32Adapter() -> type[TypeAdapter]:
    return _gen_numeric_adapter(artigraph_type=Int32, system_type=MyInt, precision=32)


def test_Type() -> None:
    with pytest.raises(ValidationError, match="cannot be instantiated directly"):
        Type()

    class MyType(Type):
        pass

    my_type = MyType(description="Hi")
    assert my_type.description == "Hi"


def test_Enum() -> None:
    float32, items = Float32(), frozenset((1.0, 2.0, 3.0))
    enum = Enum(name="Rating", type=float32, items=items)
    assert enum.items == items
    assert enum.name == "Rating"
    assert enum.type == float32

    for other_items in (items, list(items), tuple(items)):
        other = Enum(name="Rating", type=float32, items=other_items)
        assert enum == other
        assert isinstance(enum.items, frozenset)


def test_Enum_errors() -> None:
    float32, items = Float32(), frozenset((1.0, 2.0, 3.0))

    with pytest.raises(ValueError, match="cannot be empty"):
        Enum(type=float32, items=[])

    with pytest.raises(ValueError, match="Expected an instance of"):
        Enum(type=float32, items={v: v for v in items})

    mismatch_prefix = "incompatible Float32() (<class 'float'>) item(s)"
    with pytest.raises(ValueError, match=re.escape(f"{mismatch_prefix}: [1, 2, 3]")):
        Enum(type=float32, items=(1, 2, 3))
    with pytest.raises(ValueError, match=re.escape(f"{mismatch_prefix}: [3]")):
        Enum(type=float32, items=(1.0, 2.0, 3))

    with pytest.raises(ValueError, match="Expected an instance of <class 'arti.types.Type'>"):
        # NOTE: using a python type instead of an Artigraph type
        Enum(type=float, items=items)


def test_Struct() -> None:
    fields: dict[str, Type] = {"x": Int32()}
    assert Struct(fields=fields).fields == fields


def test_Timestamp() -> None:
    assert Timestamp(precision="second").precision == "second"
    assert Timestamp(precision="millisecond").precision == "millisecond"


def test_TypeSystem(
    Float16Adapter: type[TypeAdapter],
    Float32Adapter: type[TypeAdapter],
    Float64Adapter: type[TypeAdapter],
    Int32Adapter: type[TypeAdapter],
) -> None:
    dummy = TypeSystem(key="dummy")
    assert dummy.key == "dummy"

    with pytest.raises(NotImplementedError):
        dummy.to_system(Float32())
    with pytest.raises(NotImplementedError):
        dummy.to_artigraph(MyFloat)
    # Register adapters sequentially. With a single matching adapter registered, we expect the
    # matching type. With conflicting matching adapters registered, we expect the type of the
    # adapter with the highest priority.
    for adapter, artigraph_type in [
        (Float32Adapter, Float32),
        (Float16Adapter, Float32),
        (Float64Adapter, Float64),
    ]:
        dummy.register_adapter(adapter)
        assert isinstance(dummy.to_artigraph(MyFloat), artigraph_type)
        assert dummy.to_system(artigraph_type()) is MyFloat

    with pytest.raises(NotImplementedError):
        assert isinstance(dummy.to_artigraph(MyInt), Int32)
    with pytest.raises(NotImplementedError):
        assert dummy.to_system(Int32()) is MyInt
    dummy.register_adapter(Int32Adapter)
    assert isinstance(dummy.to_artigraph(MyInt), Int32)
    assert dummy.to_system(Int32()) is MyInt


def test_type_metadata() -> None:
    metadata = {"a": {"b": "c"}}
    m = Int32(metadata=metadata)
    assert m.metadata == metadata  # type: ignore
    assert m.with_metadata({"test": True}).metadata == metadata | {"test": True}  # type: ignore
    assert isinstance(m.metadata, ObjectBox)  # type: ignore
    # Confirm box is immutable
    assert m.metadata._Box__box_config()["frozen_box"]
    with pytest.raises(BoxError, match="Box is frozen"):
        m.metadata["a"] = 5
    # ... including sub-dicts.
    with pytest.raises(BoxError, match="Box is frozen"):
        m.metadata["a"]["b"] = 5
    # And confirm odd input errors helpfully
    with pytest.raises(ValidationError, match="Expected an instance of"):
        Int32(metadata=5)


def test_type_get_metadata() -> None:
    m = Int32(metadata={"a": {"b": "c"}})
    assert m.metadata.a == m.get_metadata("a")
    assert m.metadata.a.b == m.get_metadata("a.b")

    # Check missing keys ops
    with pytest.raises(KeyError, match="'z'"):
        assert m.get_metadata("z.y")
    assert m.get_metadata("z", 0) == 0  # Fetch first level default
    assert m.get_metadata("z.y.x", 0) == 0  # Fetch nested default
