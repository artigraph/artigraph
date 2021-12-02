import re
from typing import Any

import pytest
from pydantic import ValidationError

from arti.internal.utils import frozendict
from arti.types import (
    Collection,
    Enum,
    Float16,
    Float32,
    Float64,
    Int32,
    List,
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
        def to_artigraph(cls, type_: Any, *, hints: dict[str, Any]) -> Type:
            return cls.artigraph()

        @classmethod
        def matches_system(cls, type_: Any, *, hints: dict[str, Any]) -> bool:
            return type_ is cls.system

        @classmethod
        def to_system(cls, type_: Type, *, hints: dict[str, Any]) -> Any:
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

    with pytest.raises(ValueError, match="expected an instance of"):
        Enum(type=float32, items={v: v for v in items})

    mismatch_prefix = "incompatible Float32() (<class 'float'>) item(s)"
    with pytest.raises(ValueError, match=re.escape(f"{mismatch_prefix}: [1, 2, 3]")):
        Enum(type=float32, items=(1, 2, 3))
    with pytest.raises(ValueError, match=re.escape(f"{mismatch_prefix}: [3]")):
        Enum(type=float32, items=(1.0, 2.0, 3))

    with pytest.raises(ValueError, match="expected an instance of <class 'arti.types.Type'>"):
        # NOTE: using a python type instead of an Artigraph type
        Enum(type=float, items=items)


def test_List() -> None:
    lst = List(element=Int32())
    assert lst.element == Int32()


def test_Collection() -> None:
    collection = Collection(element=Int32())
    assert isinstance(collection, List)  # Confirm subclass
    assert collection.cluster_by == ()
    assert collection.element == Int32()
    assert collection.is_partitioned is False
    assert collection.name == "anon"
    assert collection.partition_by == ()
    assert collection.partition_fields == frozendict()


def test_Collection_partitioned() -> None:
    fields = {"x": Int32(), "y": Int32()}

    collection = Collection(element=Struct(fields=fields), partition_by=("x",), cluster_by=("y",))
    assert collection.partition_by == ("x",)
    assert collection.cluster_by == ("y",)
    assert collection.partition_fields == frozendict({"x": fields["x"]})
    assert collection.is_partitioned is True

    with pytest.raises(ValueError, match="requires element to be a Struct"):
        Collection(element=Int32(), partition_by=("x",))
    with pytest.raises(ValueError, match="requires element to be a Struct"):
        Collection(element=Int32(), cluster_by=("x",))


@pytest.mark.parametrize(["param"], (("partition_by",), ("cluster_by",)))
def test_Collection_field_references(param: str) -> None:
    match = re.escape("unknown field(s): {'z'}")

    with pytest.raises(ValueError, match=match):
        Collection(element=Struct(fields={"x": Int32(), "y": Int32()}), **{param: ("z",)})

    # Test with a bad `fields` and confirm the error *does not* contain the "unknown field" error.
    with pytest.raises(ValueError, match="expected an instance of") as exc:
        Collection(element="junk", **{param: ("z",)})
    with pytest.raises(AssertionError):
        exc.match(match)


def test_Collection_partition_cluster_overlapping() -> None:
    match = re.escape("clustering fields overlap with partition fields: {'x'}")

    with pytest.raises(ValueError, match=match):
        Collection(
            element=Struct(fields={"x": Int32(), "y": Int32()}),
            partition_by=("x",),
            cluster_by=("x",),
        )

    # Test with a bad `partition_by` and confirm the error *does not* contain the "unknown field" error.
    with pytest.raises(ValueError, match="expected an instance of") as exc:
        Collection(
            element=Struct(fields={"x": Int32(), "y": Int32()}), partition_by="x", cluster_by=("y",)
        )
    with pytest.raises(AssertionError):
        exc.match(match)


def test_Struct() -> None:
    fields = {"x": Int32(), "y": Int32()}
    s = Struct(fields=fields)
    assert isinstance(s.fields, frozendict)
    assert s.fields == frozendict(fields)


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
        dummy.to_system(Float32(), hints={})
    with pytest.raises(NotImplementedError):
        dummy.to_artigraph(MyFloat, hints={})
    # Register adapters sequentially. With a single matching adapter registered, we expect the
    # matching type. With conflicting matching adapters registered, we expect the type of the
    # adapter with the highest priority.
    for adapter, artigraph_type in [
        (Float32Adapter, Float32),
        (Float16Adapter, Float32),
        (Float64Adapter, Float64),
    ]:
        dummy.register_adapter(adapter)
        assert isinstance(dummy.to_artigraph(MyFloat, hints={}), artigraph_type)
        assert dummy.to_system(artigraph_type(), hints={}) is MyFloat

    with pytest.raises(NotImplementedError):
        assert isinstance(dummy.to_artigraph(MyInt, hints={}), Int32)
    with pytest.raises(NotImplementedError):
        assert dummy.to_system(Int32(), hints={}) is MyInt
    dummy.register_adapter(Int32Adapter)
    assert isinstance(dummy.to_artigraph(MyInt, hints={}), Int32)
    assert dummy.to_system(Int32(), hints={}) is MyInt
