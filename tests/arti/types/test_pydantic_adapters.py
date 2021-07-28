import pydantic

from arti.types import Struct, Type
from arti.types.pydantic import pydantic_type_system


class MyModel(pydantic.BaseModel):
    x: int
    y: str


class NestedModel(pydantic.BaseModel):
    value: MyModel
    name: str
    # TODO: figure out pass-through of artigraph Types that
    # timestamp: Timestamp
    # struct: Struct


def test_pydantic_base_model() -> None:
    arti_model = pydantic_type_system.to_artigraph(MyModel)

    assert isinstance(arti_model, Struct)
    assert arti_model.name == MyModel.__name__
    for field_name in MyModel.__fields__:
        assert field_name in arti_model.fields

    back_to_pydantic = pydantic_type_system.to_system(arti_model)

    assert issubclass(back_to_pydantic, pydantic.BaseModel)
    assert back_to_pydantic.__name__ == MyModel.__name__
    assert back_to_pydantic.__annotations__ == MyModel.__annotations__


def test_pydantic_nested_model() -> None:
    arti_model = pydantic_type_system.to_artigraph(NestedModel)
    assert isinstance(arti_model, Struct)

    back_to_pydantic = pydantic_type_system.to_system(arti_model)

    for k, v in NestedModel.__fields__.items():
        if issubclass(v.type_, pydantic.BaseModel):
            # verify that BaseModel fields get nested as Structs
            assert isinstance(arti_model.fields[k], Struct)

            # verify that these nested Structs are turned back to BaseModel fields
            assert issubclass(back_to_pydantic.__fields__[k].type_, pydantic.BaseModel)

        elif issubclass(v.type_, Type):
            # verify arti Types are passed through
            assert back_to_pydantic.__fields__[k].type_ == v.type_
