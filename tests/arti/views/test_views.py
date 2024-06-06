from typing import Annotated, ClassVar

import pytest

from arti import Artifact, TypeSystem, View, types
from arti.types import Int64
from arti.types.python import python_type_system


@pytest.fixture()
def MockView() -> type[View]:
    class V(View):
        _abstract_ = True
        _by_python_type_: ClassVar[dict[type | None, type[View]]] = {}

        # NOTE: The type hint is needed to fix https://github.com/pydantic/pydantic/issues/1777#issuecomment-1465026331
        type_system: ClassVar[TypeSystem] = python_type_system

    return V


def test_View_serialization(MockView: type[View]) -> None:
    class Int(MockView):
        python_type = int

    v = Int(type=Int64(), mode="READ")
    assert v.model_dump(include="artifact_class") == {"artifact_class": Artifact}
    assert v.model_dump_json(include="artifact_class") == '{"artifact_class":"Artifact"}'


def test_View_registry(MockView: type[View]) -> None:
    class Int(MockView):
        python_type = int

    class Int2(MockView):
        priority = Int.priority + 1
        python_type = int

    class Str(MockView):
        python_type = str

    assert MockView._by_python_type_ == {int: Int2, str: Str}


def test_View_get_class_for(MockView: type[View]) -> None:
    with pytest.raises(ValueError, match="cannot be matched to a View, try setting one explicitly"):
        MockView.get_class_for(list)

    class List(MockView):
        python_type = list

    for annotation in [list, list[int]]:
        assert MockView.get_class_for(annotation) == List


def test_View_from_annotation(MockView: type[View]) -> None:
    with pytest.raises(ValueError, match="cannot be matched to a View, try setting one explicitly"):
        MockView.from_annotation(list, mode="READ")

    class List(MockView):
        python_type = list

    int_list = types.List(element=types.Int64())
    for annotation, hint_type, expected_type in [
        (list, int_list, int_list),
        (list[int], None, int_list),
        (list[int], int_list, int_list),
    ]:
        output = MockView.from_annotation(Annotated[annotation, hint_type], mode="READ")
        assert output == List(artifact_class=Artifact, mode="READ", type=expected_type)

    with pytest.raises(ValueError, match="cannot be used to represent Float64"):
        MockView.from_annotation(Annotated[list[int], types.Float64()], mode="READ")
