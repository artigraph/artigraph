from typing import ClassVar

import pytest
from pydantic import ValidationError

from arti import View, types
from arti.types.python import python_type_system


@pytest.fixture
def MockView() -> type[View]:
    class V(View):
        _abstract_ = True
        _by_python_type_: ClassVar[dict[type, type[View]]] = {}
        type_system = python_type_system

    return V


def test_View_init() -> None:
    with pytest.raises(ValidationError, match="cannot be instantiated directly"):
        View()


def test_View_registry(MockView: type[View]) -> None:
    class Int(MockView):  # type: ignore
        python_type = int

    class Int2(MockView):  # type: ignore
        priority = Int.priority + 1
        python_type = int

    class Str(MockView):  # type: ignore
        python_type = str

    assert MockView._by_python_type_ == {int: Int2, str: Str}


def test_View_get_class_for(MockView: type[View]) -> None:
    with pytest.raises(ValueError, match="cannot be matched to a View, try setting one explicitly"):
        MockView.get_class_for(list)

    class List(MockView):  # type: ignore
        python_type = list

    for (annotation, validation_type) in [
        (list, None),
        (list, types.List(element=types.Int64())),
        (list[int], None),
        (list[int], types.List(element=types.Int64())),
    ]:
        assert MockView.get_class_for(annotation, validation_type=validation_type) is List

    with pytest.raises(ValueError, match="list'> cannot be used to represent Float64"):
        MockView.get_class_for(list, validation_type=types.Float64())
