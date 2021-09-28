from typing import ClassVar

import pytest
from pydantic import ValidationError

from arti.types.python import python_type_system
from arti.views import View


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

    assert MockView.get_class_for(list) is List
    assert MockView.get_class_for(list[int]) is List
