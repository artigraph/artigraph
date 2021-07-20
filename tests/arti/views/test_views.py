from __future__ import annotations

from typing import ClassVar

import pytest
from pydantic import ValidationError

from arti.views.core import View


def test_View() -> None:
    with pytest.raises(ValidationError, match="cannot be instantiated directly"):
        View()


def test_View_registry() -> None:
    class V(View):
        _abstract_ = True
        _registry_: ClassVar[dict[type, type[View]]] = {}

    class Int(V):
        python_type = int

    class Int2(V):
        python_type = int
        priority = Int.priority + 1

    class Str(V):
        python_type = str

    assert V._registry_ == {int: Int2, str: Str}
