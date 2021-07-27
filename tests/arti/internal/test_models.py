from typing import Any, ClassVar, Literal

import pytest
from pydantic import ValidationError

from arti.internal.models import Model


class Abstract(Model):
    _abstract_: ClassVar[bool] = True


class Concrete(Model):
    pass


def test_Model() -> None:
    obj = Concrete()
    assert str(obj) == repr(obj)
    for model in (Model, Abstract):
        with pytest.raises(ValidationError, match="cannot be instantiated directly"):
            model()


def test_Model_unknown_kwargs() -> None:
    with pytest.raises(ValidationError, match="extra fields not permitted"):
        Concrete(junk=1)


def test_Model_static_types() -> None:
    class M(Model):
        a: Any
        b: Literal["b"]
        c: int

    M(a=5, b="b", c=0)
    with pytest.raises(ValidationError, match="unexpected value; permitted: 'b'"):
        M(a=5, b=5, c=0)
    with pytest.raises(ValidationError, match=r"Expected an instance of int, got"):
        M(a=5, b="b", c=0.0)
