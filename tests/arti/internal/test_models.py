from typing import ClassVar

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
