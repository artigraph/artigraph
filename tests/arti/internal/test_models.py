from typing import ClassVar

import pytest
from pydantic import ValidationError

from arti.internal.models import Model


class Abstract(Model):
    __abstract__: ClassVar[bool] = True


class Concrete(Model):
    pass


def test_Model() -> None:
    assert Concrete()
    for model in (Model, Abstract):
        with pytest.raises(ValidationError, match="cannot be instantiated directly"):
            model()


def test_Model_unknown_kwargs() -> None:
    with pytest.raises(ValidationError, match="extra fields not permitted"):
        Concrete(junk=1)
