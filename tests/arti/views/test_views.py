import pytest
from pydantic import ValidationError

from arti.artifacts.core import Artifact
from arti.views.core import View


def test_View() -> None:
    with pytest.raises(ValidationError, match="cannot be instantiated directly"):
        View()

    class V(View):
        pass

    v = V()
    with pytest.raises(ValueError, match="doesn't have any data to write"):
        v.write(Artifact())
