import pytest
from pydantic import ValidationError

from arti.views.core import View


def test_View() -> None:
    with pytest.raises(ValidationError, match="cannot be instantiated directly"):
        View()

    class V(View):
        pass
