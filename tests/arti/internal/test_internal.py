import pytest

from arti.internal import wrap_exc


def test_wrap_exc() -> None:
    root_error = ValueError("test")
    with pytest.raises(ValueError, match="a - test") as exc:
        with wrap_exc(ValueError, prefix="a"):
            raise root_error
    assert exc.value.__cause__ is root_error

    with pytest.raises(ValueError, match="ab - test") as exc:
        with wrap_exc(ValueError, prefix="a"), wrap_exc(ValueError, prefix="b"):
            raise root_error
    assert exc.value.__cause__ is root_error

    with wrap_exc(ValueError, prefix="shouldn't run"):
        x = 5
    assert x == 5
