import pytest

from arti.formats.parquet import Parquet
from arti.types import Set, String


def test_parquet_format() -> None:
    format = Parquet()

    assert format.extension == ".parquet"
    with pytest.raises(NotImplementedError, match=r"No TypeSystem.* adapter"):
        format.visit_type(Set(element=String()))
