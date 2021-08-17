from collections.abc import Mapping, Sequence
from typing import Literal, Optional, Tuple, Union, get_origin

import pytest

from arti.internal.type_hints import is_union, lenient_issubclass


def test_lenient_issubclass() -> None:
    class MyTuple(tuple):  # type: ignore
        pass

    assert lenient_issubclass(MyTuple, tuple)
    assert lenient_issubclass(tuple, Tuple)  # type: ignore
    assert lenient_issubclass(tuple, tuple)
    assert not lenient_issubclass(Tuple, tuple)
    assert not lenient_issubclass(list, tuple)
    assert not lenient_issubclass(tuple[int], tuple)
    # These raise a `TypeError: issubclass() arg 1 must be a class` for stock `issubclass` (but
    # oddly, `issubclass(tuple[int], tuple)` does not).
    #
    # It might be nice for them to return True, but we will likely need to handle Generics more
    # specifically anyway.
    assert not lenient_issubclass(Optional[int], int)
    assert not lenient_issubclass(Tuple[int], tuple)
    assert not lenient_issubclass(dict[str, int], Mapping)
    assert not lenient_issubclass(tuple[int], Sequence)

    assert not lenient_issubclass(5, 5)  # type: ignore
    with pytest.raises(TypeError, match="arg 2 must be a class or tuple of classes"):
        lenient_issubclass(tuple, 5)  # type: ignore
    with pytest.raises(TypeError, match="argument 2 cannot be a parameterized generic"):
        lenient_issubclass(tuple, tuple[int])


def test_is_union() -> None:
    assert is_union(Union)
    assert not is_union(Literal)

    assert is_union(get_origin(Union[int, str]))
    assert not is_union(Union[int, str])

    assert not is_union(Optional[int])
    assert is_union(get_origin(Optional[int]))
