from itertools import permutations
from typing import Any, cast

import pytest

from arti import Fingerprint


def test_Fingerprint() -> None:
    assert Fingerprint(5) == 5
    assert Fingerprint.from_int(-5) == -5
    assert Fingerprint.from_string("OK") == -7962813320811223369
    assert Fingerprint.from_string("ok") == 5227454011934222951
    assert Fingerprint.identity() == 0

    assert repr(Fingerprint(-5)) == "Fingerprint(-5)"
    assert str(Fingerprint(-5)) == "-5"
    assert Fingerprint(-(2**63)) == -(2**63)
    assert Fingerprint((2**63) - 1) == (2**63) - 1
    for out_of_range in (-(2**63) - 1, 2**63):
        with pytest.raises(ValueError, match="is out of range for int64"):
            Fingerprint.from_int(out_of_range)

    f1, f2 = Fingerprint(1), Fingerprint(2)
    other: object = "1"
    assert f1 != other
    assert f1 != f2
    assert not f1.is_identity
    assert Fingerprint.identity().is_identity


def test_Fingerprint_math() -> None:
    f1, f2, f3, f4, f5 = (Fingerprint(i) for i in range(5))
    # associative
    assert f1.combine(f2.combine(f3)) == f3.combine(f1.combine(f2))
    # commutative
    combined = f1.combine(f2, f3, f4, f5)
    for permutation in permutations([f1, f2, f3, f4, f5]):
        head, *tail = permutation
        assert head.combine(*tail) == combined
    # identity
    assert f1.combine(Fingerprint.identity()) == f1
    # self-inverse
    assert f1.combine(f1) == 0
    assert isinstance(f1.combine(f2), Fingerprint)
    # bitwise operators
    assert f2 & 15 == 1
    assert f2 << 15 == 32768
    assert f2 >> 15 == 0
    assert f2 ^ 15 == 14
    assert f2 | 15 == 15

    for val in [None, "a"]:
        with pytest.raises(TypeError):
            f1.combine(cast(Any, val))
