from itertools import permutations

import pytest

from arti import Fingerprint
from arti.internal.utils import int64, uint64


def test_Fingerprint() -> None:
    assert Fingerprint(key=int64(5)).key == 5
    assert Fingerprint.empty().key is None
    assert Fingerprint.from_int(-5) == -5
    assert Fingerprint.from_int(None) == Fingerprint.empty()
    assert Fingerprint.from_int64(None) == Fingerprint.empty()
    assert Fingerprint.from_int64(int64(-5)) == -5
    assert Fingerprint.from_string("OK") == -7962813320811223369
    assert Fingerprint.from_string("ok") == 5227454011934222951
    assert Fingerprint.from_string(None) == Fingerprint.empty()
    assert Fingerprint.from_uint64(None) == Fingerprint.empty()
    assert Fingerprint.from_uint64(uint64(5)) == 5
    assert Fingerprint.from_uint64(uint64(int64(-5))) == -5
    assert Fingerprint.identity() == 0

    with pytest.raises(ValueError, match="is too large for int64"):
        Fingerprint.from_int(uint64._max)

    f1, f2 = Fingerprint(key=int64(1)), Fingerprint(key=int64(2))
    assert f1 != "1"
    assert f1 != f2
    assert not f1.is_empty
    assert not f1.is_identity
    assert Fingerprint.empty().is_empty
    assert Fingerprint.identity().is_identity
    assert not Fingerprint.empty().is_identity
    assert not Fingerprint.identity().is_empty

    with pytest.warns(match="returns itself"):
        assert f1.fingerprint is f1


def test_Fingerprint_math() -> None:
    f1, f2, f3, f4, f5 = (Fingerprint(key=int64(i)) for i in range(5))
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
    # empty cascades
    assert Fingerprint.empty().combine(f1).is_empty
    assert f1.combine(Fingerprint.empty()).is_empty
    # bitwise operators
    assert f2 & 15 == 1
    assert f2 << 15 == 32768
    assert f2 >> 15 == 0
    assert f2 ^ 15 == 14
    assert f2 | 15 == 15

    for val in [None, "a"]:
        with pytest.raises(TypeError):
            f1.combine(val)  # type: ignore[arg-type]
