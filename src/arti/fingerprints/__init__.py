from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import operator
from functools import reduce

import farmhash

from arti.internal.utils import int64, uint64


class Fingerprint(int64):
    """Fingerprint represents a unique identity as an int64 value.

    Using an int(64) has a number of convenient properties:
    - can be combined independent of order with XOR
    - can be stored relatively cheaply
    - 0 acts as an "identity" value when combined (5 ^ 0 = 5)
    - is relatively cross-platform (across databases, languages, etc)
    """

    def combine(self, *others: Fingerprint) -> Fingerprint:
        return reduce(operator.xor, others, self)

    @classmethod
    def from_int(cls, x: int, /) -> Fingerprint:
        return cls.from_int64(int64(x))

    @classmethod
    def from_int64(cls, x: int64, /) -> Fingerprint:
        return cls(x)

    @classmethod
    def from_string(cls, x: str, /) -> Fingerprint:
        """Fingerprint an arbitrary string.

        Fingerprints using Farmhash Fingerprint64, converted to int64 via two's complement.
        """
        return cls.from_uint64(uint64(farmhash.fingerprint64(x)))

    @classmethod
    def from_uint64(cls, x: uint64, /) -> Fingerprint:
        return cls.from_int64(int64(x))

    @classmethod
    def identity(cls) -> Fingerprint:
        """Return a Fingerprint that, when combined, will return the other Fingerprint."""
        return cls(0)

    @property
    def is_identity(self) -> bool:
        return self == 0
