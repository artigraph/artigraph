from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import operator
from collections.abc import Callable
from functools import reduce
from typing import Optional, Union

import farmhash

from arti.internal.models import Model
from arti.internal.utils import int64, uint64


def _gen_fingerprint_binop(
    op: Callable[[int, int], int]
) -> Callable[[Fingerprint, Union[int, Fingerprint]], Fingerprint]:
    def _fingerprint_binop(self: Fingerprint, other: Union[int, Fingerprint]) -> Fingerprint:
        if isinstance(other, int):
            other = Fingerprint.from_int(other)
        if isinstance(other, Fingerprint):
            if self.key is None or other.key is None:
                return Fingerprint.empty()
            return Fingerprint(key=op(self.key, other.key))
        return NotImplemented

    return _fingerprint_binop


class Fingerprint(Model):
    """Fingerprint represents a unique identity as an int64 value.

    Using an int(64) has a number of convenient properties:
    - can be combined independent of order with XOR
    - can be stored relatively cheaply
    - empty 0 values drop out when combined (5 ^ 0 = 5)
    - is relatively cross-platform (across databases, languages, etc)

    There are two "special" Fingerprints w/ factory functions that, when combined with other
    Fingerprints:
    - `empty()`: returns `empty()`
    - `identity()`: return the other Fingerprint
    """

    key: Optional[int64]

    def combine(self, *others: Fingerprint) -> Fingerprint:
        return reduce(operator.xor, others, self)

    @classmethod
    def empty(cls) -> Fingerprint:
        """Return a Fingerprint that, when combined, will return Fingerprint.empty()"""
        return cls(key=None)

    @classmethod
    def from_int(cls, x: int, /) -> Fingerprint:
        return cls.from_int64(int64(x))

    @classmethod
    def from_int64(cls, x: int64, /) -> Fingerprint:
        return cls(key=x)

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
        return cls(key=int64(0))

    @property
    def is_empty(self) -> bool:
        return self.key is None

    @property
    def is_identity(self) -> bool:
        return self.key == 0

    __and__ = _gen_fingerprint_binop(operator.__and__)
    __lshift__ = _gen_fingerprint_binop(operator.__lshift__)
    __or__ = _gen_fingerprint_binop(operator.__or__)
    __rshift__ = _gen_fingerprint_binop(operator.__rshift__)
    __xor__ = _gen_fingerprint_binop(operator.__xor__)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, int):
            other = Fingerprint.from_int(other)
        if isinstance(other, Fingerprint):
            return self.key == other.key
        return NotImplemented
