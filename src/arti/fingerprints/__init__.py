from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from dataclasses import dataclass
from typing import Any, Self, final

import annotated_types
import farmhash
from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


@final
class Fingerprint(int):
    """
    Fingerprint represents a unique identity as an int64 value.

    Using an int(64) has a number of convenient properties:
    - can be combined independent of order with XOR
    - can be stored relatively cheaply
    - 0 acts as an "identity" value when combined (5 ^ 0 = 5)
    - is relatively cross-platform (across databases, languages, etc)
    """

    def __new__(cls, x: int, /) -> Self:
        if not -(2**63) <= x < 2**63:
            raise ValueError(f"{x} is out of range for int64.")
        return super().__new__(cls, x)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({int(self)})"

    def __str__(self) -> str:
        return str(int(self))

    def combine(self, *others: Fingerprint) -> Fingerprint:
        result = int(self)
        for other in others:
            result ^= other
        return Fingerprint(result)

    @classmethod
    def from_int(cls, x: int, /) -> Fingerprint:
        return cls(x)

    @classmethod
    def from_string(cls, x: str, /) -> Fingerprint:
        """
        Fingerprint an arbitrary string.

        Fingerprints using Farmhash Fingerprint64, converted to int64 via two's complement.
        """
        value = farmhash.fingerprint64(x)
        return cls(value - 2**64 if value >= 2**63 else value)

    @classmethod
    def identity(cls) -> Fingerprint:
        """Return a Fingerprint that, when combined, will return the other Fingerprint."""
        return cls(0)

    @property
    def is_identity(self) -> bool:
        return self == 0

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        # TODO: Add LT/GT to limit to 64-bit values.
        return core_schema.no_info_after_validator_function(cls, handler.generate_schema(int))


@dataclass(frozen=True, slots=True)
class SkipFingerprint(annotated_types.BaseMetadata):
    """
    Determine whether the field contributes a model's fingerprint.

    Set with `Annotated`, eg: `x: Annotated[int, SkipFingerprint()]`. Unlike other `Annotated`
    metadata, subclasses that override the field inherit the setting from the nearest parent that
    sets it, unless the override sets it again, eg: `x: Annotated[int, SkipFingerprint(False)]`.
    """

    value: bool = True

    def __bool__(self) -> bool:
        return self.value

    def __eq__(self, other: object) -> bool:
        return self.value == other
