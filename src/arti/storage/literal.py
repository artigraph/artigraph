from typing import Optional

from pydantic import validator

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes, PartitionKey
from arti.storage import InputFingerprints, Storage, StoragePartition
from arti.types import Type

_cannot_be_partitioned_err = ValueError("Literal storage cannot be partitioned")
_not_written_err = FileNotFoundError("Literal has not been written yet")


class StringLiteralPartition(StoragePartition):
    value: Optional[str]

    @validator("keys")
    @classmethod
    def _no_partition_keys(cls, value: CompositeKey) -> CompositeKey:
        if value:
            raise _cannot_be_partitioned_err
        return value

    def compute_content_fingerprint(self) -> Fingerprint:
        if self.value is None:
            raise _not_written_err
        return Fingerprint.from_string(self.value)


class StringLiteral(Storage[StringLiteralPartition]):
    """StringLiteral stores a literal String value directly in the Backend."""

    value: Optional[str]

    @property
    def _format_fields(self) -> frozendict[str, str]:
        return frozendict[str, str]()

    def discover_partitions(
        self,
        key_types: CompositeKeyTypes,
        input_fingerprints: InputFingerprints = InputFingerprints(),
    ) -> tuple[StringLiteralPartition, ...]:
        if key_types:
            raise _cannot_be_partitioned_err
        if input_fingerprints and self.value is not None:
            raise ValueError(
                f"Literal storage cannot have a `value` preset ({self.value}) for a Producer output"
            )
        if self.value is None:
            return ()
        return (StringLiteralPartition(keys=CompositeKey(), value=self.value),)

    def supports(self, type_: Type, format: Format) -> None:
        if bool(PartitionKey.types_from(type_)):
            raise _cannot_be_partitioned_err
