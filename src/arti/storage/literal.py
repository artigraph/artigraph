from pydantic import validator

from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes, PartitionKey
from arti.storage import InputFingerprints, Storage, StoragePartition
from arti.types import Type

_CannotBePartitioned = ValueError("Literal storage cannot be partitioned")
_CannotHaveInputs = ValueError("Literal storage cannot have an `input_fingerprint`")


class StringLiteralPartition(StoragePartition):
    value: str

    @validator("keys")
    @classmethod
    def _no_partition_keys(cls, value: CompositeKey) -> CompositeKey:
        if value:
            raise _CannotBePartitioned
        return value

    @validator("input_fingerprint")
    @classmethod
    def _no_input_fingerprint(cls, value: Fingerprint) -> Fingerprint:
        if not value.is_empty:
            raise _CannotHaveInputs
        return value

    def compute_content_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.value)


class StringLiteral(Storage[StringLiteralPartition]):
    value: str

    @property
    def _format_fields(self) -> frozendict[str, str]:
        return frozendict[str, str]()

    def discover_partitions(
        self,
        key_types: CompositeKeyTypes,
        input_fingerprints: InputFingerprints = InputFingerprints(),
    ) -> tuple[StringLiteralPartition, ...]:
        if key_types:
            raise _CannotBePartitioned
        if input_fingerprints:
            raise _CannotHaveInputs
        return (StringLiteralPartition(keys=CompositeKey(), value=self.value),)

    def supports(self, type_: Type, format: Format) -> None:
        if bool(PartitionKey.types_from(type_)):
            raise _CannotBePartitioned
