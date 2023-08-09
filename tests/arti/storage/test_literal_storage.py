import re

import pytest

from arti import Fingerprint, InputFingerprints, PartitionKey
from arti.storage.literal import StringLiteral, StringLiteralPartition
from arti.types import Collection, Int64, Struct
from tests.arti.dummies import DummyFormat


def test_StringLiteral() -> None:
    t, f = Int64(), DummyFormat()
    literal = StringLiteral(id="test", value="test")._visit_type(t)._visit_format(f)
    partitions = literal.discover_partitions()
    assert len(partitions) == 1
    partition = partitions[0]
    assert isinstance(partition, StringLiteralPartition)
    assert partition.value is not None
    assert partition.value == literal.value
    assert partition.compute_content_fingerprint() == Fingerprint.from_string(partition.value)
    # Confirm value=None returns no partitions
    assert not StringLiteral(id="test")._visit_type(t)._visit_format(f).discover_partitions()
    # Confirm input_fingerprint and partition_key validators don't error for empty values
    assert (
        partition
        == StringLiteralPartition(
            id="test", value="test", storage=literal
        ).with_content_fingerprint()
    )
    # Confirm empty value raises
    with pytest.raises(FileNotFoundError, match="Literal has not been written yet"):
        StringLiteralPartition(id="test", storage=literal).compute_content_fingerprint()


def test_StringLiteral_errors() -> None:
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Literal storage cannot have a `value` preset (test) for a Producer output"
        ),
    ):
        StringLiteral(value="test")._visit_type(Int64()).discover_partitions(
            input_fingerprints=InputFingerprints({PartitionKey(): Fingerprint.from_int(5)}),
        )
    with pytest.raises(
        ValueError,
        match=re.escape("Literal storage can only be partitioned if generated by a Producer."),
    ):
        StringLiteral(value="test")._visit_type(
            Collection(element=Struct(fields={"i": Int64()}), partition_by=("i",)),
        ).discover_partitions(input_fingerprints=InputFingerprints())
