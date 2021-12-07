import re

import pytest

from arti.fingerprints import Fingerprint
from arti.partitions import CompositeKey, CompositeKeyTypes, Int64Key
from arti.storage import InputFingerprints
from arti.storage.literal import StringLiteral, StringLiteralPartition
from arti.types import Collection, Int64, Struct
from tests.arti.dummies import DummyFormat


def test_StringLiteral() -> None:
    literal = StringLiteral(value="test")
    partitions = literal.discover_partitions(key_types=CompositeKeyTypes())
    assert len(partitions) == 1
    partition = partitions[0]
    assert isinstance(partition, StringLiteralPartition)
    assert partition.value is not None
    assert partition.value == literal.value
    assert partition.compute_content_fingerprint() == Fingerprint.from_string(partition.value)
    # Confirm value=None returns no partitions
    assert StringLiteral().discover_partitions(key_types=CompositeKeyTypes()) == ()
    # Confirm keys/input_fingerprint validators don't error for empty values
    assert partition == StringLiteralPartition(
        keys=CompositeKey(), input_fingerprint=Fingerprint.empty(), value="test"
    )
    # Confirm empty value raises
    with pytest.raises(FileNotFoundError, match=""):
        StringLiteralPartition(keys=CompositeKey()).compute_content_fingerprint()


def test_StringLiteral_errors() -> None:
    literal = StringLiteral(value="test")

    literal.supports(Int64(), DummyFormat())
    literal.supports(Collection(element=Struct(fields={"a": Int64()})), DummyFormat())
    with pytest.raises(ValueError, match="Literal storage cannot be partitioned"):
        literal.supports(
            Collection(element=Struct(fields={"a": Int64()}), partition_by=("a",)), DummyFormat()
        )

    value = "test"
    key_types = CompositeKeyTypes(i=Int64Key)
    keys = CompositeKey(i=Int64Key(key=5))
    input_fingerprint = Fingerprint.from_int(5)

    with pytest.raises(ValueError, match="Literal storage cannot be partitioned"):
        literal.discover_partitions(key_types=key_types)

    with pytest.raises(ValueError, match="Literal storage cannot be partitioned"):
        StringLiteralPartition(keys=keys, value=value)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Literal storage cannot have a `value` preset (test) for a Producer output"
        ),
    ):
        literal.discover_partitions(
            key_types=CompositeKeyTypes(),
            input_fingerprints=InputFingerprints({keys: input_fingerprint}),
        )
