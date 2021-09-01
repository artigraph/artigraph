import re
from datetime import date

import pytest

from arti.partitions import DateKey, IntKey, NullKey, PartitionKey, key_component


def test_PartitionKey_key_components() -> None:
    components = PartitionKey.key_components
    assert isinstance(components, frozenset)
    assert components == frozenset()

    components = IntKey.key_components
    assert isinstance(components, frozenset)
    assert components == {"hex"}

    assert isinstance(IntKey.hex, key_component)
    assert isinstance(IntKey.hex, property)


def test_DateKey() -> None:
    k = DateKey(key=date(1970, 1, 1))
    assert k.Y == 1970
    assert k.m == 1
    assert k.d == 1
    assert k.iso == "1970-01-01"

    assert k == DateKey.from_key_components(Y="1970", m="1", d="1")
    assert k == DateKey.from_key_components(Y="1970", m="01", d="01")
    assert k == DateKey.from_key_components(iso="1970-01-01")
    assert k == DateKey.from_key_components(key="1970-01-01")
    with pytest.raises(
        NotImplementedError,
        match=re.escape("Unable to parse 'DateKey' from: {'junk': 'abc'}"),
    ):
        DateKey.from_key_components(junk="abc")


def test_IntKey() -> None:
    k = IntKey(key=1)
    assert k.hex == "0x1"

    assert k == IntKey.from_key_components(key="1")
    assert k == IntKey.from_key_components(hex="0x1")

    with pytest.raises(
        NotImplementedError,
        match=re.escape("Unable to parse 'IntKey' from: {'junk': 'abc'}"),
    ):
        IntKey.from_key_components(junk="abc")


def test_NullKey() -> None:
    k = NullKey()
    assert k.key is None

    assert k == NullKey.from_key_components(key="None")

    with pytest.raises(
        NotImplementedError, match=re.escape("Unable to parse 'NullKey' from: {'junk': 'abc'}")
    ):
        NullKey.from_key_components(junk="abc")
    with pytest.raises(ValueError, match="'NullKey' can only be used with 'None'!"):
        NullKey.from_key_components(key="abc")
