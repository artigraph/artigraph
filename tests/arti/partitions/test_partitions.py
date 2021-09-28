import re
from datetime import date
from typing import ClassVar

import pytest

from arti.internal.utils import frozendict
from arti.partitions import (
    DateKey,
    Int8Key,
    Int16Key,
    Int32Key,
    Int64Key,
    NullKey,
    PartitionKey,
    _IntKey,
    key_component,
)
from arti.types import Date, Int8, Int16, Int32, Int64, List, Struct, Type


def test_PartitionKey_key_components() -> None:
    components = PartitionKey.key_components
    assert isinstance(components, frozenset)
    assert components == frozenset()

    components = Int8Key.key_components
    assert isinstance(components, frozenset)
    assert components == {"hex"}

    assert isinstance(Int8Key.hex, key_component)
    assert isinstance(Int8Key.hex, property)


def test_PartitionKey_lookup_from_type() -> None:
    assert PartitionKey.get_class_for(Int8()) is Int8Key
    assert PartitionKey.get_class_for(Int16()) is Int16Key
    assert PartitionKey.get_class_for(Date()) is DateKey

    assert PartitionKey.types_from(Int8()) == frozendict()
    assert PartitionKey.types_from(Struct(fields={"date": Date(), "i": Int8()})) == frozendict()
    assert PartitionKey.types_from(
        List(element=Struct(fields={"date": Date(), "i": Int8()}), partition_by=("date", "i"))
    ) == frozendict({"date": DateKey, "i": Int8Key})


def test_PartitionKey_subclass() -> None:
    class AbstractKey(PartitionKey):
        _abstract_ = True
        _by_type_: ClassVar[dict[type[Type], type[PartitionKey]]] = {}

    with pytest.raises(TypeError, match="must set `matching_type`"):

        class JunkKey(AbstractKey):
            pass

    class SomeKey(AbstractKey):
        matching_type = Int8

    assert AbstractKey._by_type_ == {Int8: SomeKey}
    assert SomeKey not in PartitionKey._by_type_.values()


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


@pytest.mark.parametrize(
    ["IntKey", "matching_type"],
    (
        (Int8Key, Int8),
        (Int16Key, Int16),
        (Int32Key, Int32),
        (Int64Key, Int64),
    ),
)
def test_IntKeys(IntKey: type[_IntKey], matching_type: type[Type]) -> None:
    assert IntKey.matching_type is matching_type

    k = IntKey(key=1)
    assert k.hex == "0x1"

    assert k == IntKey.from_key_components(key="1")
    assert k == IntKey.from_key_components(hex="0x1")

    with pytest.raises(
        NotImplementedError,
        match=re.escape(f"Unable to parse '{IntKey.__name__}' from: {{'junk': 'abc'}}"),
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
