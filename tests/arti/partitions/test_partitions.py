import re
from datetime import date
from typing import ClassVar

import pytest

from arti import PartitionField, Type
from arti.internal.utils import frozendict
from arti.partitions import (
    DateField,
    Int8Field,
    Int16Field,
    Int32Field,
    Int64Field,
    NullField,
    PartitionKey,
    _IntField,
    field_component,
)
from arti.types import Collection, Date, Int8, Int16, Int32, Int64, Struct


def test_PartitionField_components() -> None:
    components = PartitionField.components
    assert isinstance(components, frozenset)
    assert components == frozenset()

    components = Int8Field.components
    assert isinstance(components, frozenset)
    assert components == {"key", "hex"}

    assert isinstance(Int8Field.hex, field_component)
    assert isinstance(Int8Field.hex, property)


def test_PartitionField_get_class_for() -> None:
    assert PartitionField.get_class_for(Int8()) is Int8Field
    assert PartitionField.get_class_for(Int16()) is Int16Field
    assert PartitionField.get_class_for(Date()) is DateField


def test_PartitionField_subclass() -> None:
    class AbstractKey(PartitionField):
        _abstract_ = True
        _by_type_: ClassVar[dict[type[Type], type[PartitionField]]] = {}

        key: int

    with pytest.raises(TypeError, match="NoDefaultKey must set `default_components`"):

        class NoDefaultKey(AbstractKey):
            pass

    with pytest.raises(TypeError, match="NoMatchingTypeKey must set `matching_type`"):

        class NoMatchingTypeKey(AbstractKey):
            default_components: ClassVar[frozendict[str, str]] = frozendict(key="key")

    with pytest.raises(
        TypeError,
        match=re.escape(r"Unknown components in UnknownDefaultKey.default_components: {'junk'}"),
    ):

        class UnknownDefaultKey(AbstractKey):
            default_components: ClassVar[frozendict[str, str]] = frozendict(junk="junk")
            matching_type: ClassVar[type[Type]] = Int8

    class SomeKey(AbstractKey):
        default_components: ClassVar[frozendict[str, str]] = frozendict(key="key")
        matching_type: ClassVar[type[Type]] = Int8

    assert AbstractKey._by_type_ == {Int8: SomeKey}
    assert SomeKey not in PartitionField._by_type_.values()


def test_DateField() -> None:
    k = DateField(key=date(1970, 1, 1))
    assert k.Y == 1970
    assert k.m == 1
    assert k.d == 1
    assert k.iso == "1970-01-01"

    assert k == DateField.from_components(Y="1970", m="1", d="1")
    assert k == DateField.from_components(Y="1970", m="01", d="01")
    assert k == DateField.from_components(iso="1970-01-01")
    assert k == DateField.from_components(key="1970-01-01")
    with pytest.raises(
        NotImplementedError,
        match=re.escape("Unable to parse 'DateField' from: {'junk': 'abc'}"),
    ):
        DateField.from_components(junk="abc")


@pytest.mark.parametrize(
    ("IntKey", "matching_type"),
    [
        (Int8Field, Int8),
        (Int16Field, Int16),
        (Int32Field, Int32),
        (Int64Field, Int64),
    ],
)
def test_IntFields(IntKey: type[_IntField], matching_type: type[Type]) -> None:
    assert IntKey.matching_type is matching_type

    k = IntKey(key=1)
    assert k.hex == "0x1"

    assert k == IntKey.from_components(key="1")
    assert k == IntKey.from_components(hex="0x1")

    with pytest.raises(
        NotImplementedError,
        match=re.escape(f"Unable to parse '{IntKey.__name__}' from: {{'junk': 'abc'}}"),
    ):
        IntKey.from_components(junk="abc")


def test_NullField() -> None:
    k = NullField()
    assert k.key is None

    assert k == NullField.from_components(key="None")

    with pytest.raises(
        NotImplementedError, match=re.escape("Unable to parse 'NullField' from: {'junk': 'abc'}")
    ):
        NullField.from_components(junk="abc")
    with pytest.raises(ValueError, match="'NullField' can only be used with 'None'!"):
        NullField.from_components(key="abc")


def test_PartitionKey_types_frome() -> None:
    assert PartitionKey.types_from(Int8()) == frozendict()
    assert PartitionKey.types_from(Struct(fields={"date": Date(), "i": Int8()})) == frozendict()
    assert PartitionKey.types_from(
        Collection(element=Struct(fields={"date": Date(), "i": Int8()}), partition_by=("date", "i"))
    ) == frozendict({"date": DateField, "i": Int8Field})
