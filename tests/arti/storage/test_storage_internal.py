import re

import pytest

from arti.partitions import IntKey, PartitionKey
from arti.storage._internal import (
    WildcardFormatDict,
    WildcardPlaceholder,
    parse_partition_keys,
    spec_to_wildcard,
)


@pytest.mark.parametrize(
    ("partition_key_type", "attribute", "valid_attributes"),
    (
        (IntKey, "key", {"hex", "key"}),
        (IntKey, "hex", {"hex", "key"}),
    ),
)
def test_WildcardPlaceholder(
    partition_key_type: type[PartitionKey], attribute: str, valid_attributes: set[str]
) -> None:
    placeholder = WildcardPlaceholder(name="test", key_type=partition_key_type)
    assert placeholder._name == "test"
    assert placeholder._key_type == partition_key_type
    assert placeholder._valid_attributes == valid_attributes
    assert placeholder._attribute is None
    assert getattr(placeholder, attribute) is placeholder
    assert placeholder._attribute == attribute
    assert str(placeholder) == "*"
    # We don't currently validate the indexed values (eg: these aren't valid hex)
    assert placeholder[5] == 5
    assert placeholder[10] == 10

    placeholder = WildcardPlaceholder(name="test", key_type=partition_key_type)
    getattr(placeholder, attribute)
    with pytest.raises(
        ValueError, match=f"'test.{attribute}.{attribute}' cannot be used in a partition path"
    ):
        getattr(placeholder, attribute)

    placeholder = WildcardPlaceholder(name="test", key_type=partition_key_type)
    with pytest.raises(
        AttributeError,
        match=f"'{partition_key_type.__name__}' has no field or key component 'abc123'",
    ):
        placeholder.abc123

    placeholder = WildcardPlaceholder(name="test", key_type=partition_key_type)
    with pytest.raises(
        ValueError,
        match="'test' cannot be used directly in a partition path; access one of the key components",
    ):
        str(placeholder)
    with pytest.raises(
        ValueError,
        match="'test' cannot be used directly in a partition path; access one of the key components",
    ):
        placeholder[5]


def test_WildcardFormatDict() -> None:
    d = WildcardFormatDict({"test": IntKey}, tag="x")
    test_placeholder = d["test"]
    assert isinstance(test_placeholder, WildcardPlaceholder)
    assert test_placeholder._name == "test"
    assert test_placeholder._key_type == IntKey
    assert d["tag"] == "x"

    with pytest.raises(
        ValueError, match=re.escape("No 'junk' partition key found, expected one of ('test',)")
    ):
        d["junk"]


def test_spec_to_wildcard() -> None:
    PKS = {"x": IntKey, "y": IntKey}
    assert spec_to_wildcard("/p/{x.key}/", PKS) == "/p/*/"
    assert spec_to_wildcard("/p/{x.key[5]}/", PKS) == "/p/5/"
    assert spec_to_wildcard("/p/{x.key[5]}/{y.hex}/", PKS) == "/p/5/*/"


def test_parse_partition_keys() -> None:
    PKS = {"x": IntKey, "y": IntKey}

    pks = parse_partition_keys(
        {"/p/1/0x1", "/p/2/0x2", "/p/3/0x3"}, spec="/p/{x.key}/{y.hex}", key_types=PKS
    )
    assert pks == {
        "/p/1/0x1": {"x": IntKey(key=1), "y": IntKey(key=1)},
        "/p/2/0x2": {"x": IntKey(key=2), "y": IntKey(key=2)},
        "/p/3/0x3": {"x": IntKey(key=3), "y": IntKey(key=3)},
    }

    with pytest.raises(
        ValueError, match=re.escape("Unable to parse '/p/1/' with '/p/{x.key}/{y.hex}'")
    ):
        parse_partition_keys({"/p/1/"}, spec="/p/{x.key}/{y.hex}", key_types=PKS)
    with pytest.raises(
        ValueError,
        match=re.escape("Expected to find partition keys for ['x', 'y'], only found ['x']."),
    ):
        parse_partition_keys({"/p/1/"}, spec="/p/{x.key}/", key_types=PKS)
