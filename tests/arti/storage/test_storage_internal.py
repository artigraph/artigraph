import re

import parse
import pytest

from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, IntKey, PartitionKey
from arti.storage._internal import (
    WildcardFormatDict,
    WildcardPlaceholder,
    extract_partition_keys,
    parse_partition_keys,
    spec_to_wildcard,
)


@pytest.fixture
def PKs() -> dict[str, type[PartitionKey]]:
    return {"x": IntKey, "y": IntKey}


@pytest.fixture
def spec() -> str:
    return "/p/{x.key}/{y.hex}"


@pytest.fixture
def paths() -> set[str]:
    return {"/p/1/0x1", "/p/2/0x2", "/p/3/0x3"}


@pytest.fixture
def paths_to_keys() -> dict[str, CompositeKey]:
    return {
        "/p/1/0x1": frozendict({"x": IntKey(key=1), "y": IntKey(key=1)}),
        "/p/2/0x2": frozendict({"x": IntKey(key=2), "y": IntKey(key=2)}),
        "/p/3/0x3": frozendict({"x": IntKey(key=3), "y": IntKey(key=3)}),
    }


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


def test_spec_to_wildcard(PKs: dict[str, type[PartitionKey]]) -> None:
    assert spec_to_wildcard("/p/{x.key}/", PKs) == "/p/*/"
    assert spec_to_wildcard("/p/{x.key[5]}/", PKs) == "/p/5/"
    assert spec_to_wildcard("/p/{x.key[5]}/{y.hex}/", PKs) == "/p/5/*/"


def test_extract_partition_keys(
    PKs: dict[str, type[PartitionKey]],
    spec: str,
    paths: set[str],
    paths_to_keys: dict[str, CompositeKey],
) -> None:
    parser = parse.compile(spec, case_sensitive=True)

    assert {
        path: extract_partition_keys(
            key_types=PKs,
            parser=parser,
            path=path,
            spec=spec,
        )
        for path in paths
    } == paths_to_keys
    with pytest.raises(
        ValueError, match=re.escape("Unable to parse 'fake' with '/p/{x.key}/{y.hex}'.")
    ):
        extract_partition_keys(
            key_types=PKs,
            parser=parser,
            path="fake",
            spec=spec,
        )
    assert (
        extract_partition_keys(
            error_on_no_match=False,
            key_types=PKs,
            parser=parser,
            path="fake",
            spec=spec,
        )
        is None
    )


def test_parse_partition_keys(
    PKs: dict[str, type[PartitionKey]],
    spec: str,
    paths: set[str],
    paths_to_keys: dict[str, CompositeKey],
) -> None:
    pks = parse_partition_keys(paths, spec=spec, key_types=PKs)
    assert pks == paths_to_keys

    with pytest.raises(
        ValueError, match=re.escape("Unable to parse '/p/1/' with '/p/{x.key}/{y.hex}'")
    ):
        parse_partition_keys({"/p/1/"}, spec="/p/{x.key}/{y.hex}", key_types=PKs)
    with pytest.raises(
        ValueError,
        match=re.escape("Expected to find partition keys for ['x', 'y'], only found ['x']."),
    ):
        parse_partition_keys({"/p/1/"}, spec="/p/{x.key}/", key_types=PKs)
