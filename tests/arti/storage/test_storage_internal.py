import re
from typing import Optional

import parse
import pytest

from arti.fingerprints import Fingerprint
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, CompositeKeyTypes, Int8Key, PartitionKey
from arti.storage._internal import (
    FormatDict,
    FormatPlaceholder,
    WildcardPlaceholder,
    extract_placeholders,
    parse_spec,
    partial_format,
    spec_to_wildcard,
    strip_partition_indexes,
)

PathPlaceholders = dict[str, tuple[Optional[Fingerprint], CompositeKey]]


@pytest.fixture
def PKs() -> dict[str, type[PartitionKey]]:
    return {"x": Int8Key, "y": Int8Key}


@pytest.fixture
def spec() -> str:
    return "/p/{x.key}/{y.hex}"


@pytest.fixture
def paths() -> set[str]:
    return {"/p/1/0x1", "/p/2/0x2", "/p/3/0x3"}


@pytest.fixture
def paths_to_placeholders() -> PathPlaceholders:
    return {
        "/p/1/0x1": (Fingerprint.empty(), frozendict({"x": Int8Key(key=1), "y": Int8Key(key=1)})),
        "/p/2/0x2": (Fingerprint.empty(), frozendict({"x": Int8Key(key=2), "y": Int8Key(key=2)})),
        "/p/3/0x3": (Fingerprint.empty(), frozendict({"x": Int8Key(key=3), "y": Int8Key(key=3)})),
    }


@pytest.mark.parametrize(
    ("spec", "key"),
    (
        ("baseline", None),
        ("{}", None),
        ("{a}", "a"),
        ("{a.5}", "a"),
        ("{a[1]}", "a"),
        ("{a[b]}", "a"),
        ("{a[b].c}", "a"),
        ("{a[b].c[d]}", "a"),
        ("{a[b].c[d]:2d}", "a"),
        ("hello {world}", "world"),
    ),
)
def test_FormatPlaceholder(spec: str, key: str) -> None:
    if key:
        out = spec.format(**{key: FormatPlaceholder(key)})
    else:
        out = spec.format(FormatPlaceholder(""))
    assert out == spec


@pytest.mark.parametrize(
    ("key", "partition_key_types", "attribute"),
    (
        ("test", CompositeKeyTypes(test=Int8Key), "key"),
        ("test", CompositeKeyTypes(test=Int8Key), "hex"),
    ),
)
def test_WildcardPlaceholder(
    key: str, partition_key_types: CompositeKeyTypes, attribute: str
) -> None:
    partition_key_type = partition_key_types[key]

    placeholder = WildcardPlaceholder(key, partition_key_types)
    assert placeholder._key == key
    assert placeholder._key_type == partition_key_type
    assert placeholder._attribute is None
    assert getattr(placeholder, attribute) is placeholder
    assert placeholder._attribute == attribute
    assert str(placeholder) == "*"
    # We don't currently validate the indexed values (eg: these aren't valid hex)
    assert placeholder[5] == 5
    assert placeholder[10] == 10

    partial = WildcardPlaceholder.with_key_types(partition_key_types)
    applied = partial(key)
    assert applied._key == key
    assert applied._key_type == partition_key_type

    placeholder = WildcardPlaceholder(key, partition_key_types)
    getattr(placeholder, attribute)
    with pytest.raises(
        ValueError, match=f"'{key}.{attribute}.{attribute}' cannot be used in a partition path"
    ):
        getattr(placeholder, attribute)

    placeholder = WildcardPlaceholder(key, partition_key_types)
    with pytest.raises(
        AttributeError,
        match=f"'{partition_key_type.__name__}' has no field or key component 'abc123'",
    ):
        placeholder.abc123

    placeholder = WildcardPlaceholder(key, partition_key_types)
    with pytest.raises(
        ValueError,
        match=f"'{key}' cannot be used directly in a partition path; access one of the key components",
    ):
        str(placeholder)
    with pytest.raises(
        ValueError,
        match=f"'{key}' cannot be used directly in a partition path; access one of the key components",
    ):
        placeholder[5]


def test_FormatDict() -> None:
    d = FormatDict(WildcardPlaceholder.with_key_types({"test": Int8Key}), tag="x")
    test_placeholder = d["test"]
    assert isinstance(test_placeholder, WildcardPlaceholder)
    assert test_placeholder._key == "test"
    assert test_placeholder._key_type == Int8Key
    assert d["tag"] == "x"

    with pytest.raises(
        ValueError, match=re.escape("No 'junk' partition key found, expected one of ('test',)")
    ):
        d["junk"]


@pytest.mark.parametrize(
    ("spec", "expected", "kwargs"),
    (
        ("baseline", "baseline", {}),
        ("{a}", "a", {"a": "a"}),
        ("{a}", "{a}", {}),
        ("{hello} {world}", "hello {world}", {"hello": "hello"}),
        ("{hello} {wo[rld]}", "hello {wo[rld]}", {"hello": "hello"}),
    ),
)
def test_partial_format(spec: str, expected: str, kwargs: dict[str, str]) -> None:
    assert partial_format(spec, **kwargs) == expected


# To support positional args, we would need to introduce a FormatSequence (w/
# __getitem__) similar to FormatDict. Easy enough, but don't need (yet).
@pytest.mark.xfail(raises=IndexError)
def test_partial_format_positional_args() -> None:
    partial_format("{}")


@pytest.mark.parametrize(
    ("spec", "expected"),
    (
        ("baseline", "baseline"),
        ("{a}", "{a}"),
        ("{a[b]}", "{a}"),
    ),
)
def test_strip_partition_indexes(spec: str, expected: str) -> None:
    assert strip_partition_indexes(spec) == expected


def test_spec_to_wildcard(PKs: dict[str, type[PartitionKey]]) -> None:
    assert spec_to_wildcard("/p/{x.key}/", PKs) == "/p/*/"
    assert spec_to_wildcard("/p/{x.key[5]}/", PKs) == "/p/5/"
    assert spec_to_wildcard("/p/{x.key[5]}/{y.hex}/", PKs) == "/p/5/*/"


def test_extract_placeholders(
    PKs: dict[str, type[PartitionKey]],
    spec: str,
    paths: set[str],
    paths_to_placeholders: PathPlaceholders,
) -> None:
    parser = parse.compile(spec, case_sensitive=True)

    assert {
        path: extract_placeholders(
            key_types=PKs,
            parser=parser,
            path=path,
            spec=spec,
        )
        for path in paths
    } == paths_to_placeholders
    with pytest.raises(
        ValueError, match=re.escape("Unable to parse 'fake' with '/p/{x.key}/{y.hex}'.")
    ):
        extract_placeholders(
            key_types=PKs,
            parser=parser,
            path="fake",
            spec=spec,
        )
    assert (
        extract_placeholders(
            error_on_no_match=False,
            key_types=PKs,
            parser=parser,
            path="fake",
            spec=spec,
        )
        == (Fingerprint.empty(), CompositeKey())
    )


def test_parse_spec(
    PKs: dict[str, type[PartitionKey]],
    spec: str,
    paths: set[str],
    paths_to_placeholders: PathPlaceholders,
) -> None:
    pks = parse_spec(paths, key_types=PKs, spec=spec)
    assert pks == paths_to_placeholders

    with pytest.raises(
        ValueError, match=re.escape("Unable to parse '/p/1/' with '/p/{x.key}/{y.hex}'")
    ):
        parse_spec({"/p/1/"}, spec="/p/{x.key}/{y.hex}", key_types=PKs)
    with pytest.raises(
        ValueError,
        match=re.escape("Expected to find partition keys for ['x', 'y'], only found ['x']."),
    ):
        parse_spec({"/p/1/"}, spec="/p/{x.key}/", key_types=PKs)
