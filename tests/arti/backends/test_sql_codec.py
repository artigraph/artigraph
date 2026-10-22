from __future__ import annotations

import json
from contextlib import suppress
from copy import deepcopy
from datetime import UTC, date, datetime, time
from enum import Enum
from pathlib import Path
from typing import Any, cast

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import create_model

from arti import Fingerprint, Graph
from arti.backends.sql.codec import (
    MetadataCodecError,
    decode_document,
    encode_document,
    metadata_type_path,
)
from arti.internal.models import Model
from arti.storage.literal import StringLiteral
from tests.arti.backends.pipeline import Number, add
from tests.arti.dummies import A1, P1


class Color(Enum):
    RED = "red"


class CodecValues(Model):
    blob: bytes
    color: Color
    date: date
    datetime: datetime
    fingerprint_value: Fingerprint
    path: Path
    time: time
    values: frozenset[int]


class Anything(Model):
    value: Any

    def __hash__(self) -> int:
        return 0


NOT_A_TYPE = Color.RED


def _document(value: Any, *models: Any) -> dict[str, Any]:
    return {"codec_version": 2, "models": list(models), "value": value}


def _root(document: dict[str, Any]) -> dict[str, Any]:
    # Models are encoded after the Models they reference, so the root is last.
    return document["models"][-1]


def test_metadata_codec_round_trip() -> None:
    value = CodecValues(
        blob=b"\x00\xff",
        color=Color.RED,
        date=date(2026, 7, 26),
        datetime=datetime(2026, 7, 26, 12, 30, tzinfo=UTC),
        fingerprint_value=Fingerprint.from_int(-10),
        path=Path("artigraph"),
        time=time(12, 30, tzinfo=UTC),
        values=frozenset({1, 2}),
    )
    document = encode_document(value)

    assert document["codec_version"] == 2
    assert decode_document(document, CodecValues) == value


def test_metadata_codec_sorts_sets() -> None:
    # Set order follows the process' hash seed, so documents sort the encoded items.
    letters = [chr(ord("a") + i) for i in range(26)]
    for value in (frozenset(letters), set(letters)):
        assert _root(encode_document(Anything(value=value)))["fields"]["value"]["items"] == letters


def test_metadata_codec_graph_round_trip() -> None:
    with Graph(name="codec") as graph:
        graph.artifacts.input = A1()
        graph.artifacts.output = P1(a1=graph.artifacts.input)

    restored = decode_document(encode_document(graph), Graph)

    assert restored == graph
    assert restored.fingerprint == graph.fingerprint
    assert restored.artifact_to_key == graph.artifact_to_key


def _chain(stages: int) -> Graph:
    """Build a Graph where each Producer reads the previous Artifact twice."""
    with Graph(name="chain") as graph:
        graph.artifacts.x0 = Number(storage=StringLiteral(value="1"))
        for i in range(1, stages + 1):
            previous = cast(Number, graph.artifacts[f"x{i - 1}"])
            graph.artifacts[f"x{i}"] = add(left=previous, right=previous).out()
    return graph


def test_metadata_codec_encodes_shared_models_once() -> None:
    graph = _chain(10)
    document = encode_document(graph)
    restored = decode_document(document, Graph)

    assert restored == graph
    assert restored.fingerprint == graph.fingerprint
    producer = restored.artifacts.x10.producer_output.producer
    assert producer.left is producer.right
    # Repeating shared Artifacts doubled the document for every stage.
    assert len(json.dumps(document)) < 3 * len(json.dumps(encode_document(_chain(5))))


def test_metadata_codec_does_not_merge_equal_models() -> None:
    # Pydantic considers these equal, but the values differ.
    value = Anything(value=(Anything(value=1), Anything(value=True)))

    restored = decode_document(encode_document(value), Anything)

    assert [type(item.value) for item in restored.value] == [int, bool]


@pytest.mark.parametrize(
    "value",
    [
        1.5,
        float("inf"),
        float("-inf"),
        int,
        [1, "two"],
        {"one": 1},
        {1, 2},
    ],
)
def test_metadata_codec_round_trips_additional_structured_values(value: Any) -> None:
    restored = decode_document(encode_document(Anything(value=value)), Anything).value

    assert restored == value


def test_metadata_codec_round_trips_nan() -> None:
    restored = decode_document(encode_document(Anything(value=float("nan"))), Anything).value

    assert isinstance(restored, float)
    assert restored != restored


def test_metadata_codec_round_trips_storage_without_key_types() -> None:
    storage = StringLiteral()

    restored = decode_document(encode_document(storage), StringLiteral)

    with pytest.raises(ValueError, match="have not been set"):
        _ = restored.key_types


def test_metadata_codec_rejects_unimportable_types() -> None:
    class LocalModel(Model):
        pass

    with pytest.raises(MetadataCodecError, match="is not importable"):
        encode_document(LocalModel())
    with pytest.raises(MetadataCodecError, match="is not importable"):
        metadata_type_path(LocalModel)


def test_metadata_codec_rejects_missing_module_binding() -> None:
    missing_model = create_model("MissingModel", __base__=Model, __module__=__name__)

    with pytest.raises(MetadataCodecError, match="is not importable"):
        encode_document(missing_model())


def test_metadata_codec_rejects_rebound_module_path() -> None:
    rebound_model = create_model("Color", __base__=Model, __module__=__name__)

    with pytest.raises(MetadataCodecError, match="resolves to a different object"):
        encode_document(rebound_model())


def test_metadata_codec_rejects_unsupported_values() -> None:
    with pytest.raises(MetadataCodecError, match="Unsupported persisted metadata value"):
        encode_document(Anything(value=object()))


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("version", "Unsupported metadata codec version: 1"),
        ("document-keys", "Unexpected metadata document keys"),
        ("models", "Expected document models items to be a list"),
        ("kind", "Unknown persisted metadata kind"),
        ("index", "Invalid persisted model index"),
        ("forward-index", "Invalid persisted model index"),
        ("type", "Unable to import persisted type"),
        ("fields", "Unknown fields"),
        ("fields-set", "Invalid field"),
        ("state", "Unexpected persisted"),
    ],
)
def test_metadata_codec_rejects_corruption(case: str, message: str) -> None:
    document = deepcopy(
        encode_document(
            CodecValues(
                blob=b"",
                color=Color.RED,
                date=date(2026, 7, 26),
                datetime=datetime(2026, 7, 26),
                fingerprint_value=Fingerprint.from_int(1),
                path=Path("artigraph"),
                time=time(12),
                values=frozenset(),
            )
        )
    )
    value = _root(document)
    match case:
        case "version":
            # Older documents are not migrated.
            document["codec_version"] = 1
        case "document-keys":
            document["extra"] = True
        case "models":
            document["models"] = {}
        case "kind":
            document["value"]["kind"] = "unknown"
        case "index":
            document["value"]["index"] = len(document["models"])
        case "forward-index":
            reference = {"kind": "model", "index": 1}
            document["models"].insert(0, {**value, "fields": {"blob": reference}})
        case "type":
            value["type"] = "missing.module:Missing"
        case "fields":
            fields = cast(dict[str, Any], value["fields"])
            fields["extra"] = 1
        case "fields-set":
            value["fields_set"] = ["extra"]
        case "state":
            value["state"] = {"extra": None}
        case _:  # coverage: ignore
            raise AssertionError(case)

    with pytest.raises(MetadataCodecError, match=message):
        decode_document(document, CodecValues)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ([], "Expected encoded metadata to be a mapping"),
        ({}, "missing a string 'kind'"),
        ({"kind": "model", "index": 0}, "Invalid persisted model index"),
        ({"kind": "model", "index": True}, "Invalid persisted model index"),
        (
            {
                "kind": "typed_box",
                "type": "builtins:dict",
                "items": [],
            },
            "is not a TypedBox",
        ),
        (
            {
                "kind": "mapping",
                "type": "builtins:list",
                "items": [],
            },
            "is not a Mapping",
        ),
        (
            {
                "kind": "enum",
                "type": "builtins:int",
                "name": "missing",
            },
            "is not an Enum",
        ),
        (
            {
                "kind": "enum",
                "type": "tests.arti.backends.test_sql_codec:Color",
                "name": "missing",
            },
            "Unknown persisted Color member",
        ),
        (
            {
                "kind": "int",
                "type": "builtins:str",
                "value": 1,
            },
            "is not an int",
        ),
        ({"kind": "float", "value": "finite"}, "Invalid persisted float"),
        ({"kind": "bytes", "value": "***"}, "Invalid persisted bytes"),
        ({"kind": "date", "value": "invalid"}, "Invalid persisted date"),
        ({"kind": "datetime", "value": "invalid"}, "Invalid persisted datetime"),
        ({"kind": "time", "value": "invalid"}, "Invalid persisted time"),
        (
            {
                "kind": "path",
                "type": "builtins:str",
                "value": "path",
            },
            "is not a path",
        ),
    ],
)
def test_metadata_codec_rejects_invalid_structured_values(
    value: Any,
    message: str,
) -> None:
    with pytest.raises(MetadataCodecError, match=message):
        decode_document(_document(value), Anything)


@pytest.mark.parametrize(
    ("model", "message"),
    [
        ([], "Expected model metadata to be a mapping"),
        ({"type": "invalid"}, "Invalid persisted type path"),
        ({"type": 1}, "Invalid persisted type path"),
        (
            {"type": "tests.arti.backends.test_sql_codec:NOT_A_TYPE"},
            "does not resolve to a type",
        ),
        (
            {"type": "builtins:int", "fields": {}, "fields_set": [], "state": {}},
            "is not an Artigraph Model",
        ),
    ],
)
def test_metadata_codec_rejects_invalid_models(model: Any, message: str) -> None:
    with pytest.raises(MetadataCodecError, match=message):
        decode_document(_document({"kind": "model", "index": 0}, model), Anything)


def test_metadata_codec_rejects_invalid_model_values() -> None:
    document = encode_document(
        CodecValues(
            blob=b"",
            color=Color.RED,
            date=date(2026, 7, 26),
            datetime=datetime(2026, 7, 26),
            fingerprint_value=Fingerprint.from_int(1),
            path=Path("artigraph"),
            time=time(12),
            values=frozenset(),
        )
    )
    fields = cast(dict[str, Any], _root(document)["fields"])
    fields["date"] = 1

    with pytest.raises(MetadataCodecError, match="Unable to restore CodecValues"):
        decode_document(document, CodecValues)


def test_metadata_codec_rejects_invalid_storage_state() -> None:
    document = encode_document(StringLiteral())
    state = cast(dict[str, Any], _root(document)["state"])
    state["extra"] = None

    with pytest.raises(MetadataCodecError, match="Unexpected persisted StringLiteral state"):
        decode_document(document, StringLiteral)


def test_metadata_codec_rejects_invalid_items() -> None:
    with pytest.raises(MetadataCodecError, match="Expected list items to be a list"):
        decode_document(_document({"kind": "list", "items": {}}), Anything)


@given(
    st.recursive(
        st.none() | st.booleans() | st.integers() | st.floats() | st.text(),
        lambda children: (
            st.lists(children, max_size=5) | st.dictionaries(st.text(), children, max_size=5)
        ),
        max_leaves=20,
    )
)
def test_metadata_codec_rejects_arbitrary_json_without_internal_errors(value: Any) -> None:
    with suppress(MetadataCodecError):
        decode_document(_document(value), Anything)
    with suppress(MetadataCodecError):
        decode_document(_document({"kind": "model", "index": 0}, value), Anything)


@given(
    kind=st.sampled_from(
        (
            "model",
            "typed_box",
            "mapping",
            "tuple",
            "list",
            "set",
            "frozenset",
            "enum",
            "type",
            "int",
            "float",
            "bytes",
            "date",
            "datetime",
            "time",
            "path",
        )
    ),
    fields=st.dictionaries(
        st.text(),
        st.none() | st.booleans() | st.integers() | st.floats() | st.text(),
        max_size=8,
    ),
)
def test_metadata_codec_rejects_malformed_known_kinds_without_internal_errors(
    kind: str,
    fields: dict[str, Any],
) -> None:
    value = {**fields, "kind": kind}
    with suppress(MetadataCodecError):
        decode_document(_document(value), Anything)


def test_metadata_codec_expected_type() -> None:
    with pytest.raises(MetadataCodecError, match="Expected persisted Anything"):
        decode_document(
            encode_document(
                CodecValues(
                    blob=b"",
                    color=Color.RED,
                    date=date(2026, 7, 26),
                    datetime=datetime(2026, 7, 26),
                    fingerprint_value=Fingerprint.from_int(1),
                    path=Path("artigraph"),
                    time=time(12),
                    values=frozenset(),
                )
            ),
            Anything,
        )


@pytest.mark.parametrize("value", [1.5, 1e16, -0.0, float("inf"), float("-inf")])
def test_metadata_codec_tags_floats(value: float) -> None:
    # Postgres JSONB normalizes JSON numbers (eg: `1e+16` reads back as an int), so floats are
    # never stored as bare JSON numbers.
    document = encode_document(Anything(value=value))
    restored = decode_document(document, Anything).value

    assert _root(document)["fields"]["value"] == {"kind": "float", "value": repr(value)}
    assert type(restored) is float
    assert repr(restored) == repr(value)


@pytest.mark.parametrize(
    "value",
    [
        1.5,
        {"kind": "float", "value": 1.5},
        {"kind": "float", "value": "finite"},
        {"kind": "int", "type": "builtins:int", "value": None},
        {"kind": "int", "type": "arti.fingerprints:Fingerprint", "value": 2**70},
        {"kind": "mapping", "type": "builtins:dict", "items": [[1]]},
        {"kind": "path", "type": "pathlib:PurePath", "value": None},
        {"kind": "set", "items": [{"kind": "list", "items": list[Any]()}]},
    ],
)
def test_metadata_codec_rejects_malformed_values(value: Any) -> None:
    document = encode_document(Anything(value=None))
    _root(document)["fields"]["value"] = value

    with pytest.raises(MetadataCodecError):
        decode_document(document, Anything)
