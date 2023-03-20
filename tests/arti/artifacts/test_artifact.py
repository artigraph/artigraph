import json
import pickle
import re
from datetime import date, datetime
from typing import Any

import pytest
from pydantic import validator

from arti import Annotation, Artifact, Format, Statistic, Storage, StoragePartition, Type
from arti.formats.json import JSON
from arti.internal.type_hints import Self
from arti.storage.literal import StringLiteral
from arti.types import (
    Binary,
    Boolean,
    Collection,
    Date,
    Float64,
    Int64,
    List,
    Map,
    Null,
    Set,
    String,
    Struct,
)
from tests.arti.dummies import A1, A2, P1, P2, DummyFormat, DummyStatistic, DummyStorage


def test_cast() -> None:
    assert isinstance(Artifact.cast(A1()), A1)
    assert isinstance(Artifact.cast(P1(a1=A1())), A2)
    with pytest.raises(ValueError, match="P2 produces 2 Artifacts"):
        Artifact.cast(P2(a2=A2()))


@pytest.mark.parametrize(
    ["value", "expected_type"],
    [
        ("hi", String()),
        (5, Int64()),
        (5.0, Float64()),
        (None, Null()),
        (True, Boolean()),
        ((1, 2, 3), List(element=Int64())),
        ([1, 2, 3], List(element=Int64())),
        ({"a": 1, "b": 2}, Map(key=String(), value=Int64())),
    ],
)
def test_cast_literals(value: Any, expected_type: Type) -> None:
    artifact = Artifact.cast(value)
    assert artifact.type == expected_type
    assert isinstance(artifact.format, JSON)
    assert isinstance(artifact.storage, StringLiteral)
    assert artifact.storage.value == json.dumps(value)


@pytest.mark.xfail
@pytest.mark.parametrize(
    ["value", "expected_type"],
    [
        (b"hi", Binary()),
        (date(1970, 1, 1), Date()),
        (datetime(1970, 1, 1), Date()),
        ({1, 2, 3}, Set(element=Int64())),
    ],
)
def test_cast_literals_not_yet_implemented(value: Any, expected_type: Type) -> None:
    test_cast_literals(value, expected_type)


@pytest.mark.parametrize(
    ["value"],
    [
        ((1, "a"),),
        ({"a": "b", 1: 2},),
    ],
)
def test_cast_literals_errors(value: Any) -> None:
    with pytest.raises(
        NotImplementedError, match=re.escape(f"Unable to determine type of {value}")
    ):
        Artifact.cast(value)


def test_Artifact_validation() -> None:
    class BadFormat(DummyFormat):
        def _visit_type(self, type_: Type) -> Self:
            raise ValueError("Format - Boo!")

    class BadStorage(DummyStorage):
        @validator("type")
        @classmethod
        def validate_type(cls, type_: Type) -> Type:
            raise ValueError("Storage - Boo!")

        @validator("format")
        @classmethod
        def validate_format(cls, format: Format) -> Format:
            raise ValueError("Storage - Boo!")

    with pytest.raises(ValueError, match="type\n  field required"):
        Artifact()  # type: ignore[call-arg]

    with pytest.raises(ValueError, match="Format - Boo!"):

        class BadFormatArtifact(Artifact):
            type: Type = Int64()
            format: Format = BadFormat()
            storage: Storage[StoragePartition] = DummyStorage()

        BadFormatArtifact()

    with pytest.raises(ValueError, match="Storage - Boo!"):

        class BadStorageArtifact(Artifact):
            type: Type = Int64()
            format: Format = DummyFormat()
            storage: Storage[StoragePartition] = BadStorage()

        BadStorageArtifact()

    class GoodArtifact(Artifact):
        type: Type = Int64()
        format: Format = DummyFormat()
        storage: Storage[StoragePartition] = DummyStorage()

    GoodArtifact()


def test_instance_attr_merging() -> None:
    class Ann1(Annotation):
        x: int

    class Ann2(Annotation):
        y: int

    class Stat1(DummyStatistic):
        pass

    class Stat2(DummyStatistic):
        pass

    class MyArtifact(A1):
        annotations: tuple[Annotation, ...] = (Ann1(x=5),)
        statistics: tuple[Statistic, ...] = (Stat1(),)

    artifact = MyArtifact(annotations=[Ann2(y=10)], statistics=[Stat2()])
    assert tuple(type(a) for a in artifact.annotations) == (Ann1, Ann2)
    assert tuple(type(s) for s in artifact.statistics) == (Stat1, Stat2)


def test_Artifact_storage_path_resolution() -> None:
    class S(DummyStorage):
        key: str = "test-{partition_key_spec}"

    class A(Artifact):
        type: Type = Collection(element=Struct(fields={"a": Int64()}), partition_by=("a",))
        format: Format = DummyFormat()
        storage: S

    assert A(storage=S()).storage.key == "test-a_key={a.key}"


def test_Artifact_pickle() -> None:
    artifact = Artifact.cast(10)
    msg = pickle.dumps(artifact)
    assert pickle.loads(msg) == artifact
