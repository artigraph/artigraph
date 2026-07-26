import json
import pickle
from typing import Any

import pytest
from pydantic import InstanceOf

from arti import Artifact, Graph, Storage
from arti.internal.models import Model
from arti.storage.google.cloud.storage import GCSFile
from arti.storage.literal import StringLiteral
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import String


class StorageHolder(Model):
    storage: InstanceOf[Storage[Any]]


@pytest.mark.parametrize(
    ("left", "right", "field"),
    [
        (LocalFile(path="/one"), LocalFile(path="/two"), "path"),
        (StringLiteral(id="one", value="a"), StringLiteral(id="two", value="a"), "id"),
        (StringLiteral(id="one", value="a"), StringLiteral(id="one", value="b"), "value"),
        (GCSFile(bucket="one", path="file"), GCSFile(bucket="two", path="file"), "bucket"),
        (GCSFile(bucket="one", path="a"), GCSFile(bucket="one", path="b"), "path"),
    ],
)
def test_nested_storage_preserves_concrete_fields(
    left: Storage[Any], right: Storage[Any], field: str
) -> None:
    for storage in (left, right):
        holder = StorageHolder(storage=storage)
        assert holder.model_dump()["storage"][field] == getattr(storage, field)
        assert json.loads(holder.model_dump_json())["storage"][field] == getattr(storage, field)
        assert pickle.loads(pickle.dumps(holder)) == holder
        artifact = Artifact(type=String(), storage=storage)
        assert artifact.model_dump()["storage"][field] == getattr(storage, field)
        assert json.loads(artifact.model_dump_json())["storage"][field] == getattr(storage, field)

    assert StorageHolder(storage=left).fingerprint != StorageHolder(storage=right).fingerprint
    assert (
        Artifact(type=String(), storage=left).fingerprint
        != Artifact(type=String(), storage=right).fingerprint
    )

    with Graph(name="same") as first:
        first.artifacts.data = Artifact(type=String(), storage=left)
    with Graph(name="same") as second:
        second.artifacts.data = Artifact(type=String(), storage=right)
    assert first.fingerprint != second.fingerprint


def test_storage_any_erasure_preserves_concrete_specializations() -> None:
    assert Storage[Any] is Storage
    assert Storage[LocalFilePartition] is not Storage
    assert LocalFile.storage_partition_type is LocalFilePartition
    assert not hasattr(Storage, "storage_partition_type")
    with pytest.raises(TypeError):
        LocalFile[Any]  # pyright: ignore[reportInvalidTypeArguments]


def test_partition_retains_concrete_storage_fields() -> None:
    storage = LocalFile(path="/one")
    partition = LocalFilePartition(path="/one", storage=storage)
    assert partition.model_dump()["storage"]["path"] == "/one"
    assert json.loads(partition.model_dump_json())["storage"]["path"] == "/one"
    assert pickle.loads(pickle.dumps(partition)) == partition
