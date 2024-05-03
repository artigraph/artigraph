import base64
import hashlib

from gcsfs import GCSFileSystem

from arti import Fingerprint, PartitionKey
from arti.partitions import Int32Field
from arti.storage.google.cloud.storage import GCSFile, GCSFilePartition
from arti.types import Collection, Int32, Struct
from tests.arti.dummies import DummyFormat


def test_GCSFile() -> None:
    f = GCSFile(bucket="test", path="folder/file")
    assert f.qualified_path == "test/folder/file"


def test_GCSFile_discover_partitions(gcs: GCSFileSystem, gcs_bucket: str) -> None:
    storage = (
        GCSFile(bucket=gcs_bucket, path="{i.value}")
        ._visit_type(Collection(element=Struct(fields={"i": Int32()}), partition_by=("i",)))
        ._visit_format(DummyFormat())
    )
    expected_keys = {PartitionKey(i=Int32Field(value=i)) for i in [0, 1]}
    gcs.pipe({storage.qualified_path.format(**key): b"data" for key in expected_keys})
    for partition_snapshot in storage.discover_partitions():
        assert partition_snapshot.partition_key in expected_keys
        assert partition_snapshot.content_fingerprint == Fingerprint.from_string(
            base64.b64encode(
                hashlib.md5(b"data").digest()  # noqa: S324 (GCS only provides md5)
            ).decode()
        )
        partition = partition_snapshot.storage_partition
        assert isinstance(partition, GCSFilePartition)
        assert partition.qualified_path == storage.qualified_path.format(**partition.partition_key)
