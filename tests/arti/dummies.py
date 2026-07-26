from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any

from arti import (
    Annotation,
    Artifact,
    Fingerprint,
    Format,
    InputFingerprints,
    PartitionKeyTypes,
    Producer,
    Statistic,
    Storage,
    StoragePartition,
    StoragePartitionSnapshots,
    Type,
    TypeSystem,
    View,
    io,
    producer,
)
from arti.formats.json import JSON
from arti.types import Int32, Int64, Struct
from arti.types.python import python_type_system


class Num(Artifact):
    type: Type = Int64()
    format: Format = JSON()
    # Omit storage to require callers to set the instance in a separate tempdir.


@producer()
def div(a: Annotated[int, Num], b: Annotated[int, Num]) -> Annotated[int, Num]:
    return a // b


dummy_type_system = TypeSystem(key="dummy", extends=(python_type_system,))


class DummyAnnotation(Annotation):
    pass


class DummyFormat(Format):
    type_system = dummy_type_system


class DummyPartition(StoragePartition):
    key: str = "test"

    def compute_content_fingerprint(self) -> Fingerprint:
        return Fingerprint.from_string(self.key)


class DummyStorage(Storage[DummyPartition]):
    key: str = "test-{graph_name}-{path_tags}-{names}-{partition_key_spec}-{input_fingerprint}-{name}.{extension}"

    def discover_partitions(
        self, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitionSnapshots:
        if self.key_types != PartitionKeyTypes():
            raise NotImplementedError()
        if input_fingerprints != InputFingerprints():
            raise NotImplementedError()
        return (self.generate_partition().snapshot(),)


@io.register_reader
def dummy_reader(
    type_: Type,
    format: DummyFormat,
    storage_partitions: Sequence[DummyPartition],
    view: View,
) -> Any:
    return "test-read"


@io.register_writer
def dummy_writer(
    data: object,
    type_: Type,
    format: DummyFormat,
    storage_partition: DummyPartition,
    view: View,
) -> None:
    return None


class DummyStatistic(Statistic):
    type: Type = Int32()
    format: Format = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A1(Artifact):
    type: Type = Struct(fields={"a": Int32()})
    format: Format = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A2(Artifact):
    type: Type = Struct(fields={"b": Int32()})
    format: Format = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A3(Artifact):
    type: Type = Struct(fields={"c": Int32()})
    format: Format = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A4(Artifact):
    type: Type = Struct(fields={"d": Int32()})
    format: Format = DummyFormat()
    storage: DummyStorage = DummyStorage()


class P1(Producer):
    a1: A1

    @staticmethod
    def build(a1: dict[Any, Any]) -> Annotated[dict[Any, Any], A2]:
        return {}


class P2(Producer):
    a2: A2

    @staticmethod
    def build(
        a2: dict[Any, Any],
    ) -> tuple[Annotated[dict[Any, Any], A3], Annotated[dict[Any, Any], A4]]:
        return {}, {}


class P3(Producer):
    a1: A1
    a2: A2

    @staticmethod
    def build(
        a1: dict[Any, Any], a2: dict[Any, Any]
    ) -> tuple[Annotated[dict[Any, Any], A3], Annotated[dict[Any, Any], A4]]:
        return {}, {}
