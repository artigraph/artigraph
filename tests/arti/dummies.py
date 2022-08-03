from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any

from arti import (
    Annotation,
    Artifact,
    CompositeKeyTypes,
    Fingerprint,
    Format,
    InputFingerprints,
    Producer,
    Statistic,
    Storage,
    StoragePartition,
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
    type: Type = Int64()  # https://github.com/python/mypy/issues/11897
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
    ) -> tuple[DummyPartition, ...]:
        if self.key_types != CompositeKeyTypes():
            raise NotImplementedError()
        if input_fingerprints is not None and input_fingerprints != InputFingerprints():
            raise NotImplementedError()
        return (self.generate_partition(),)


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
    pass


class DummyStatistic(Statistic):
    type = Int32()
    format = DummyFormat()
    storage = DummyStorage()


class A1(Artifact):
    type = Struct(fields={"a": Int32()})
    format = DummyFormat()
    storage = DummyStorage()


class A2(Artifact):
    type = Struct(fields={"b": Int32()})
    format = DummyFormat()
    storage = DummyStorage()


class A3(Artifact):
    type = Struct(fields={"c": Int32()})
    format = DummyFormat()
    storage = DummyStorage()


class A4(Artifact):
    type = Struct(fields={"d": Int32()})
    format = DummyFormat()
    storage = DummyStorage()


class P1(Producer):
    a1: A1

    @staticmethod
    def build(a1: dict) -> Annotated[dict, A2]:  # type: ignore
        return {}


class P2(Producer):
    a2: A2

    @staticmethod
    def build(a2: dict) -> tuple[Annotated[dict, A3], Annotated[dict, A4]]:  # type: ignore
        return {}, {}


class P3(Producer):
    a1: A1
    a2: A2

    @staticmethod
    def build(a1: dict, a2: dict) -> tuple[Annotated[dict, A3], Annotated[dict, A4]]:  # type: ignore
        return {}, {}
