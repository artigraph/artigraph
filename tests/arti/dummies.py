from arti.annotations import Annotation
from arti.artifacts import Artifact
from arti.formats import Format
from arti.partitions import PartitionKey
from arti.producers import Producer
from arti.statistics import Statistic
from arti.storage import Storage
from arti.types import Int32, Struct, TypeSystem

dummy_type_system = TypeSystem(key="dummy")


class DummyAnnotation(Annotation):
    pass


class DummyFormat(Format):
    type_system = dummy_type_system


class DummyStorage(Storage):
    def discover_partitions(
        self, **key_types: type[PartitionKey]
    ) -> tuple[dict[str, PartitionKey], ...]:
        return tuple()


# NOTE: when using a subclass of the original type hint, we must override[1].
#
# 1: https://github.com/samuelcolvin/pydantic/pull/3018


class DummyStatistic(Statistic):
    type: Int32 = Int32()
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A1(Artifact):
    type: Struct = Struct(fields={"1": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A2(Artifact):
    type: Struct = Struct(fields={"2": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A3(Artifact):
    type: Struct = Struct(fields={"3": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class A4(Artifact):
    type: Struct = Struct(fields={"4": Int32()})
    format: DummyFormat = DummyFormat()
    storage: DummyStorage = DummyStorage()


class P1(Producer):
    a1: A1

    @staticmethod
    def build(a1: A1) -> A2:
        return A2()


class P2(Producer):
    a2: A2

    @staticmethod
    def build(a2: A2) -> tuple[A3, A4]:
        return A3(), A4()


class P3(Producer):
    a1: A1
    a2: A2

    @staticmethod
    def build(a1: A1, a2: A2) -> tuple[A3, A4]:
        return A3(), A4()
