from arti.annotations.core import Annotation
from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.producers.core import Producer
from arti.statistics.core import Statistic
from arti.storage.core import Storage
from arti.types.core import Int32, Struct, TypeSystem

dummy_type_system = TypeSystem(key="dummy")


class DummyAnnotation(Annotation):
    pass


class DummyFormat(Format):
    type_system = dummy_type_system


class DummyStorage(Storage):
    pass


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
