from arti.annotations.core import Annotation
from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.producers.core import Producer
from arti.statistics.core import Statistic
from arti.storage.core import Storage
from arti.types.core import Int32, Struct, TypeSystem

dummy_type_system = TypeSystem("dummy")


class DummyAnnotation(Annotation):
    pass


class DummyFormat(Format):
    type_system = dummy_type_system


class DummyStatistic(Statistic):
    pass


class DummyStorage(Storage):
    pass


class A1(Artifact):
    schema = Struct({"x": Int32()})
    format = DummyFormat()
    storage = DummyStorage()


class A2(Artifact):
    pass


class A3(Artifact):
    pass


class A4(Artifact):
    pass


class P1(Producer):
    def build(self, a1: A1) -> A2:
        return A2()


class P2(Producer):
    def build(self, a2: A2) -> tuple[A3, A4]:
        return A3(), A4()


class P3(Producer):
    def build(self, a1: A1, a2: A2) -> tuple[A3, A4]:
        return A3(), A4()
