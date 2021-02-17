from arti.artifacts.core import Artifact
from arti.formats.core import Format
from arti.producers.core import Producer
from arti.types.core import Int32, Struct, TypeSystem

dummy_type_system = TypeSystem("dummy")


class TestFormat(Format):
    type_system = dummy_type_system


class A1(Artifact):
    schema = Struct({"x": Int32()})


class A2(Artifact):
    pass


class A3(Artifact):
    pass


class A4(Artifact):
    pass


class P1(Producer):
    def build(self, input_artifact: A1) -> A2:
        return A2()


class P2(Producer):
    def build(self, input_artifact: A2) -> tuple[A3, A4]:
        return A3(), A4()
