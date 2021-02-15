from arti.artifacts.core import Artifact
from arti.producers.core import Producer


class A1(Artifact):
    pass


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
