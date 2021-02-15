from typing import no_type_check

import pytest

from arti.producers.core import Producer
from tests.arti.dummies import A1, A2, A3


class DummyProducer(Producer):
    def build(self, input_artifact: A1) -> tuple[A2, A3]:
        pass


def test_Producer() -> None:
    # Imitate a ref to avoid "use outside graph" error
    input_artifact = A1()
    producer = DummyProducer(input_artifact=input_artifact)
    assert producer._input_artifacts["input_artifact"] is input_artifact
    assert len(list(producer)) == 2
    expected_output_classes = [A2, A3]
    for i, output in enumerate(producer):
        assert isinstance(output, expected_output_classes[i])


def test_Producer_mutations() -> None:
    input_artifact = A1()
    producer = DummyProducer(input_artifact=input_artifact)
    assert producer in input_artifact.consumers
    o1, o2 = A2(), A3()
    producer.to(o1, o2)
    assert o1.producer is producer
    assert o2.producer is producer


@no_type_check
def test_Producer_bad_signature() -> None:
    # pylint: disable=function-redefined

    with pytest.raises(ValueError, match="Producers must implement"):

        class BadProducer(Producer):
            pass

    with pytest.raises(ValueError, match="must have a type hint"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact):
                pass

    with pytest.raises(ValueError, match="type hint must be an Artifact subclass"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: str) -> tuple[A2, A3]:
                pass

    with pytest.raises(ValueError, match="must not have a default"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1 = A1()):
                pass

    with pytest.raises(ValueError, match="parameter must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1, /):
                pass

    with pytest.raises(ValueError, match="parameter must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, *args: A1):
                pass

    with pytest.raises(ValueError, match="parameter must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, **kwargs: A1):
                pass

    with pytest.raises(ValueError, match="A return value must be set"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1):
                pass

    with pytest.raises(ValueError, match="return value must be an Artifact"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1) -> str:
                pass

    with pytest.raises(ValueError, match="return value must be an Artifact"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1) -> tuple[A2, str]:
                pass


def test_Producer_bad_kwargs() -> None:
    with pytest.raises(ValueError, match="Unknown argument"):
        DummyProducer(junk=5)  # type: ignore
    with pytest.raises(ValueError, match="Missing argument"):
        DummyProducer()
    with pytest.raises(ValueError, match="expects an instance of"):
        DummyProducer(input_artifact=5)  # type: ignore
    producer = DummyProducer(input_artifact=A1())
    with pytest.raises(ValueError, match="Expected .* arguments"):
        producer.to()
    with pytest.raises(ValueError, match="Expected the 1st argument to be"):
        producer.to(1, 2)  # type: ignore
    with pytest.raises(ValueError, match="Expected the 2nd argument to be"):
        producer.to(A2(), A2())
    producer.to(A2(), A3())
