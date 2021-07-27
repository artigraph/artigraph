from typing import Any

import pytest

from arti.fingerprints.core import Fingerprint
from arti.internal.models import Model
from arti.producers.core import Producer
from arti.versions.core import String
from tests.arti.dummies import A1, A2, A3, A4, P1, P2


class DummyProducer(Producer):
    def build(self, a1: A1) -> tuple[A2, A3]:
        pass

    def map(self, a1: A1) -> Any:
        pass


def check_model_matches(a: Model, b: Model, *, exclude: set[str]) -> None:
    assert a.dict(exclude=exclude) == b.dict(exclude=exclude)


def test_Producer() -> None:
    a1 = A1()
    producer = DummyProducer(a1=a1)
    assert producer._input_artifacts["a1"] is a1
    assert len(list(producer)) == 2
    expected_output_classes = [A2, A3]
    for i, output in enumerate(producer):
        assert isinstance(output, expected_output_classes[i])


def test_Producer_fingerprint() -> None:
    p1 = P1(a1=A1())
    assert p1.fingerprint == Fingerprint.from_string("P1") ^ p1.version.fingerprint
    p1.key, p1.version = "abc", String(value="xyz")
    assert p1.fingerprint == Fingerprint.from_string("abc") ^ String(value="xyz").fingerprint


def test_Producer_out() -> None:
    a1, a2, a3, a4 = A1(), A2(), A3(), A4()
    # single return Producer
    p1 = P1(a1=a1)
    a2_ = p1.out(a2)
    # multi return Producer
    p2 = P2(a2=a2)
    a3_, a4_ = p2.out(a3, a4)
    for (producer, inp, out, type_) in (
        (p1, a2, a2_, A2),
        (p2, a3, a3_, A3),
        (p2, a4, a4_, A4),
    ):
        assert inp is not out
        assert isinstance(out, type_)
        assert out.producer is producer
        check_model_matches(inp, out, exclude={"producer"})
    assert list(p1) == [a2_]
    assert list(p2) == [a3_, a4_]


@pytest.mark.xfail(raises=ValueError)
def test_Producer_map_defaults() -> None:
    p1 = P1(a1=A1())
    # We can make .map defaulting a bit smarter by inspecting how the input artifacts are
    # partitioned (or not).
    p1.map()  # ValueError


def test_Producer_bad_signature() -> None:  # noqa: C901
    # pylint: disable=function-redefined

    with pytest.raises(ValueError, match="Producers must implement"):

        class BadProducer(Producer):  # type: ignore
            pass

    with pytest.raises(ValueError, match="must have a type hint"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact):  # type: ignore
                pass

    with pytest.raises(ValueError, match="type hint must be an Artifact subclass"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: str) -> tuple[A2, A3]:  # type: ignore
                pass

    with pytest.raises(ValueError, match="must not have a default"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1 = A1()):  # type: ignore
                pass

    with pytest.raises(ValueError, match="parameter must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1, /):  # type: ignore
                pass

    with pytest.raises(ValueError, match="parameter must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, *args: A1):  # type: ignore
                pass

    with pytest.raises(ValueError, match="parameter must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, **kwargs: A1):  # type: ignore
                pass

    with pytest.raises(ValueError, match="return value must be an Artifact"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1) -> None:  # type: ignore
                pass

    with pytest.raises(ValueError, match="A return value must be set"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1):  # type: ignore
                pass

    with pytest.raises(ValueError, match="return value must be an Artifact"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1) -> str:  # type: ignore
                pass

    with pytest.raises(ValueError, match="return value must be an Artifact"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1) -> tuple[A2, str]:  # type: ignore
                pass

    with pytest.raises(ValueError, match="The parameters to `map` .* must match `build`"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(self, input_artifact: A1) -> A2:
                pass

            def map(self, input_artifact: A2) -> Any:
                pass


def test_Producer_bad_init() -> None:
    # NOTE: Producers aren't currently models - so raises a ValueError rather than pydantic.ValidationError
    with pytest.raises(ValueError, match="cannot be instantiated directly"):
        Producer()
    with pytest.raises(ValueError, match="Unknown argument"):
        DummyProducer(junk=5)  # type: ignore
    with pytest.raises(ValueError, match="Missing argument"):
        DummyProducer()  # type: ignore
    with pytest.raises(ValueError, match="expects an instance of"):
        DummyProducer(a1=5)  # type: ignore


def test_Producer_bad_out() -> None:
    producer = DummyProducer(a1=A1())
    with pytest.raises(ValueError, match="Expected 2 arguments of"):
        producer.out(1)
    with pytest.raises(ValueError, match="Expected the 1st argument to be"):
        producer.out(1, 2)
    with pytest.raises(ValueError, match="Expected the 2nd argument to be"):
        producer.out(A2(), A2())
    output = producer.out(A2(), A3())
    with pytest.raises(ValueError, match="is produced by"):
        producer.out(*output)
