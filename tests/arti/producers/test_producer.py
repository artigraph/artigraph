import re
from typing import Annotated, Optional

import pytest

from arti.annotations import Annotation
from arti.artifacts import Artifact
from arti.fingerprints import Fingerprint
from arti.internal.models import Model
from arti.internal.utils import frozendict
from arti.producers import PartitionDependencies, Producer
from arti.producers import producer as producer_decorator  # Avoid shadowing
from arti.storage import StoragePartitions
from arti.types import Int64, List, Struct
from arti.versions import String as StringVersion
from arti.views import python as python_views
from tests.arti.dummies import A1, A2, A3, A4, P1, P2


class DummyProducer(Producer):
    a1: A1

    @staticmethod
    def build(a1: dict) -> tuple[Annotated[dict, A2], Annotated[dict, A3]]:  # type: ignore
        pass

    @staticmethod
    def map(a1: StoragePartitions) -> PartitionDependencies:
        pass


def check_model_matches(a: Model, b: Model, *, exclude: set[str]) -> None:
    assert a.dict(exclude=exclude) == b.dict(exclude=exclude)


def test_Producer() -> None:
    a1 = A1()
    producer = DummyProducer(a1=a1)
    assert producer.a1 == a1
    assert len(list(producer)) == 2
    expected_output_classes = [A2, A3]
    for i, output in enumerate(producer):
        assert isinstance(output, expected_output_classes[i])


def test_producer_decorator() -> None:
    @producer_decorator()
    def dummy_producer(a1: Annotated[dict, A1]) -> Annotated[dict, A2]:  # type: ignore
        return {}

    assert dummy_producer.__name__ == "dummy_producer"
    assert dummy_producer._input_artifact_types_ == frozendict(a1=A1)
    assert len(dummy_producer._output_metadata_) == 1
    assert dummy_producer._output_metadata_[0][0] == A2
    assert dummy_producer(a1=A1()).annotations == Producer.__fields__["annotations"].default
    assert dummy_producer(a1=A1()).version == Producer.__fields__["version"].default

    class MyAnnotation(Annotation):
        pass

    def mapper() -> PartitionDependencies:
        return PartitionDependencies()

    @producer_decorator(
        annotations=(MyAnnotation(),), map=mapper, name="test", version=StringVersion(value="test")
    )
    def dummy_producer2(a1: Annotated[dict, A1]) -> Annotated[dict, A2]:  # type: ignore
        return {}

    assert dummy_producer2.__name__ == "test"
    assert dummy_producer2.map == mapper
    assert dummy_producer2(a1=A1()).annotations == (MyAnnotation(),)
    assert dummy_producer2(a1=A1()).version == StringVersion(value="test")


def test_Producer_partitioned_input_validation() -> None:
    class A(Artifact):
        type: List = List(element=Struct(fields={"x": Int64()}), partition_by=("x",))

    class P(Producer):
        a: A

        @staticmethod
        def build(a: list[dict]) -> Annotated[dict, A2]:  # type: ignore
            pass

    assert P._input_artifact_types_ == frozendict(a=A)
    assert P._build_input_views_ == frozendict(a=python_views.List)

    with pytest.raises(ValueError, match="dict.* cannot be used to represent List"):

        class SingularInput(Producer):
            a: A

            @staticmethod
            def build(a: dict) -> Annotated[dict, A2]:  # type: ignore
                pass

    with pytest.raises(ValueError, match=re.escape("list[int] cannot be used to represent List")):

        class IncompatibleInput(Producer):
            a: A

            @staticmethod
            def build(a: list[int]) -> Annotated[dict, A]:  # type: ignore
                pass


def test_Producer_output_metadata() -> None:
    assert DummyProducer._output_metadata_ == ((A2, python_views.Dict), (A3, python_views.Dict))

    class ExplicitView(Producer):
        a1: A1

        @staticmethod
        def build(a1: dict) -> Annotated[dict, A2, python_views.Dict]:  # type: ignore
            pass

    assert ExplicitView._output_metadata_ == ((A2, python_views.Dict),)

    with pytest.raises(ValueError, match="DupView.build 1st return - multiple Views set"):

        class DupView(Producer):
            a1: A1

            @staticmethod
            def build(a1: dict) -> Annotated[dict, A2, python_views.Dict, python_views.Int]:  # type: ignore
                pass

    with pytest.raises(ValueError, match="NoArtifact.build 1st return - Artifact is not set "):

        class NoArtifact(Producer):
            a1: A1

            @classmethod
            def build(cls, a1: dict) -> Annotated[None, 5]:  # type: ignore
                pass

    with pytest.raises(ValueError, match="DupArtifact.build 1st return - multiple Artifacts set"):

        class DupArtifact(Producer):
            a1: A1

            @staticmethod
            def build(a1: dict) -> Annotated[dict, A1, A2]:  # type: ignore
                pass


def test_Producer_string_annotation() -> None:
    # This may be from `x: "Type"` or `from __future__ import annotations`.
    class StrAnnotation(Producer):
        a1: "A1"

        @staticmethod
        def build(a1: "dict") -> "Annotated[dict, A2]":  # type: ignore
            pass

    assert isinstance(StrAnnotation(a1=A1()).out(), A2)


def test_Producer_fingerprint() -> None:
    p1 = P1(a1=A1())
    assert p1.fingerprint == Fingerprint.from_string(
        f'P1:{{"a1": {p1.a1.fingerprint.key}, "version": {p1.version.fingerprint.key}}}'
    )


def test_Producer_compute_input_fingerprint() -> None:
    p1 = P1(a1=A1())
    assert p1.compute_input_fingerprint(
        frozendict(a1=StoragePartitions())
    ) == Fingerprint.from_string(p1._class_key_).combine(p1.version.fingerprint)

    storage_partition = p1.a1.storage.storage_partition_type(
        keys={}, content_fingerprint=Fingerprint.from_int(10)
    )
    assert p1.compute_input_fingerprint(
        frozendict(a1=StoragePartitions([storage_partition]))
    ) == Fingerprint.from_string(p1._class_key_).combine(
        p1.version.fingerprint, storage_partition.content_fingerprint
    )

    with pytest.raises(
        ValueError, match=re.escape("Mismatched dependency inputs; expected {'a1'}, got {'junk'}")
    ):
        p1.compute_input_fingerprint(frozendict(junk=StoragePartitions()))


def test_Producer_out() -> None:
    a1, a2, a3, a4 = A1(), A2(), A3(), A4()
    # single return Producer
    p1 = P1(a1=a1)
    a2_ = p1.out(a2)
    # multi return Producer
    p2 = P2(a2=a2)
    a3_, a4_ = p2.out(a3, a4)
    for (producer, inp, out, type_, position) in (
        (p1, a2, a2_, A2, 0),
        (p2, a3, a3_, A3, 0),
        (p2, a4, a4_, A4, 1),
    ):
        assert inp is not out
        assert isinstance(out, type_)
        assert out.producer_output is not None
        assert out.producer_output.producer == producer
        assert out.producer_output.position == position
        check_model_matches(inp, out, exclude={"producer_output"})
    assert list(p1) == [a2_]
    assert list(p2) == [a3_, a4_]


@pytest.mark.xfail(raises=NotImplementedError)
def test_Producer_map_defaults() -> None:
    p1 = P1(a1=A1())
    # We can make .map defaulting a bit smarter by inspecting how the input artifacts are
    # partitioned (or not).
    p1.map()  # ValueError


def test_Producer_map_artifacts() -> None:
    class P(Producer):
        a1: A1

        @staticmethod
        def build(a1: dict) -> Annotated[dict, A2]:  # type: ignore
            pass

        @staticmethod
        def map(a1: StoragePartitions) -> PartitionDependencies:
            pass

    assert P._map_input_metadata_ == frozendict(a1=A1)

    with pytest.raises(
        ValueError,
        match="BadMapParam.map a1 param - type hint must be `StoragePartitions`",
    ):

        class BadMapParam(P):
            @staticmethod
            def map(a1: list) -> PartitionDependencies:  # type: ignore
                pass


def test_Producer_build_outputs_check() -> None:
    class A(Artifact):
        type: Int64 = Int64()

    class B(Artifact):
        type: Int64 = Int64()

    class C(Artifact):
        type: List = List(element=Struct(fields={"a": Int64()}), partition_by=("a",))

    class D(Artifact):
        type: List = List(element=Struct(fields={"a": Int64(), "b": Int64()}), partition_by=("b",))

    class NoPartitioning(Producer):
        @staticmethod
        def build() -> tuple[Annotated[int, A], Annotated[int, B]]:
            pass

    class MatchingPartitioning(Producer):
        @staticmethod
        def build() -> tuple[Annotated[list[dict], C], Annotated[list[dict], C]]:  # type: ignore
            pass

        @staticmethod
        def map() -> PartitionDependencies:
            return PartitionDependencies()

    for first_output in [Annotated[int, A], Annotated[list[dict], C]]:  # type: ignore
        with pytest.raises(
            ValueError, match="all output Artifacts must have the same partitioning scheme"
        ):

            class MixedPartitioning(Producer):
                @staticmethod
                def build() -> tuple[first_output, Annotated[list[dict], D]]:  # type: ignore
                    pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - must be implemented when the `build` outputs are partitioned",
    ):

        class BadProducer(Producer):  # noqa: F811
            @staticmethod
            def build() -> Annotated[list[dict], C]:  # type: ignore
                pass


def test_Producer_bad_signature() -> None:  # noqa: C901
    # pylint: disable=function-redefined

    # Ensure no error if _abstract_
    class OkProducer(Producer):
        _abstract_ = True

    with pytest.raises(ValueError, match="BadProducer.build - must be implemented"):

        class BadProducer(Producer):
            pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.build - the following parameter\(s\) must be defined as a field: {'a1'}",
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            @classmethod
            def build(cls, a1: dict) -> Annotated[dict, A2]:  # type: ignore
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - the following parameter\(s\) must be defined as a field: {'a1'}",
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            @classmethod
            def build(cls) -> Annotated[dict, A2]:  # type: ignore
                pass

            @classmethod
            def map(cls, a1: StoragePartitions) -> PartitionDependencies:
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer - the following fields aren't used in `.build` or `.map`: {'a2'}",
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1
            a2: A2

            @classmethod
            def build(cls, a1: dict) -> Annotated[dict, A3]:  # type: ignore
                pass

    with pytest.raises(ValueError, match="must have a type hint"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1):  # type: ignore
                pass

    with pytest.raises(ValueError, match="type hint must be an Artifact subclass"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: str

            @classmethod
            def build(cls, a1: str) -> tuple[A2, A3]:
                pass

    with pytest.raises(ValueError, match="must not have a default"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict = A1()):  # type: ignore
                pass

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict, /):  # type: ignore
                pass

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, *a1: dict):  # type: ignore
                pass

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, **a1: dict):  # type: ignore
                pass

    with pytest.raises(ValueError, match="a return value must be set"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict):  # type: ignore
                pass

    with pytest.raises(ValueError, match="missing return signature"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict) -> None:  # type: ignore
                pass

    with pytest.raises(ValueError, match="2nd return - must be an Annotated hint"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict) -> tuple[Annotated[dict, A2], str]:  # type: ignore
                pass

    with pytest.raises(
        ValueError, match="BadProducer.a1 - field must not have a default nor be Optional."
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1 = None  # type: ignore

            @classmethod
            def build(cls, a1: dict):  # type: ignore
                pass

    with pytest.raises(
        ValueError, match="BadProducer.a1 - field must not have a default nor be Optional."
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: Optional[A1]

            @classmethod
            def build(cls, a1: dict):  # type: ignore
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.a1 - field must not have a default nor be Optional.",
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            a1: A1 = A1()

            @classmethod
            def build(cls, a1: dict) -> A2:  # type: ignore
                pass

    with pytest.raises(ValueError, match=r"str.* cannot be used to represent Struct"):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            @classmethod
            def build(cls) -> Annotated[str, A2]:
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.build - must be a @classmethod or @staticmethod",
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            def build(cls) -> Annotated[dict, A2]:  # type: ignore
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - must be a @classmethod or @staticmethod",
    ):

        class BadProducer(Producer):  # type: ignore # noqa: F811
            @classmethod
            def build(cls) -> Annotated[dict, A2]:  # type: ignore
                pass

            def map(cls) -> PartitionDependencies:
                pass


def test_Producer_bad_init() -> None:
    with pytest.raises(ValueError, match="cannot be instantiated directly"):
        Producer()
    with pytest.raises(ValueError, match="extra fields not permitted"):
        DummyProducer(junk=5)
    with pytest.raises(ValueError, match="field required"):
        DummyProducer()
    with pytest.raises(ValueError, match="expected an instance of"):
        DummyProducer(a1=5)
    with pytest.raises(ValueError, match="expected an instance of"):
        DummyProducer(a1=A2())


def test_Producer_bad_out() -> None:
    producer = DummyProducer(a1=A1())
    with pytest.raises(ValueError, match="expected 2 arguments of"):
        producer.out(1)  # type: ignore
    with pytest.raises(
        ValueError, match=r"DummyProducer.out\(\) 1st argument - expected instance of"
    ):
        producer.out(1, 2)  # type: ignore
    with pytest.raises(
        ValueError, match=r"DummyProducer.out\(\) 2nd argument - expected instance of"
    ):
        producer.out(A2(), A2())
    output = producer.out(A2(), A3())
    with pytest.raises(ValueError, match="is produced by"):
        producer.out(*output)
