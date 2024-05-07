import re
from collections.abc import Sequence
from typing import Annotated, Any, ClassVar, Optional

import pytest

from arti import (
    Annotation,
    Artifact,
    Fingerprint,
    Format,
    PartitionDependencies,
    Producer,
    StoragePartition,
    StoragePartitions,
    Type,
    TypeSystem,
    View,
    io,
)
from arti import producer as producer_decorator  # Avoid shadowing
from arti.internal.models import Model, get_field_default
from arti.internal.utils import frozendict
from arti.producers import ValidateSig
from arti.types import Collection, Int64, List, Struct
from arti.versions import String as StringVersion
from arti.views import python as python_views
from tests.arti.dummies import A1, A2, A3, A4, P1, P2, DummyStorage, dummy_type_system


class DummyProducer(Producer):
    a1: A1

    @staticmethod
    def build(a1: dict) -> tuple[Annotated[dict, A2], Annotated[dict, A3]]:  # type: ignore[empty-body,type-arg]
        pass

    @staticmethod
    def map(a1: StoragePartitions) -> PartitionDependencies:  # type: ignore[empty-body]
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
    def dummy_producer(a1: Annotated[dict, A1]) -> Annotated[dict, A2]:  # type: ignore[type-arg]
        return {}

    assert dummy_producer.__name__ == "dummy_producer"
    assert dummy_producer._input_artifact_classes_ == frozendict(a1=A1)
    assert len(dummy_producer._outputs_) == 1
    assert dummy_producer._outputs_[0].artifact_class == A2
    assert dummy_producer(a1=A1()).annotations == Producer.__fields__["annotations"].default  # type: ignore[call-arg]
    assert dummy_producer(a1=A1()).version == Producer.__fields__["version"].default  # type: ignore[call-arg]

    class MyAnnotation(Annotation):
        pass

    def mapper() -> PartitionDependencies:
        return PartitionDependencies()

    @producer_decorator(
        annotations=(MyAnnotation(),), map=mapper, name="test", version=StringVersion(value="test")
    )
    def dummy_producer2(a1: Annotated[dict, A1]) -> Annotated[dict, A2]:  # type: ignore[type-arg]
        return {}

    assert dummy_producer2.__name__ == "test"
    assert dummy_producer2.map == mapper
    assert dummy_producer2(a1=A1()).annotations == (MyAnnotation(),)  # type: ignore[call-arg]
    assert dummy_producer2(a1=A1()).version == StringVersion(value="test")  # type: ignore[call-arg]


def test_Producer_input_artifact_classes() -> None:
    @producer_decorator()
    def dummy_producer(
        a1: Annotated[dict, A1],
        *,
        a: int,
        b: Annotated[int, "non-Artifact"],
    ) -> Annotated[dict, A2]:
        return {}

    assert dummy_producer._input_artifact_classes_ == frozendict(a1=A1, a=Artifact, b=Artifact)


def test_Producer_partitioned_input_validation() -> None:
    class A(Artifact):
        type: Type = Collection(element=Struct(fields={"x": Int64()}), partition_by=("x",))

    class P(Producer):
        a: A

        @staticmethod
        def build(a: list[dict]) -> Annotated[dict, A2]:  # type: ignore[empty-body,type-arg]
            pass

    assert P._input_artifact_classes_ == frozendict(a=A)
    assert P._build_inputs_ == frozendict(
        a=python_views.List(artifact_class=A, type=get_field_default(A, "type"), mode="READ")
    )

    with pytest.raises(ValueError, match="dict.* cannot be used to represent Collection"):

        class SingularInput(Producer):
            a: A

            @staticmethod
            def build(a: dict) -> Annotated[dict, A2]:  # type: ignore[empty-body,type-arg]
                pass

    with pytest.raises(
        ValueError, match=re.escape("list[int] cannot be used to represent Collection")
    ):

        class IncompatibleInput(Producer):
            a: A

            @staticmethod
            def build(a: list[int]) -> Annotated[dict, A]:  # type: ignore[empty-body,type-arg]
                pass


def test_Producer_outputs() -> None:
    assert DummyProducer._outputs_ == (
        python_views.Dict(artifact_class=A2, type=get_field_default(A2, "type"), mode="WRITE"),
        python_views.Dict(artifact_class=A3, type=get_field_default(A3, "type"), mode="WRITE"),
    )

    class ImplicitArtifact(Producer):
        a1: A1

        @classmethod
        def build(cls, a1: dict) -> tuple[int, Annotated[dict, A2]]:  # type: ignore[empty-body,type-arg]
            pass

    assert ImplicitArtifact._outputs_ == (
        python_views.Int(artifact_class=Artifact, type=Int64(), mode="WRITE"),
        python_views.Dict(artifact_class=A2, type=get_field_default(A2, "type"), mode="WRITE"),
    )

    class ExplicitView(Producer):
        a1: A1

        @staticmethod
        def build(a1: dict) -> Annotated[dict, A2, python_views.Dict]:  # type: ignore[empty-body,type-arg]
            pass

    assert ExplicitView._outputs_ == (
        python_views.Dict(artifact_class=A2, type=get_field_default(A2, "type"), mode="WRITE"),
    )

    with pytest.raises(
        ValueError, match=re.escape("DupView.build 1st return - multiple View values found")
    ):

        class DupView(Producer):
            a1: A1

            @staticmethod
            def build(a1: dict) -> Annotated[dict, A2, python_views.Dict, python_views.Int]:  # type: ignore[empty-body,type-arg]
                pass

    with pytest.raises(
        ValueError, match="DupArtifact.build 1st return - multiple Artifact values found"
    ):

        class DupArtifact(Producer):
            a1: A1

            @staticmethod
            def build(a1: dict) -> Annotated[dict, A1, A2]:  # type: ignore[empty-body,type-arg]
                pass


def test_Producer_string_annotation() -> None:
    # This may be from `x: "Type"` or `from __future__ import annotations`.
    class StrAnnotation(Producer):
        a1: "A1"

        @staticmethod
        def build(a1: "dict") -> "Annotated[dict, A2]":  # type: ignore[empty-body,type-arg]
            pass

    assert isinstance(StrAnnotation(a1=A1()).out(), A2)


def test_Producer_fingerprint() -> None:
    p1 = P1(a1=A1())
    assert p1.fingerprint == Fingerprint.from_string(
        f'P1:{{"a1": {p1.a1.fingerprint}, "version": {p1.version.fingerprint}}}'
    )


def test_Producer_compute_input_fingerprint() -> None:
    p1 = P1(a1=A1(storage=DummyStorage(key="test")))
    assert p1.compute_input_fingerprint(
        frozendict(a1=StoragePartitions())
    ) == Fingerprint.from_string(p1._class_key_).combine(p1.version.fingerprint)

    storage_partition = p1.a1.storage.generate_partition().copy(
        update={"content_fingerprint": Fingerprint.from_int(10)}
    )
    assert p1.compute_input_fingerprint(
        frozendict(a1=StoragePartitions([storage_partition]))
    ) == Fingerprint.from_string(p1._class_key_).combine(
        p1.version.fingerprint, storage_partition.get_or_compute_content_fingerprint()
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
    for producer, inp, out, type_, position in (
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


def test_Producer_map_artifacts() -> None:
    class P(Producer):
        a1: A1

        @staticmethod
        def build(a1: dict) -> Annotated[dict, A2]:  # type: ignore[empty-body,type-arg]
            pass

        @staticmethod
        def map(a1: StoragePartitions) -> PartitionDependencies:  # type: ignore[empty-body]
            pass

    assert P._map_inputs_ == {"a1"}

    with pytest.raises(
        ValueError,
        match="BadMapParam.map a1 param - type hint must be `StoragePartitions`",
    ):

        class BadMapParam(P):
            @staticmethod
            def map(a1: list) -> PartitionDependencies:  # type: ignore[override,empty-body,type-arg]
                pass


def test_Producer_validate_output() -> None:
    positive, negative = (True, "Positive"), (False, "Negative")

    def is_positive(i: int) -> tuple[bool, str]:
        return positive if i >= 0 else negative

    @producer_decorator(validate_outputs=is_positive)
    def p(x: int) -> int:
        return x

    assert p.validate_outputs(p.build(1)) == positive
    assert p.validate_outputs(p.build(-1)) == negative


def test_Producer_validate_output_hint_validation() -> None:
    def validate_any(i: Any) -> tuple[bool, str]:
        return bool(i), ""

    def validate_vargs_any(*vals: Any) -> tuple[bool, str]:
        return bool(vals), ""

    def validate_int(i: int) -> tuple[bool, str]:
        return bool(i), ""

    for validate_outputs in list[ValidateSig](
        [
            lambda x: (True, ""),
            validate_any,
            validate_vargs_any,
            validate_int,
        ]
    ):

        @producer_decorator(validate_outputs=validate_outputs)
        def single_return_build(x: int) -> int:
            return x

        assert single_return_build.validate_outputs(5)

    with pytest.raises(ValueError, match="i param - type hint must be `Any` or "):

        def accepts_vargs_float(*i: float) -> tuple[bool, str]:
            return bool(i), ""

        @producer_decorator(validate_outputs=accepts_vargs_float)
        def bad_vargs(x: int) -> int:
            return x

    with pytest.raises(ValueError, match="validate_output - must match the `.build` return"):

        @producer_decorator(validate_outputs=validate_int)
        def too_few_arg(x: int) -> tuple[int, int]:
            return x, x + 1

    with pytest.raises(ValueError, match="validate_output i param - must not have a default."):

        @producer_decorator(validate_outputs=lambda i=5: (True, ""))
        def bad_default(x: int) -> int:
            return x

    with pytest.raises(
        ValueError, match="validate_output i param - must be usable as a positional argument."
    ):

        def validate_kwarg(*, i: int) -> tuple[bool, str]:
            return bool(i), ""

        @producer_decorator(validate_outputs=validate_kwarg)
        def kwarg_only(x: int) -> int:
            return x

    with pytest.raises(
        ValueError, match="validate_output i param - type hint must match the 1st `.build` return"
    ):

        def accepts_float(i: float) -> tuple[bool, str]:
            return bool(i), ""

        @producer_decorator(validate_outputs=accepts_float)
        def mismatched_hint(x: int) -> int:
            return x


def test_Producer_build_outputs_check() -> None:
    class A(Artifact):
        type: Type = Int64()

    class B(Artifact):
        type: Type = Int64()

    class C(Artifact):
        type: Type = Collection(element=Struct(fields={"a": Int64()}), partition_by=("a",))

    class D(Artifact):
        type: Type = Collection(
            element=Struct(fields={"a": Int64(), "b": Int64()}), partition_by=("b",)
        )

    class NoPartitioning(Producer):
        @staticmethod
        def build() -> tuple[Annotated[int, A], Annotated[int, B]]:  # type: ignore[empty-body]
            pass

    class MatchingPartitioning(Producer):
        @staticmethod
        def build() -> tuple[Annotated[list[dict], C], Annotated[list[dict], C]]:  # type: ignore[empty-body,type-arg]
            pass

        @staticmethod
        def map() -> PartitionDependencies:
            return PartitionDependencies()

    for first_output in [Annotated[int, A], Annotated[list[dict], C]]:  # type: ignore[type-arg]
        with pytest.raises(ValueError, match="all outputs must have the same partitioning scheme"):

            class MixedPartitioning(Producer):
                @staticmethod
                def build() -> tuple[first_output, Annotated[list[dict], D]]:  # type: ignore[empty-body,type-arg,valid-type]
                    pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - must be implemented when the `build` outputs are partitioned",
    ):

        class BadProducer(Producer):
            @staticmethod
            def build() -> Annotated[list[dict], C]:  # type: ignore[empty-body,type-arg]
                pass


def test_Producer_bad_signature() -> None:
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

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            @classmethod
            def build(cls, a1: dict) -> Annotated[dict, A2]:  # type: ignore[empty-body,type-arg]
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - the following parameter\(s\) must be defined as a field: {'a1'}",
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            @classmethod
            def build(cls) -> Annotated[dict, A2]:  # type: ignore[empty-body,type-arg]
                pass

            @classmethod
            def map(cls, a1: StoragePartitions) -> PartitionDependencies:  # type: ignore[empty-body]
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer - the following fields aren't used in `.build` or `.map`: {'a2'}",
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1
            a2: A2

            @classmethod
            def build(cls, a1: dict) -> Annotated[dict, A3]:  # type: ignore[empty-body,type-arg]
                pass

    with pytest.raises(ValueError, match="must have a type hint"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1):  # type: ignore[no-untyped-def]
                pass

    with pytest.raises(ValueError, match="type hint must be an Artifact subclass"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: str

            @classmethod
            def build(cls, a1: str) -> tuple[A2, A3]:  # type: ignore[empty-body]
                pass

    with pytest.raises(ValueError, match="must not have a default"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict = A1()):  # type: ignore[assignment,no-untyped-def,type-arg]
                pass

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict, /):  # type: ignore[no-untyped-def,type-arg]
                pass

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, *a1: dict):  # type: ignore[no-untyped-def,type-arg]
                pass

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, **a1: dict):  # type: ignore[no-untyped-def,type-arg]
                pass

    with pytest.raises(ValueError, match="a return value must be set"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict):  # type: ignore[no-untyped-def,type-arg]
                pass

    with pytest.raises(ValueError, match="missing return signature"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: dict) -> None:  # type: ignore[type-arg]
                pass

    with pytest.raises(
        ValueError, match="BadProducer.a1 - field must not have a default nor be Optional."
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1 = None  # type: ignore[assignment]

            @classmethod
            def build(cls, a1: dict):  # type: ignore[no-untyped-def,type-arg]
                pass

    with pytest.raises(
        ValueError, match="BadProducer.a1 - field must not have a default nor be Optional."
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: Optional[A1]

            @classmethod
            def build(cls, a1: dict):  # type: ignore[no-untyped-def,type-arg]
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.a1 - field must not have a default nor be Optional.",
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1 = A1()

            @classmethod
            def build(cls, a1: dict) -> A2:  # type: ignore[empty-body,type-arg]
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.build a1 param - annotation Artifact class .* does not match that set on the field",
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            a1: A1

            @classmethod
            def build(cls, a1: Annotated[dict, A2]) -> A2:  # type: ignore[empty-body,type-arg]
                pass

    with pytest.raises(ValueError, match=r"str.* cannot be used to represent Struct"):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            @classmethod
            def build(cls) -> Annotated[str, A2]:  # type: ignore[empty-body]
                pass

    with pytest.raises(
        ValueError,
        match=re.escape(
            r"the specified Type (`Int64()`) is not compatible with the Artifact's Type"
        ),
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            @classmethod
            def build(cls) -> Annotated[int, Int64(), A2]:  # type: ignore[empty-body]
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.build - must be a @classmethod or @staticmethod",
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            def build(cls) -> Annotated[dict, A2]:  # type: ignore[empty-body,type-arg]
                pass

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - must be a @classmethod or @staticmethod",
    ):

        class BadProducer(Producer):  # type: ignore[no-redef] # noqa: F811
            @classmethod
            def build(cls) -> Annotated[dict, A2]:  # type: ignore[empty-body,type-arg]
                pass

            def map(cls) -> PartitionDependencies:  # type: ignore[empty-body]
                pass


def test_Producer_bad_init() -> None:
    with pytest.raises(ValueError, match="cannot be instantiated directly"):
        Producer()
    with pytest.raises(ValueError, match="extra fields not permitted"):
        DummyProducer(junk=5)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="field required"):
        DummyProducer()  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="expected an instance of"):
        DummyProducer(a1=5)
    with pytest.raises(ValueError, match="expected an instance of"):
        DummyProducer(a1=A2())


def test_Producer_bad_out() -> None:
    producer = DummyProducer(a1=A1())
    with pytest.raises(ValueError, match="expected 2 arguments of"):
        producer.out(1)  # type: ignore[arg-type]
    with pytest.raises(
        ValueError, match=r"DummyProducer.out\(\) 1st argument - expected an instance of"
    ):
        producer.out(1, 2)  # type: ignore[arg-type]
    with pytest.raises(
        ValueError, match=r"DummyProducer.out\(\) 2nd argument - expected an instance of"
    ):
        producer.out(A2(), A2())
    output = producer.out(A2(), A3())
    with pytest.raises(ValueError, match="is produced by"):
        producer.out(*output)


numbers_type = List(element=Int64())


class Numbers(Artifact):
    type = numbers_type


@pytest.mark.parametrize(
    ("annotation", "type_", "artifact_class"),
    [
        pytest.param(list[int], numbers_type, Artifact, id="simple"),
        pytest.param(
            Annotated[list[int], numbers_type],
            numbers_type,
            Artifact,
            id="annotated-Type",
        ),
        pytest.param(
            Annotated[list[int], Numbers],
            numbers_type,
            Numbers,
            id="annotated-Artifact",
        ),
        pytest.param(
            Annotated[list[int], numbers_type, Numbers],
            numbers_type,
            Numbers,
            id="annotated-Type-and-Artifact",
        ),
    ],
)
def test_Producer_type_inference(
    annotation: Any, type_: Type, artifact_class: type[Artifact]
) -> None:
    numbers_view_read = python_views.List(artifact_class=artifact_class, type=type_, mode="READ")
    numbers_view_write = numbers_view_read.copy(update={"mode": "WRITE"})

    @producer_decorator()
    def plusone(numbers: annotation) -> annotation:
        return [n + 1 for n in numbers]

    assert plusone._input_artifact_classes_ == frozendict(numbers=artifact_class)
    assert plusone._build_inputs_ == frozendict(numbers=numbers_view_read)
    assert plusone._outputs_ == (numbers_view_write,)
    assert plusone._map_inputs_ == {"numbers"}


def test_Producer_io_checks() -> None:
    # NOTE: Using class style with different kwargs for map/build to confirm we only validate
    # `build` kwargs (since we won't read map-only kwargs anyway).
    class CheckIO(Producer):
        m: Artifact
        b: Artifact

        @staticmethod
        def map(m: StoragePartitions) -> PartitionDependencies:  # type: ignore[empty-body]
            pass

        @staticmethod
        def build(b: int) -> int:
            return b + 1

    # Create some fake format for which we don't have any io implemented.
    class FakeFormat(Format):
        type_system: ClassVar[TypeSystem] = dummy_type_system

    good_artifact = Artifact.cast(1)
    fake_artifact = good_artifact.copy(update={"format": FakeFormat()})

    # Check reading
    producer = CheckIO(m=fake_artifact, b=good_artifact)
    with pytest.raises(ValueError, match="No `io.read` implementation found for: "):
        CheckIO(m=fake_artifact, b=fake_artifact)
    # Check writing
    producer.out(good_artifact)
    with pytest.raises(ValueError, match="No `io.write` implementation found for: "):
        producer.out(fake_artifact)

    @io.register_reader
    def dummy_reader(
        type_: Type,
        format: FakeFormat,
        storage_partitions: Sequence[StoragePartition],
        view: View,
    ) -> Any:
        return "test-read"

    # We should now be able to read it...
    CheckIO(m=fake_artifact, b=fake_artifact)

    @io.register_writer
    def dummy_writer(
        data: object,
        type_: Type,
        format: FakeFormat,
        storage_partition: StoragePartition,
        view: View,
    ) -> None:
        pass

    # and write it.
    producer.out(fake_artifact)
