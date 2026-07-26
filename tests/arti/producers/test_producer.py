# pyright: reportPrivateUsage=false
import re
from collections.abc import Callable, Sequence
from typing import Annotated, Any, ClassVar, Literal, cast

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
    StoragePartitionSnapshots,
    Type,
    TypeSystem,
    View,
    io,
)
from arti import producer as producer_decorator  # Avoid shadowing
from arti.internal.models import get_field_default
from arti.producers import ValidateSig
from arti.types import Collection, Int64, List, Struct
from arti.versions import String as StringVersion
from arti.views import python as python_views
from tests.arti.dummies import A1, A2, A3, A4, P1, P2, DummyStorage, dummy_type_system


class DummyProducer(Producer):
    a1: A1

    @staticmethod
    def build(
        a1: dict[Any, Any],
    ) -> tuple[Annotated[dict[Any, Any], A2], Annotated[dict[Any, Any], A3]]:
        raise NotImplementedError

    @staticmethod
    def map(a1: StoragePartitions) -> PartitionDependencies:
        raise NotImplementedError


def _define_bad_producer(
    *,
    name: str = "BadProducer",
    base: type[Producer] = Producer,
    annotations: dict[str, Any] | None = None,
    defaults: dict[str, Any] | None = None,
    build: Callable[..., Any] | None = None,
    build_kind: Literal["class", "instance", "static"] = "class",
    map_: Callable[..., Any] | None = None,
    map_kind: Literal["class", "instance", "static"] = "class",
) -> type[Producer]:
    namespace: dict[str, Any] = {"__annotations__": annotations or {}} | (defaults or {})
    if build is not None:
        namespace["build"] = _wrap_method(build, build_kind)
    if map_ is not None:
        namespace["map"] = _wrap_method(map_, map_kind)
    return cast(type[Producer], type(name, (base,), namespace))


def _wrap_method(
    method: Callable[..., Any],
    kind: Literal["class", "instance", "static"],
) -> Any:
    if kind == "class":
        return classmethod(method)
    if kind == "static":
        return staticmethod(method)
    return method


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
    def dummy_producer(
        a1: Annotated[dict[Any, Any], A1],
    ) -> Annotated[dict[Any, Any], A2]:
        return {}

    assert dummy_producer.__name__ == "dummy_producer"
    assert dummy_producer._input_artifact_classes_ == {"a1": A1}
    assert len(dummy_producer._outputs_) == 1
    assert dummy_producer._outputs_[0].artifact_class == A2
    assert dummy_producer(a1=A1()).annotations == Producer.model_fields["annotations"].default
    assert dummy_producer(a1=A1()).version == Producer.model_fields["version"].default

    class MyAnnotation(Annotation):
        pass

    def mapper() -> PartitionDependencies:
        return PartitionDependencies()

    @producer_decorator(
        annotations=(MyAnnotation(),), map=mapper, name="test", version=StringVersion(value="test")
    )
    def dummy_producer2(
        a1: Annotated[dict[Any, Any], A1],
    ) -> Annotated[dict[Any, Any], A2]:
        return {}

    assert dummy_producer2.__name__ == "test"
    assert dummy_producer2.map == mapper
    assert dummy_producer2(a1=A1()).annotations == (MyAnnotation(),)
    assert dummy_producer2(a1=A1()).version == StringVersion(value="test")


def test_Producer_input_artifact_classes() -> None:
    @producer_decorator()
    def dummy_producer(
        a1: Annotated[dict[Any, Any], A1],
        *,
        a: int,
        b: Annotated[int, "non-Artifact"],
    ) -> Annotated[dict[Any, Any], A2]:
        return {}

    assert dummy_producer._input_artifact_classes_ == {"a1": A1, "a": Artifact, "b": Artifact}


def test_Producer_partitioned_input_validation() -> None:
    class A(Artifact):
        type: Type = Collection(element=Struct(fields={"x": Int64()}), partition_by=("x",))

    class P(Producer):
        a: A

        @staticmethod
        def build(
            a: list[dict[Any, Any]],
        ) -> Annotated[dict[Any, Any], A2]:
            raise NotImplementedError

    assert P._input_artifact_classes_ == {"a": A}
    assert P._build_inputs_ == {
        "a": python_views.List(artifact_class=A, type=get_field_default(A, "type"), mode="READ")
    }

    def singular_build(a: dict[Any, Any]) -> Annotated[dict[Any, Any], A2]:
        raise NotImplementedError

    with pytest.raises(ValueError, match=r"dict.* cannot be used to represent Collection"):
        _define_bad_producer(
            name="SingularInput",
            annotations={"a": A},
            build=singular_build,
            build_kind="static",
        )

    def incompatible_build(a: list[int]) -> Annotated[dict[Any, Any], A]:
        raise NotImplementedError

    with pytest.raises(
        ValueError, match=re.escape("list[int] cannot be used to represent Collection")
    ):
        _define_bad_producer(
            name="IncompatibleInput",
            annotations={"a": A},
            build=incompatible_build,
            build_kind="static",
        )


def test_Producer_outputs() -> None:
    assert DummyProducer._outputs_ == (
        python_views.Dict(artifact_class=A2, type=get_field_default(A2, "type"), mode="WRITE"),
        python_views.Dict(artifact_class=A3, type=get_field_default(A3, "type"), mode="WRITE"),
    )

    class ImplicitArtifact(Producer):
        a1: A1

        @classmethod
        def build(cls, a1: dict[Any, Any]) -> tuple[int, Annotated[dict[Any, Any], A2]]:
            raise NotImplementedError

    assert ImplicitArtifact._outputs_ == (
        python_views.Int(artifact_class=Artifact, type=Int64(), mode="WRITE"),
        python_views.Dict(artifact_class=A2, type=get_field_default(A2, "type"), mode="WRITE"),
    )

    class ExplicitView(Producer):
        a1: A1

        @staticmethod
        def build(
            a1: dict[Any, Any],
        ) -> Annotated[dict[Any, Any], A2, python_views.Dict]:
            raise NotImplementedError

    assert ExplicitView._outputs_ == (
        python_views.Dict(artifact_class=A2, type=get_field_default(A2, "type"), mode="WRITE"),
    )

    def duplicate_view_build(
        a1: dict[Any, Any],
    ) -> Annotated[dict[Any, Any], A2, python_views.Dict, python_views.Int]:
        raise NotImplementedError

    with pytest.raises(
        ValueError, match=re.escape("DupView.build 1st return - multiple View values found")
    ):
        _define_bad_producer(
            name="DupView",
            annotations={"a1": A1},
            build=duplicate_view_build,
            build_kind="static",
        )

    def duplicate_artifact_build(
        a1: dict[Any, Any],
    ) -> Annotated[dict[Any, Any], A1, A2]:
        raise NotImplementedError

    with pytest.raises(
        ValueError, match=re.escape("DupArtifact.build 1st return - multiple Artifact values found")
    ):
        _define_bad_producer(
            name="DupArtifact",
            annotations={"a1": A1},
            build=duplicate_artifact_build,
            build_kind="static",
        )


def test_Producer_string_annotation() -> None:
    # This may be from `x: "Type"` or `from __future__ import annotations`.
    class StrAnnotation(Producer):
        a1: "A1"

        @staticmethod
        def build(
            a1: "dict[Any, Any]",
        ) -> "Annotated[dict[Any, Any], A2]":
            raise NotImplementedError

    assert isinstance(StrAnnotation(a1=A1()).out(), A2)


def test_Producer_fingerprint_fields() -> None:
    p1 = P1(a1=A1())
    assert set(p1._arti_fingerprint_fields_) == {"_arti_type_", "a1", "version"}


def test_Producer_compute_input_fingerprint() -> None:
    p1 = P1(a1=A1(storage=DummyStorage(key="test")))
    assert p1.compute_input_fingerprint(
        {"a1": StoragePartitionSnapshots()}
    ) == Fingerprint.from_string(p1._arti_type_key_).combine(p1.version.fingerprint)

    storage_partition_snapshot = (
        cast(DummyStorage, cast(Any, p1.a1).storage).generate_partition().snapshot()
    )
    assert p1.compute_input_fingerprint(
        {"a1": StoragePartitionSnapshots([storage_partition_snapshot])}
    ) == Fingerprint.from_string(p1._arti_type_key_).combine(
        p1.version.fingerprint, storage_partition_snapshot.content_fingerprint
    )

    with pytest.raises(
        ValueError, match=re.escape("Mismatched dependency inputs; expected {'a1'}, got {'junk'}")
    ):
        p1.compute_input_fingerprint({"junk": StoragePartitionSnapshots()})


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
        assert inp.model_dump(exclude={"producer_output"}) == out.model_dump(
            exclude={"producer_output"}
        )
    assert list(p1) == [a2_]
    assert list(p2) == [a3_, a4_]


def test_Producer_map_artifacts() -> None:
    class P(Producer):
        a1: A1

        @staticmethod
        def build(a1: dict[Any, Any]) -> Annotated[dict[Any, Any], A2]:
            raise NotImplementedError

        @staticmethod
        def map(a1: StoragePartitions) -> PartitionDependencies:
            raise NotImplementedError

    assert P._map_inputs_ == {"a1"}

    def bad_map(a1: list[Any]) -> PartitionDependencies:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=re.escape("BadMapParam.map a1 param - type hint must be `StoragePartitions`"),
    ):
        _define_bad_producer(
            name="BadMapParam",
            base=P,
            map_=bad_map,
            map_kind="static",
        )


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

    def validate_untyped(x: Any) -> tuple[bool, str]:
        return True, ""

    for validate_outputs in list[ValidateSig](
        [
            validate_untyped,
            validate_any,
            validate_vargs_any,
            validate_int,
        ]
    ):

        @producer_decorator(validate_outputs=validate_outputs)
        def single_return_build(x: int) -> int:
            return x

        assert single_return_build.validate_outputs(5)[0]

    def accepts_vargs_float(*i: float) -> tuple[bool, str]:
        return bool(i), ""

    def bad_vargs(x: int) -> int:
        return x

    with pytest.raises(ValueError, match="i param - type hint must be `Any` or "):
        producer_decorator(validate_outputs=accepts_vargs_float)(bad_vargs)

    def too_few_arg(x: int) -> tuple[int, int]:
        return x, x + 1

    with pytest.raises(
        ValueError, match=re.escape("validate_output - must match the `.build` return")
    ):
        producer_decorator(validate_outputs=validate_int)(too_few_arg)

    def validate_default(i: int = 5) -> tuple[bool, str]:
        return bool(i), ""

    def bad_default(x: int) -> int:
        return x

    with pytest.raises(
        ValueError, match=re.escape("validate_output i param - must not have a default.")
    ):
        producer_decorator(validate_outputs=validate_default)(bad_default)

    def validate_kwarg(*, i: int) -> tuple[bool, str]:
        return bool(i), ""

    def kwarg_only(x: int) -> int:
        return x

    with pytest.raises(
        ValueError,
        match=re.escape("validate_output i param - must be usable as a positional argument."),
    ):
        producer_decorator(validate_outputs=validate_kwarg)(kwarg_only)

    def accepts_float(i: float) -> tuple[bool, str]:
        return bool(i), ""

    def mismatched_hint(x: int) -> int:
        return x

    with pytest.raises(
        ValueError,
        match=re.escape("validate_output i param - type hint must match the 1st `.build` return"),
    ):
        producer_decorator(validate_outputs=accepts_float)(mismatched_hint)


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
        def build() -> tuple[Annotated[int, A], Annotated[int, B]]:
            raise NotImplementedError

    class MatchingPartitioning(Producer):
        @staticmethod
        def build() -> tuple[
            Annotated[list[dict[Any, Any]], C],
            Annotated[list[dict[Any, Any]], C],
        ]:
            raise NotImplementedError

        @staticmethod
        def map() -> PartitionDependencies:
            return PartitionDependencies()

    assert NoPartitioning._outputs_
    assert MatchingPartitioning._outputs_

    for first_output in [
        Annotated[int, A],
        Annotated[list[dict[Any, Any]], C],
    ]:

        def mixed_build() -> Any:
            raise NotImplementedError

        mixed_build.__annotations__["return"] = cast(Any, tuple).__class_getitem__(
            (first_output, Annotated[list[dict[Any, Any]], D])
        )
        with pytest.raises(ValueError, match="all outputs must have the same partitioning scheme"):
            _define_bad_producer(build=mixed_build, build_kind="static")

    def partitioned_build() -> Annotated[list[dict[Any, Any]], C]:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - must be implemented when the `build` outputs are partitioned",
    ):
        _define_bad_producer(build=partitioned_build, build_kind="static")


def test_Producer_bad_signature() -> None:
    # Ensure no error if _abstract_
    class OkProducer(Producer):
        _abstract_ = True

    assert getattr(OkProducer, "_abstract_")  # noqa: B009

    with pytest.raises(ValueError, match=re.escape("BadProducer.build - must be implemented")):
        _define_bad_producer()

    def build_with_undefined_field(
        cls: type[Producer], a1: dict[Any, Any]
    ) -> Annotated[dict[Any, Any], A2]:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"BadProducer.build - the following parameter\(s\) must be defined as a field: {'a1'}",
    ):
        _define_bad_producer(build=build_with_undefined_field)

    def build_without_inputs(cls: type[Producer]) -> Annotated[dict[Any, Any], A2]:
        raise NotImplementedError

    def map_with_undefined_field(
        cls: type[Producer], a1: StoragePartitions
    ) -> PartitionDependencies:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - the following parameter\(s\) must be defined as a field: {'a1'}",
    ):
        _define_bad_producer(build=build_without_inputs, map_=map_with_undefined_field)

    def build_with_unused_field(
        cls: type[Producer], a1: dict[Any, Any]
    ) -> Annotated[dict[Any, Any], A3]:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"BadProducer - the following fields aren't used in `.build` or `.map`: {'a2'}",
    ):
        _define_bad_producer(annotations={"a1": A1, "a2": A2}, build=build_with_unused_field)

    def build_untyped(cls: type[Producer], a1: Any) -> Any:
        raise NotImplementedError

    build_untyped.__annotations__.pop("a1")

    with pytest.raises(ValueError, match="must have a type hint"):
        _define_bad_producer(annotations={"a1": A1}, build=build_untyped)

    def build_with_non_artifact_field(cls: type[Producer], a1: str) -> tuple[A2, A3]:
        raise NotImplementedError

    with pytest.raises(ValueError, match="type hint must be an Artifact subclass"):
        _define_bad_producer(annotations={"a1": str}, build=build_with_non_artifact_field)

    def build_with_default(
        cls: type[Producer],
        a1: dict[Any, Any] = cast(Any, A1()),
    ) -> Any:
        raise NotImplementedError

    with pytest.raises(ValueError, match="must not have a default"):
        _define_bad_producer(annotations={"a1": A1}, build=build_with_default)

    def build_positional_only(cls: type[Producer], a1: dict[Any, Any], /) -> Any:
        raise NotImplementedError

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):
        _define_bad_producer(annotations={"a1": A1}, build=build_positional_only)

    def build_varargs(cls: type[Producer], *a1: dict[Any, Any]) -> Any:
        raise NotImplementedError

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):
        _define_bad_producer(annotations={"a1": A1}, build=build_varargs)

    def build_kwargs(cls: type[Producer], **a1: dict[Any, Any]) -> Any:
        raise NotImplementedError

    with pytest.raises(ValueError, match="must be usable as a keyword argument"):
        _define_bad_producer(annotations={"a1": A1}, build=build_kwargs)

    def build_without_return(cls: type[Producer], a1: dict[Any, Any]) -> Any:
        raise NotImplementedError

    build_without_return.__annotations__.pop("return")

    with pytest.raises(ValueError, match="a return value must be set"):
        _define_bad_producer(annotations={"a1": A1}, build=build_without_return)

    def build_returning_none(cls: type[Producer], a1: dict[Any, Any]) -> None:
        raise NotImplementedError

    with pytest.raises(ValueError, match="missing return signature"):
        _define_bad_producer(annotations={"a1": A1}, build=build_returning_none)

    with pytest.raises(
        ValueError, match=re.escape("BadProducer.a1 - field must not have a default.")
    ):
        _define_bad_producer(
            annotations={"a1": A1},
            defaults={"a1": None},
            build=build_without_return,
        )

    with pytest.raises(ValueError, match=re.escape("BadProducer.a1 - field must not be optional.")):
        _define_bad_producer(annotations={"a1": A1 | None}, build=build_without_return)

    with pytest.raises(
        ValueError,
        match=r"BadProducer.a1 - field must not have a default.",
    ):
        _define_bad_producer(
            annotations={"a1": A1},
            defaults={"a1": A1()},
            build=build_with_undefined_field,
        )

    def build_with_mismatched_artifact(
        cls: type[Producer],
        a1: Annotated[dict[Any, Any], A2],
    ) -> A2:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"BadProducer.build a1 param - annotation Artifact class .* does not match that set on the field",
    ):
        _define_bad_producer(annotations={"a1": A1}, build=build_with_mismatched_artifact)

    def build_with_incompatible_view(cls: type[Producer]) -> Annotated[str, A2]:
        raise NotImplementedError

    with pytest.raises(ValueError, match=r"str.* cannot be used to represent Struct"):
        _define_bad_producer(build=build_with_incompatible_view)

    def build_with_incompatible_type(
        cls: type[Producer],
    ) -> Annotated[int, Int64(), A2]:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"the specified Type \(`Int64\(.*\)`\) is not compatible with the Artifact's Type",
    ):
        _define_bad_producer(build=build_with_incompatible_type)

    def instance_build(self: Producer) -> Annotated[dict[Any, Any], A2]:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"BadProducer.build - must be a @classmethod or @staticmethod",
    ):
        _define_bad_producer(build=instance_build, build_kind="instance")

    def class_build(cls: type[Producer]) -> Annotated[dict[Any, Any], A2]:
        raise NotImplementedError

    def instance_map(self: Producer) -> PartitionDependencies:
        raise NotImplementedError

    with pytest.raises(
        ValueError,
        match=r"BadProducer.map - must be a @classmethod or @staticmethod",
    ):
        _define_bad_producer(build=class_build, map_=instance_map, map_kind="instance")


def test_Producer_bad_init() -> None:
    with pytest.raises(TypeError, match="cannot be instantiated directly"):
        Producer()
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        cast(Any, DummyProducer)(junk=5)
    with pytest.raises(ValueError, match="Field required"):
        cast(Any, DummyProducer)()
    with pytest.raises(ValueError, match="Input should be a valid dictionary or instance of A1"):
        cast(Any, DummyProducer)(a1=5)
    with pytest.raises(ValueError, match="Input should be a valid dictionary or instance of A1"):
        cast(Any, DummyProducer)(a1=A2())


def test_Producer_bad_out() -> None:
    producer = DummyProducer(a1=A1())
    with pytest.raises(ValueError, match="expected 2 arguments of"):
        cast(Any, producer.out)(1)
    with pytest.raises(
        ValueError, match=r"DummyProducer.out\(\) 1st argument - expected an instance of"
    ):
        cast(Any, producer.out)(1, 2)
    with pytest.raises(
        ValueError, match=r"DummyProducer.out\(\) 2nd argument - expected an instance of"
    ):
        producer.out(A2(), A2())
    output = producer.out(A2(), A3())
    with pytest.raises(ValueError, match="is produced by"):
        producer.out(*output)


numbers_type = List(element=Int64())


class Numbers(Artifact):
    type: Type = numbers_type


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
    numbers_view_write = numbers_view_read.model_copy(update={"mode": "WRITE"})

    def plusone_build(numbers: list[int]) -> list[int]:
        return [n + 1 for n in numbers]

    plusone_build.__annotations__ = {"numbers": annotation, "return": annotation}
    plusone = producer_decorator()(plusone_build)

    assert plusone._input_artifact_classes_ == {"numbers": artifact_class}
    assert plusone._build_inputs_ == {"numbers": numbers_view_read}
    assert plusone._outputs_ == (numbers_view_write,)
    assert plusone._map_inputs_ == {"numbers"}


def test_Producer_io_checks() -> None:
    # NOTE: Using class style with different kwargs for map/build to confirm we only validate
    # `build` kwargs (since we won't read map-only kwargs anyway).
    class CheckIO(Producer):
        m: Artifact
        b: Artifact

        @staticmethod
        def map(m: StoragePartitions) -> PartitionDependencies:
            raise NotImplementedError

        @staticmethod
        def build(b: int) -> int:
            return b + 1

    # Create some fake format for which we don't have any io implemented.
    class FakeFormat(Format):
        type_system: ClassVar[TypeSystem] = dummy_type_system

    good_artifact = Artifact.cast(1)
    fake_artifact = good_artifact.model_copy(update={"format": FakeFormat()})

    # Check reading
    producer = CheckIO(m=fake_artifact, b=good_artifact)
    with pytest.raises(ValueError, match=re.escape("No `io.read` implementation found for: ")):
        CheckIO(m=fake_artifact, b=fake_artifact)
    # Check writing
    producer.out(good_artifact)
    with pytest.raises(ValueError, match=re.escape("No `io.write` implementation found for: ")):
        producer.out(fake_artifact)

    @io.register_reader
    def dummy_reader(
        type_: Type,
        format: FakeFormat,
        storage_partitions: Sequence[StoragePartition],
        view: View,
    ) -> Any:
        return "test-read"

    assert callable(dummy_reader)

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
        return None

    assert callable(dummy_writer)

    # and write it.
    producer.out(fake_artifact)
