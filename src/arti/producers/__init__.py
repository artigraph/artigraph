from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from collections.abc import Callable, Iterator
from inspect import Parameter, Signature, getattr_static
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Optional,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

from pydantic import validator
from pydantic.fields import ModelField

from arti.annotations import Annotation
from arti.artifacts import Artifact
from arti.fingerprints import Fingerprint
from arti.internal import wrap_exc
from arti.internal.models import Model
from arti.internal.type_hints import (
    NoneType,
    get_item_from_annotated,
    lenient_issubclass,
    signature,
)
from arti.internal.utils import frozendict, get_module_name, ordinal
from arti.partitions import CompositeKey, InputFingerprints, NotPartitioned, PartitionKey
from arti.storage import StoragePartitions
from arti.types import is_partitioned
from arti.versions import SemVer, Version
from arti.views import View

_T = TypeVar("_T")


MapInputs = set[str]
BuildInputs = frozendict[str, View]
Outputs = tuple[View, ...]

InputPartitions = frozendict[str, StoragePartitions]
PartitionDependencies = frozendict[CompositeKey, InputPartitions]
MapSig = Callable[..., PartitionDependencies]
BuildSig = Callable[..., Any]
ValidateSig = Callable[..., tuple[bool, str]]


class Producer(Model):
    """A Producer is a task that builds one or more Artifacts."""

    # User fields/methods

    annotations: tuple[Annotation, ...] = ()
    version: Version = SemVer(major=0, minor=0, patch=1)

    # The map/build/validate_outputs parameters are intended to be dynamic and set by subclasses,
    # however mypy doesn't like the "incompatible" signature on subclasses if actually defined here
    # (nor support ParamSpec yet). `map` is generated during subclassing if not set, `build` is
    # required, and `validate_outputs` defaults to no-op checks (hence is the only one with a
    # provided method).
    #
    # These must be @classmethods or @staticmethods.
    map: ClassVar[MapSig]
    build: ClassVar[BuildSig]
    if TYPE_CHECKING:
        validate_outputs: ClassVar[ValidateSig]
    else:

        @staticmethod
        def validate_outputs(*outputs: Any) -> Union[bool, tuple[bool, str]]:
            """Validate the `Producer.build` outputs, returning the status and a message.

            The validation status is applied to all outputs. If validation does not pass, the
            outputs will not be written to storage to prevent checkpointing the output. In the
            future, we may still write the data to ease debugging, but track validation status in
            the Backend (preventing downstream use).

            The arguments must not be mutated.

            The parameters must be usable with positional argument. The output of `build` will be
            passed in as it was returned, for example: `def build(...): return 1, 2` will result in
            `validate_outputs(1, 2)`.

            NOTE: `validate_outputs` is a stopgap until Statistics and Thresholds are fully implemented.
            """
            return True, "No validation performed."

    # Internal fields/methods

    _abstract_: ClassVar[bool] = True
    _fingerprint_excludes_ = frozenset(["annotations"])

    # NOTE: The following are set in __init_subclass__
    _input_artifact_classes_: ClassVar[frozendict[str, type[Artifact]]]
    _build_inputs_: ClassVar[BuildInputs]
    _build_sig_: ClassVar[Signature]
    _map_inputs_: ClassVar[MapInputs]
    _map_sig_: ClassVar[Signature]
    _outputs_: ClassVar[Outputs]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            with wrap_exc(ValueError, prefix=cls.__name__):
                cls._input_artifact_classes_ = cls._validate_fields()
                with wrap_exc(ValueError, prefix=".build"):
                    (
                        cls._build_sig_,
                        cls._build_inputs_,
                        cls._outputs_,
                    ) = cls._validate_build_sig()
                with wrap_exc(ValueError, prefix=".validate_output"):
                    cls._validate_validate_output_sig()
                with wrap_exc(ValueError, prefix=".map"):
                    cls._map_sig_, cls._map_inputs_ = cls._validate_map_sig()
                cls._validate_no_unused_fields()

    @classmethod
    def _validate_fields(cls) -> frozendict[str, type[Artifact]]:
        # NOTE: Aside from the base producer fields, all others should (currently) be Artifacts.
        #
        # Users can set additional class attributes, but they must be properly hinted as ClassVars.
        # These won't interact with the "framework" and can't be parameters to build/map.
        artifact_fields = {k: v for k, v in cls.__fields__.items() if k not in Producer.__fields__}
        for name, field in artifact_fields.items():
            with wrap_exc(ValueError, prefix=f".{name}"):
                if not (field.default is None and field.default_factory is None and field.required):
                    raise ValueError("field must not have a default nor be Optional.")
                if not lenient_issubclass(field.outer_type_, Artifact):
                    raise ValueError(
                        f"type hint must be an Artifact subclass, got: {field.outer_type_}"
                    )
        return frozendict({name: field.outer_type_ for name, field in artifact_fields.items()})

    @classmethod
    def _validate_parameters(
        cls, sig: Signature, *, validator: Callable[[str, Parameter], _T]
    ) -> Iterator[_T]:
        if undefined_params := set(sig.parameters) - set(cls._input_artifact_classes_):
            raise ValueError(
                f"the following parameter(s) must be defined as a field: {undefined_params}"
            )
        for name, param in sig.parameters.items():
            with wrap_exc(ValueError, prefix=f" {name} param"):
                if param.annotation is param.empty:
                    raise ValueError("must have a type hint.")
                if param.default is not param.empty:
                    raise ValueError("must not have a default.")
                if param.kind not in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY):
                    raise ValueError("must be usable as a keyword argument.")
                yield validator(name, param)

    @classmethod
    def _validate_build_param(cls, name: str, param: Parameter) -> tuple[str, View]:
        annotation = param.annotation
        field_artifact_class = cls._input_artifact_classes_[param.name]
        # If there is no Artifact hint, add in the field value as the default.
        if get_item_from_annotated(annotation, Artifact, is_subclass=True) is None:
            annotation = Annotated[annotation, field_artifact_class]
        view = View.from_annotation(annotation, mode="READ")
        if view.artifact_class != field_artifact_class:
            raise ValueError(
                f"annotation Artifact class ({view.artifact_class}) does not match that set on the field ({field_artifact_class})."
            )
        return name, view

    @classmethod
    def _validate_build_sig_return(cls, annotation: Any, *, i: int) -> View:
        with wrap_exc(ValueError, prefix=f" {ordinal(i+1)} return"):
            return View.from_annotation(annotation, mode="WRITE")

    @classmethod
    def _validate_build_sig(cls) -> tuple[Signature, BuildInputs, Outputs]:
        """Validate the .build method"""
        if not hasattr(cls, "build"):
            raise ValueError("must be implemented")
        if not isinstance(getattr_static(cls, "build"), (classmethod, staticmethod)):
            raise ValueError("must be a @classmethod or @staticmethod")
        build_sig = signature(cls.build, force_tuple_return=True, remove_owner=True)
        # Validate the parameters
        build_inputs = BuildInputs(
            cls._validate_parameters(build_sig, validator=cls._validate_build_param)
        )
        # Validate the return definition
        return_annotation = build_sig.return_annotation
        if return_annotation is build_sig.empty:
            # TODO: "side effect" Producers: https://github.com/artigraph/artigraph/issues/11
            raise ValueError("a return value must be set with the output Artifact(s).")
        if return_annotation == (NoneType,):
            raise ValueError("missing return signature")
        outputs = Outputs(
            cls._validate_build_sig_return(annotation, i=i)
            for i, annotation in enumerate(return_annotation)
        )
        # Validate all outputs have equivalent partitioning schemes.
        #
        # We currently require the partition key type *and* name to match, but in the future we
        # might be able to extend the dependency metadata to support heterogeneous names if
        # necessary.
        seen_key_types = {PartitionKey.types_from(view.type) for view in outputs}
        if len(seen_key_types) != 1:
            raise ValueError("all outputs must have the same partitioning scheme")

        return build_sig, build_inputs, outputs

    @classmethod
    def _validate_validate_output_sig(cls) -> None:
        build_output_hints = [
            get_args(hint)[0] if get_origin(hint) is Annotated else hint
            for hint in cls._build_sig_.return_annotation
        ]
        match_build_str = f"match the `.build` return (`{build_output_hints}`)"
        validate_parameters = signature(cls.validate_outputs).parameters

        def param_matches(param: Parameter, build_return: type) -> bool:
            # Skip checking non-hinted parameters to allow lambdas.
            #
            # NOTE: Parameter type hints are *contravariant* (you can't pass a "Manager" into a
            # function expecting an "Employee"), hence the lenient_issubclass has build_return as
            # the subtype and param.annotation as the supertype.
            return param.annotation is param.empty or lenient_issubclass(
                build_return, param.annotation
            )

        if (  # Allow `*args: Any` or `*args: T` for `build(...) -> tuple[T, ...]`
            len(validate_parameters) == 1
            and (param := tuple(validate_parameters.values())[0]).kind == param.VAR_POSITIONAL
        ):
            if not all(param_matches(param, output_hint) for output_hint in build_output_hints):
                with wrap_exc(ValueError, prefix=f" {param.name} param"):
                    raise ValueError(f"type hint must be `Any` or {match_build_str}")
        else:  # Otherwise, check pairwise
            if len(validate_parameters) != len(build_output_hints):
                raise ValueError(f"must {match_build_str}")
            for i, (name, param) in enumerate(validate_parameters.items()):
                with wrap_exc(ValueError, prefix=f" {name} param"):
                    if param.default is not param.empty:
                        raise ValueError("must not have a default.")
                    if param.kind not in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
                        raise ValueError("must be usable as a positional argument.")
                    if not param_matches(param, (expected := build_output_hints[i])):
                        raise ValueError(
                            f"type hint must match the {ordinal(i+1)} `.build` return (`{expected}`)"
                        )
        # TODO: Validate return signature?

    @classmethod
    def _validate_map_param(cls, name: str, param: Parameter) -> str:
        # TODO: Should we add some ArtifactPartition[MyArtifact] type?
        if param.annotation != StoragePartitions:
            raise ValueError("type hint must be `StoragePartitions`")
        return name

    @classmethod
    def _validate_map_sig(cls) -> tuple[Signature, MapInputs]:
        """Validate partitioned Artifacts and the .map method"""
        if not hasattr(cls, "map"):
            # TODO: Add runtime checking of `map` output (ie: output aligns w/ output
            # artifacts and such).
            if any(is_partitioned(view.type) for view in cls._outputs_):
                raise ValueError("must be implemented when the `build` outputs are partitioned")

            def map(**kwargs: StoragePartitions) -> PartitionDependencies:
                return PartitionDependencies({NotPartitioned: frozendict(kwargs)})

            # Narrow the map signature, which is validated below and used at graph build time (via
            # cls._map_inputs_) to determine what arguments to pass to map.
            map.__signature__ = Signature(  # type: ignore[attr-defined]
                [
                    Parameter(name=name, annotation=StoragePartitions, kind=Parameter.KEYWORD_ONLY)
                    for name, artifact in cls._input_artifact_classes_.items()
                    if name in cls._build_inputs_
                ],
                return_annotation=PartitionDependencies,
            )
            cls.map = cast(MapSig, staticmethod(map))
        if not isinstance(getattr_static(cls, "map"), (classmethod, staticmethod)):
            raise ValueError("must be a @classmethod or @staticmethod")
        map_sig = signature(cls.map)
        map_inputs = MapInputs(cls._validate_parameters(map_sig, validator=cls._validate_map_param))
        return map_sig, map_inputs  # TODO: Verify map output hint matches TBD spec

    @classmethod
    def _validate_no_unused_fields(cls) -> None:
        if unused_fields := set(cls._input_artifact_classes_) - (
            set(cls._build_sig_.parameters) | set(cls._map_sig_.parameters)
        ):
            raise ValueError(
                f"the following fields aren't used in `.build` or `.map`: {unused_fields}"
            )

    @validator("*")
    @classmethod
    def _validate_instance_artifact_args(cls, value: Artifact, field: ModelField) -> Artifact:
        if (view := cls._build_inputs_.get(field.name)) is not None:
            view.check_artifact_compatibility(value)
        return value

    # NOTE: pydantic defines .__iter__ to return `self.__dict__.items()` to support `dict(model)`,
    # but we want to override to support easy expansion/assignment to a Graph  without `.out()` (eg:
    # `g.artifacts.a, g.artifacts.b = MyProducer(...)`).
    def __iter__(self) -> Iterator[Artifact]:  # type: ignore[override]
        ret = self.out()
        if not isinstance(ret, tuple):
            ret = (ret,)
        return iter(ret)

    def compute_input_fingerprint(
        self, dependency_partitions: frozendict[str, StoragePartitions]
    ) -> Fingerprint:
        input_names = set(dependency_partitions)
        expected_names = set(self._build_inputs_)
        if input_names != expected_names:
            raise ValueError(
                f"Mismatched dependency inputs; expected {expected_names}, got {input_names}"
            )
        # We only care if the *code* or *input partition contents* changed, not if the input file
        # paths changed (but have the same content as a prior run).
        return Fingerprint.from_string(self._class_key_).combine(
            self.version.fingerprint,
            *(
                partition.get_or_compute_content_fingerprint()
                for name, partitions in dependency_partitions.items()
                for partition in partitions
            ),
        )

    def compute_dependencies(
        self, input_partitions: InputPartitions
    ) -> tuple[PartitionDependencies, InputFingerprints]:
        # TODO: Validate the partition_dependencies against the Producer's partitioning scheme and
        # such (basically, check user error). eg: if output is not partitioned, we expect only 1
        # entry in partition_dependencies (NotPartitioned).
        partition_dependencies = self.map(
            **{
                name: partitions
                for name, partitions in input_partitions.items()
                if name in self._map_inputs_
            }
        )
        partition_input_fingerprints = InputFingerprints(
            {
                composite_key: self.compute_input_fingerprint(dependency_partitions)
                for composite_key, dependency_partitions in partition_dependencies.items()
            }
        )
        return partition_dependencies, partition_input_fingerprints

    @property
    def inputs(self) -> dict[str, Artifact]:
        return {k: getattr(self, k) for k in self._input_artifact_classes_}

    def out(self, *outputs: Artifact) -> Union[Artifact, tuple[Artifact, ...]]:
        """Configure the output Artifacts this Producer will build.

        The arguments are matched to the `Producer.build` return signature in order.
        """
        if not outputs:
            outputs = tuple(view.artifact_class(type=view.type) for view in self._outputs_)
        passed_n, expected_n = len(outputs), len(self._build_sig_.return_annotation)
        if passed_n != expected_n:
            ret_str = ", ".join([str(v) for v in self._build_sig_.return_annotation])
            raise ValueError(
                f"{self._class_key_}.out() - expected {expected_n} arguments of ({ret_str}), but got: {outputs}"
            )

        def validate(artifact: Artifact, *, ord: int) -> Artifact:
            view = self._outputs_[ord]
            with wrap_exc(ValueError, prefix=f"{self._class_key_}.out() {ordinal(ord+1)} argument"):
                view.check_artifact_compatibility(artifact)
                if artifact.producer_output is not None:
                    raise ValueError(
                        f"{artifact} is produced by {artifact.producer_output.producer}!"
                    )
            return artifact.copy(
                update={"producer_output": ProducerOutput(producer=self, position=ord)}
            )

        outputs = tuple(validate(artifact, ord=i) for i, artifact in enumerate(outputs))
        if len(outputs) == 1:
            return outputs[0]
        return outputs


def producer(
    *,
    annotations: Optional[tuple[Annotation, ...]] = None,
    map: Optional[MapSig] = None,
    name: Optional[str] = None,
    validate_outputs: Optional[ValidateSig] = None,
    version: Optional[Version] = None,
) -> Callable[[BuildSig], type[Producer]]:
    def decorate(build: BuildSig) -> type[Producer]:
        nonlocal name
        name = build.__name__ if name is None else name
        __annotations__: dict[str, Any] = {}
        for param in signature(build).parameters.values():
            with wrap_exc(ValueError, prefix=f"{name} {param.name} param"):
                view = View.from_annotation(param.annotation, mode="READ")
                __annotations__[param.name] = view.artifact_class
        # If overriding, set an explicit "annotations" hint until [1] is released.
        #
        # 1: https://github.com/samuelcolvin/pydantic/pull/3018
        if annotations:
            __annotations__["annotations"] = tuple[Annotation, ...]
        if version:
            __annotations__["version"] = Version
        return type(
            name,
            (Producer,),
            {
                k: v
                for k, v in {
                    "__annotations__": __annotations__,
                    "__module__": get_module_name(depth=2),  # Not our module, but our caller's.
                    "annotations": annotations,
                    "build": staticmethod(build),
                    "map": None if map is None else staticmethod(map),
                    "validate_outputs": (
                        None if validate_outputs is None else staticmethod(validate_outputs)
                    ),
                    "version": version,
                }.items()
                if v is not None
            },
        )

    return decorate


class ProducerOutput(Model):
    producer: Producer
    position: int  # TODO: Support named output (defaulting to artifact classname?)


Artifact.update_forward_refs(ProducerOutput=ProducerOutput)
