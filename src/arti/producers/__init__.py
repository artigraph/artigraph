__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
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

from arti.annotations import Annotation
from arti.artifacts import Artifact, BaseArtifact, Statistic
from arti.fingerprints import Fingerprint
from arti.internal import wrap_exc
from arti.internal.models import Model
from arti.internal.type_hints import NoneType, lenient_issubclass, signature
from arti.internal.utils import frozendict, ordinal
from arti.partitions import CompositeKey, CompositeKeyTypes, NotPartitioned
from arti.storage import StoragePartitions
from arti.versions import SemVer, Version
from arti.views import View


def _commas(vals: Iterable[Any]) -> str:
    return ", ".join([str(v) for v in vals])


# TODO: Add @validator over all (build) fields checking for an io.{read,write} handler.


ArtifactViewPair = tuple[type[Artifact], type[View]]
BuildInputViews = frozendict[str, type[View]]
MapInputMetadata = frozendict[str, type[Artifact]]
OutputMetadata = tuple[ArtifactViewPair, ...]

PartitionDependencies = frozendict[CompositeKey, frozendict[str, StoragePartitions]]

MapSig = Callable[..., PartitionDependencies]
BuildSig = Callable[..., Any]
ValidateSig = Callable[..., tuple[bool, str]]

_T = TypeVar("_T")


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
    _input_artifact_types_: ClassVar[frozendict[str, type[Artifact]]]
    _build_sig_: ClassVar[Signature]
    _build_input_views_: ClassVar[BuildInputViews]
    _output_metadata_: ClassVar[OutputMetadata]
    _map_sig_: ClassVar[Signature]
    _map_input_metadata_: ClassVar[MapInputMetadata]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            with wrap_exc(ValueError, prefix=cls.__name__):
                cls._input_artifact_types_ = cls._validate_fields()
                with wrap_exc(ValueError, prefix=".build"):
                    (
                        cls._build_sig_,
                        cls._build_input_views_,
                        cls._output_metadata_,
                    ) = cls._validate_build_sig()
                with wrap_exc(ValueError, prefix=".validate_output"):
                    cls._validate_validate_output_sig()
                with wrap_exc(ValueError, prefix=".map"):
                    cls._map_sig_, cls._map_input_metadata_ = cls._validate_map_sig()
                cls._validate_no_unused_fields()

    @classmethod
    def _get_artifact_from_annotation(cls, annotation: Any) -> type[Artifact]:
        # Avoid importing non-interface modules at root
        from arti.types.python import python_type_system

        origin, args = get_origin(annotation), get_args(annotation)
        if origin is not Annotated:
            return Artifact.from_type(python_type_system.to_artigraph(annotation, hints={}))
        annotation, *hints = args
        artifacts = [hint for hint in hints if lenient_issubclass(hint, Artifact)]
        if len(artifacts) == 0:
            return Artifact.from_type(python_type_system.to_artigraph(annotation, hints={}))
        if len(artifacts) > 1:
            raise ValueError("multiple Artifacts set")
        return cast(type[Artifact], artifacts[0])

    @classmethod
    def _get_view_from_annotation(cls, annotation: Any, artifact: type[Artifact]) -> type[View]:
        wrap_msg = f"{artifact.__name__}"
        if artifact.is_partitioned:
            wrap_msg = f"partitions of {artifact.__name__}"
        with wrap_exc(ValueError, prefix=f" ({wrap_msg})"):
            return View.get_class_for(annotation, validation_type=artifact._type)

    @classmethod
    def _validate_fields(cls) -> frozendict[str, type[Artifact]]:
        # NOTE: Aside from the base producer fields, all others should be Artifacts.
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
        cls, sig: Signature, *, validator: Callable[[str, Parameter, type[Artifact]], _T]
    ) -> Iterator[_T]:
        if undefined_params := set(sig.parameters) - set(cls._input_artifact_types_):
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
                artifact = cls.__fields__[param.name].outer_type_
                yield validator(name, param, artifact)

    @classmethod
    def _validate_build_sig_return(cls, annotation: Any, *, i: int) -> ArtifactViewPair:
        with wrap_exc(ValueError, prefix=f" {ordinal(i+1)} return"):
            artifact = cls._get_artifact_from_annotation(annotation)
            return artifact, cls._get_view_from_annotation(annotation, artifact)

    @classmethod
    def _validate_build_sig(cls) -> tuple[Signature, BuildInputViews, OutputMetadata]:
        """Validate the .build method"""
        if not hasattr(cls, "build"):
            raise ValueError("must be implemented")
        if not isinstance(getattr_static(cls, "build"), (classmethod, staticmethod)):
            raise ValueError("must be a @classmethod or @staticmethod")
        build_sig = signature(cls.build, force_tuple_return=True, remove_owner=True)
        # Validate the parameters
        build_input_metadata = BuildInputViews(
            cls._validate_parameters(
                build_sig,
                validator=(
                    lambda name, param, artifact: (
                        name,
                        cls._get_view_from_annotation(param.annotation, artifact),
                    )
                ),
            )
        )
        # Validate the return definition
        return_annotation = build_sig.return_annotation
        if return_annotation is build_sig.empty:
            # TODO: "side effect" Producers: https://github.com/replicahq/artigraph/issues/11
            raise ValueError("a return value must be set with the output Artifact(s).")
        if return_annotation == (NoneType,):
            raise ValueError("missing return signature")
        output_metadata = OutputMetadata(
            cls._validate_build_sig_return(annotation, i=i)
            for i, annotation in enumerate(return_annotation)
        )
        # Validate all output Artifacts have equivalent partitioning schemes.
        #
        # We currently require the partition key type *and* name to match, but in the
        # future we might be able to extend the dependency metadata to support
        # heterogeneous names if necessary.
        artifacts_by_composite_key = defaultdict[CompositeKeyTypes, list[type[Artifact]]](list)
        for (artifact, _) in output_metadata:
            artifacts_by_composite_key[artifact.partition_key_types].append(artifact)
        if len(artifacts_by_composite_key) != 1:
            raise ValueError("all output Artifacts must have the same partitioning scheme")
        # TODO: Save off output composite_key_types

        return build_sig, build_input_metadata, output_metadata

    @classmethod
    def _validate_validate_output_sig(cls) -> None:
        build_output_types = [
            get_args(hint)[0] if get_origin(hint) is Annotated else hint
            for hint in cls._build_sig_.return_annotation
        ]
        match_build_str = f"match the `.build` return (`{build_output_types}`)"
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
            if not all(param_matches(param, output_type) for output_type in build_output_types):
                with wrap_exc(ValueError, prefix=f" {param.name} param"):
                    raise ValueError(f"type hint must be `Any` or {match_build_str}")
        else:  # Otherwise, check pairwise
            if len(validate_parameters) != len(build_output_types):
                raise ValueError(f"must {match_build_str}")
            for i, (name, param) in enumerate(validate_parameters.items()):
                with wrap_exc(ValueError, prefix=f" {name} param"):
                    if param.default is not param.empty:
                        raise ValueError("must not have a default.")
                    if param.kind not in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
                        raise ValueError("must be usable as a positional argument.")
                    if not param_matches(param, (expected := build_output_types[i])):
                        raise ValueError(
                            f"type hint must match the {ordinal(i+1)} `.build` return (`{expected}`)"
                        )
        # TODO: Validate return signature?

    @classmethod
    def _validate_map_sig(cls) -> tuple[Signature, MapInputMetadata]:
        """Validate partitioned Artifacts and the .map method"""
        if not hasattr(cls, "map"):
            partitioned_outputs = [
                artifact for (artifact, view) in cls._output_metadata_ if artifact.is_partitioned
            ]
            # TODO: Add runtime checking of `map` output (ie: output aligns w/ output
            # artifacts and such).
            if partitioned_outputs:
                raise ValueError("must be implemented when the `build` outputs are partitioned")
            else:

                def map(**kwargs: StoragePartitions) -> PartitionDependencies:
                    return PartitionDependencies(
                        {NotPartitioned: {name: partitions for name, partitions in kwargs.items()}}
                    )

            # Narrow the map signature, which is validated below and used at graph build
            # time (via cls._map_input_metadata_) to determine what arguments to pass to
            # map.
            map.__signature__ = Signature(  # type: ignore
                [
                    Parameter(name=name, annotation=StoragePartitions, kind=Parameter.KEYWORD_ONLY)
                    for name, artifact in cls._input_artifact_types_.items()
                    if name in cls._build_input_views_
                ],
                return_annotation=PartitionDependencies,
            )
            cls.map = staticmethod(map)  # type: ignore
        if not isinstance(getattr_static(cls, "map"), (classmethod, staticmethod)):
            raise ValueError("must be a @classmethod or @staticmethod")
        map_sig = signature(cls.map)

        def validate_map_param(
            name: str, param: Parameter, artifact: type[Artifact]
        ) -> tuple[str, type[Artifact]]:
            # TODO: Should we add some ArtifactPartition[MyArtifact] type?
            if param.annotation != StoragePartitions:
                raise ValueError("type hint must be `StoragePartitions`")
            return name, artifact

        map_input_metadata = MapInputMetadata(
            cls._validate_parameters(map_sig, validator=validate_map_param)
        )
        return map_sig, map_input_metadata  # TODO: Verify map output hint matches TBD spec

    @classmethod
    def _validate_no_unused_fields(cls) -> None:
        if unused_fields := set(cls._input_artifact_types_) - (
            set(cls._build_sig_.parameters) | set(cls._map_sig_.parameters)
        ):
            raise ValueError(
                f"the following fields aren't used in `.build` or `.map`: {unused_fields}"
            )

    # NOTE: pydantic defines .__iter__ to return `self.__dict__.items()` to support `dict(model)`,
    # but we want to override to support easy expansion/assignment to a Graph  without `.out()` (eg:
    # `g.artifacts.a, g.artifacts.b = MyProducer(...)`).
    def __iter__(self) -> Iterator[Artifact]:  # type: ignore
        ret = self.out()
        if not isinstance(ret, tuple):
            ret = (ret,)
        return iter(ret)

    def compute_input_fingerprint(
        self, dependency_partitions: frozendict[str, StoragePartitions]
    ) -> Fingerprint:
        input_names = set(dependency_partitions)
        expected_names = set(self._build_input_views_)
        if input_names != expected_names:
            raise ValueError(
                f"Mismatched dependency inputs; expected {expected_names}, got {input_names}"
            )
        # We only care if the *code* or *input partition contents* changed, not if the input file
        # paths changed (but have the same content as a prior run).
        return Fingerprint.from_string(self._class_key_).combine(
            self.version.fingerprint,
            *(
                partition.content_fingerprint
                for name, partitions in dependency_partitions.items()
                for partition in partitions
            ),
        )

    @property
    def inputs(self) -> dict[str, Artifact]:
        return {k: getattr(self, k) for k in self._input_artifact_types_}

    def out(self, *outputs: Artifact) -> Union[Artifact, tuple[Artifact, ...]]:
        """Configure the output Artifacts this Producer will build.

        The arguments are matched to the `Producer.build` return signature in order.
        """
        if not outputs:
            # TODO: Raise a better error if the Artifacts don't have defaults set for
            # type/format/storage.
            outputs = tuple(artifact() for (artifact, _) in self._output_metadata_)
        passed_n, expected_n = len(outputs), len(self._build_sig_.return_annotation)
        if passed_n != expected_n:
            ret_str = _commas(self._build_sig_.return_annotation)
            raise ValueError(
                f"{self._class_key_}.out() - expected {expected_n} arguments of ({ret_str}), but got: {outputs}"
            )

        def validate(artifact: Artifact, *, ord: int) -> Artifact:
            (expected_type, _) = self._output_metadata_[ord]
            with wrap_exc(ValueError, prefix=f"{self._class_key_}.out() {ordinal(ord+1)} argument"):
                if not isinstance(artifact, expected_type):
                    raise ValueError(f"expected instance of {expected_type}, got {type(artifact)}")
                # TODO: Validate type/format/storage/view compatibility?
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
        __annotations__ = {}
        for param in signature(build).parameters.values():
            with wrap_exc(ValueError, prefix=f"{name} {param.name} param"):
                __annotations__[param.name] = Producer._get_artifact_from_annotation(
                    param.annotation
                )
        # If overriding, set an explicit "annotations" hint until [1] is released.
        #
        # 1: https://github.com/samuelcolvin/pydantic/pull/3018
        if annotations:
            __annotations__["annotations"] = tuple[Annotation, ...]  # type: ignore
        if version:
            __annotations__["version"] = Version  # type: ignore
        return type(
            name,
            (Producer,),
            {
                k: v
                for k, v in {
                    "__annotations__": __annotations__,
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
    position: int  # TODO: Support named output (defaulting to artifact classname)


BaseArtifact.update_forward_refs(ProducerOutput=ProducerOutput)
Statistic.update_forward_refs(ProducerOutput=ProducerOutput)
Artifact.update_forward_refs(ProducerOutput=ProducerOutput)
