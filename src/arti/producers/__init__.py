__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from inspect import Parameter, Signature, getattr_static
from typing import Annotated, Any, ClassVar, TypeVar, Union, get_args, get_origin

from pydantic.fields import ModelField

from arti.annotations import Annotation
from arti.artifacts import Artifact, BaseArtifact, Statistic
from arti.fingerprints import Fingerprint
from arti.internal import wrap_exc
from arti.internal.models import Model
from arti.internal.type_hints import NoneType, lenient_issubclass, signature
from arti.internal.utils import frozendict, ordinal
from arti.partitions import CompositeKeyTypes
from arti.versions import SemVer, Version
from arti.views import View


def _commas(vals: Iterable[Any]) -> str:
    return ", ".join([str(v) for v in vals])


ArtifactViewPair = tuple[type[Artifact], type[View]]
BuildInputMetadata = frozendict[str, ArtifactViewPair]
MapInputMetadata = frozendict[str, type[Artifact]]
# mypy doesn't yet support homogeneous tuple aliases: https://github.com/python/mypy/issues/9980
OutputMetadata = tuple[ArtifactViewPair, ...]  # type: ignore

_T = TypeVar("_T")


class Producer(Model):
    """A Producer is a task that builds one or more Artifacts."""

    # User fields/methods

    annotations: tuple[Annotation, ...] = ()
    version: ClassVar[Version] = SemVer(major=0, minor=0, patch=1)

    # build and map must be @classmethods or @staticmethods.
    build: ClassVar[Callable[..., Any]]
    map: ClassVar[Callable[..., Any]]

    # Internal fields/methods

    _abstract_: ClassVar[bool] = True
    # NOTE: The following are set in __init_subclass__
    _artifact_fields_: ClassVar[dict[str, ModelField]]
    _build_sig_: ClassVar[Signature]
    _build_input_metadata_: ClassVar[BuildInputMetadata]
    _output_metadata_: ClassVar[OutputMetadata]
    _map_sig_: ClassVar[Signature]
    _map_input_metadata_: ClassVar[MapInputMetadata]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            with wrap_exc(ValueError, prefix=cls.__name__):
                cls._artifact_fields_ = cls._validate_fields()
                with wrap_exc(ValueError, prefix=".build"):
                    (
                        cls._build_sig_,
                        cls._build_input_metadata_,
                        cls._output_metadata_,
                    ) = cls._validate_build_sig()
                with wrap_exc(ValueError, prefix=".map"):
                    cls._map_sig_, cls._map_input_metadata_ = cls._validate_map_sig()
                cls._validate_no_unused_fields()

    @classmethod
    def _get_view_from_annotation(cls, annotation: Any, artifact: type[Artifact]) -> type[View]:
        view = View.get_class_for(annotation)
        wrap_msg = f"{artifact.__name__}"
        if artifact.is_partitioned:
            wrap_msg = f"partitions of {artifact.__name__}"
        with wrap_exc(ValueError, prefix=f" ({wrap_msg})"):
            view.type_system.check_similarity(arti=artifact._type, python_type=annotation)
        return view

    @classmethod
    def _validate_fields(cls) -> dict[str, ModelField]:
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
        return artifact_fields

    @classmethod
    def _validate_parameters(
        cls, sig: Signature, *, validator: Callable[[str, Parameter, type[Artifact]], _T]
    ) -> Iterator[_T]:
        if undefined_params := set(sig.parameters) - set(cls._artifact_fields_):
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
        with wrap_exc(ValueError, prefix=f" {ordinal(i)} return"):
            hint_example = "eg: `Annotated[pd.DataFrame, MyArtifact]`"
            origin, args = get_origin(annotation), get_args(annotation)
            if origin is not Annotated:
                raise ValueError(f"must be an Annotated hint ({hint_example})")
            # We don't need the underlying type here - `_get_view_from_annotation` requires the entire
            # (possibly Annotated) annotation in order to discover explicitly set Views.
            _, *hints = args
            artifacts = [hint for hint in hints if lenient_issubclass(hint, Artifact)]
            if len(artifacts) == 0:
                raise ValueError(f"output Artifact is not set ({hint_example})")
            if len(artifacts) > 1:
                raise ValueError("multiple output Artifacts set")
            artifact = artifacts[0]
            return artifact, cls._get_view_from_annotation(annotation, artifact)

    @classmethod
    def _validate_build_sig(cls) -> tuple[Signature, BuildInputMetadata, OutputMetadata]:
        """Validate the .build method"""
        if not hasattr(cls, "build"):
            raise ValueError("must be implemented")
        if not isinstance(getattr_static(cls, "build"), (classmethod, staticmethod)):
            raise ValueError("must be a @classmethod or @staticmethod")
        build_sig = signature(cls.build, force_tuple_return=True, remove_owner=True)
        # Validate the parameters
        build_input_metadata = BuildInputMetadata(
            cls._validate_parameters(
                build_sig,
                validator=(
                    lambda name, param, artifact: (
                        name,
                        (artifact, cls._get_view_from_annotation(param.annotation, artifact)),
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
            for i, annotation in enumerate(return_annotation, 1)
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

        return build_sig, build_input_metadata, output_metadata

    @classmethod
    def _validate_map_sig(cls) -> tuple[Signature, MapInputMetadata]:
        """Validate partitioned Artifacts and the .map method"""
        if not hasattr(cls, "map"):
            # TODO: if (input_is_partitioned, output_is_partitioned):
            # - (input_is, output_is):
            #       Error requiring override (even w/ equivalent partitioning, we can't know if the
            #       columns actually align)
            # - (input_is, output_is_not):
            #       Default to all input partitions -> the one output, but may be overridden to
            #       filter input partitions
            # - (input_is_not, output_is):
            #       Error requiring override (they must define the output partitions by inspecting
            #       the input statistics or whatever)
            # - (input_is_not, output_is_not):
            #       Default 1<->1, any override must return 1<->1 too
            def map() -> Any:
                """Map dependencies between input and output Artifact partitions.

                If there are multiple output Artifacts, they must have equivalent partitioning
                schemes.
                """
                raise NotImplementedError("the default")

            cls.map = staticmethod(map)  # type: ignore
        if not isinstance(getattr_static(cls, "map"), (classmethod, staticmethod)):
            raise ValueError("must be a @classmethod or @staticmethod")
        map_sig = signature(cls.map)

        def validate_map_param(
            name: str, param: Parameter, artifact: type[Artifact]
        ) -> tuple[str, type[Artifact]]:
            # TODO: Handle (and save off) proper map annotations: expect some
            # ArtifactPartition[MyArtifact] type and match on wrapped type
            if artifact != param.annotation:
                raise ValueError(f"parameter type hint must match the field: {artifact}")
            return name, artifact

        map_input_metadata = MapInputMetadata(
            cls._validate_parameters(map_sig, validator=validate_map_param)
        )
        return map_sig, map_input_metadata  # TODO: Verify map output hint matches TBD spec

    @classmethod
    def _validate_no_unused_fields(cls) -> None:
        if unused_fields := set(cls._artifact_fields_) - (
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

    @property
    def fingerprint(self) -> Fingerprint:
        """Return a Fingerprint of the Producer key + version.

        The input and output Artifacts are ignored as the Producer instance may be used multiple
        times to produce different output partitions. The "entropy mixing" will be performed for
        *each* output with the static Producer.fingerprint + that output's specific input
        partition dependencies.
        """
        return Fingerprint.from_string(self._class_key_).combine(self.version.fingerprint)

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
                if artifact.producer is not None:
                    raise ValueError(f"{artifact} is produced by {artifact.producer}!")
                return artifact.copy(update={"producer": self})

        outputs = tuple(validate(artifact, ord=i) for i, artifact in enumerate(outputs))
        if len(outputs) == 1:
            return outputs[0]
        return outputs


BaseArtifact.update_forward_refs(Producer=Producer)
Statistic.update_forward_refs(Producer=Producer)
Artifact.update_forward_refs(Producer=Producer)
