from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from inspect import Signature, getattr_static
from typing import Any, ClassVar, Union, cast

from pydantic.fields import ModelField

from arti.annotations.core import Annotation
from arti.artifacts.core import Artifact
from arti.fingerprints.core import Fingerprint
from arti.internal.models import Model
from arti.internal.type_hints import signature
from arti.internal.utils import ordinal
from arti.versions.core import SemVer, Version


def _commas(vals: Iterable[Any]) -> str:
    return ", ".join([str(v) for v in vals])


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
    _map_sig_: ClassVar[Signature]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls._abstract_:
            cls._artifact_fields_ = cls._validate_fields()
            cls._build_sig_ = cls._validate_build_sig()
            cls._map_sig_ = cls._validate_map_sig()
            cls._validate_cohesive_params()

    @classmethod
    def _validate_fields(cls) -> dict[str, ModelField]:
        # NOTE: Aside from the base producer fields, all others should be Artifacts.
        #
        # Users can set additional class attributes, but they must be properly hinted as ClassVars.
        # These won't interact with the "framework" and can't be parameters to build/map.
        artifact_fields = {k: v for k, v in cls.__fields__.items() if k not in Producer.__fields__}
        for name, field in artifact_fields.items():
            if not (field.default is None and field.default_factory is None and field.required):
                raise ValueError(
                    f"{cls.__name__}.{name} - field must not have a default nor be Optional."
                )
            if not issubclass(field.outer_type_, Artifact):
                raise ValueError(
                    f"{cls.__name__}.{name} - type hint must be an Artifact subclass, got: {field.outer_type_}"
                )
        return artifact_fields

    @classmethod
    def _validate_parameters(cls, fn_name: str, sig: Signature) -> None:
        for name, param in sig.parameters.items():
            if param.annotation is param.empty:
                raise ValueError(
                    f"{cls.__name__}.{fn_name} - `{name}` parameter must have a type hint."
                )
            if param.default is not param.empty:
                raise ValueError(
                    f"{cls.__name__}.{fn_name} - `{name}` parameter must not have a default."
                )
            if param.kind not in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY):
                raise ValueError(
                    "{cls.__name__}.{fn_name} - `{name}` parameter must be usable as a keyword argument."
                )

    @classmethod
    def _validate_build_sig(cls) -> Signature:
        """Validate the .build method"""
        if not hasattr(cls, "build"):
            raise ValueError(f"{cls.__name__} - Producers must implement the `build` method.")
        if not isinstance(getattr_static(cls, "build"), (classmethod, staticmethod)):
            raise ValueError(f"{cls.__name__}.build - must be a @classmethod or @staticmethod.")
        build_sig = signature(cls.build)
        cls._validate_parameters("build", build_sig)
        # Validate the return definition
        return_annotation = build_sig.return_annotation
        if return_annotation is build_sig.empty:
            # TODO: "side effect" Producers: https://github.com/replicahq/artigraph/issues/11
            raise ValueError(
                f"{cls.__name__}.build - A return value must be set with the output Artifact(s)."
            )
        for i, annotation in enumerate(return_annotation, 1):
            if annotation is None or not issubclass(annotation, Artifact):
                raise ValueError(
                    f"{cls.__name__}.build - The {ordinal(i)} return value must be an Artifact, got: {annotation}"
                )
        return build_sig

    @classmethod
    def _validate_map_sig(cls) -> Signature:
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
                raise ValueError("Implement the default")

            cls.map = staticmethod(map)  # type: ignore
        if not isinstance(getattr_static(cls, "map"), (classmethod, staticmethod)):
            raise ValueError(f"{cls.__name__}.map - must be a @classmethod or @staticmethod.")
        map_sig = signature(cls.map)
        cls._validate_parameters("map", map_sig)
        # TODO: Verify cls._build_sig_ output Artifacts (if multiple) have equivalent partitioning schemes
        return map_sig  # TODO: Verify map output hint matches TBD spec

    @classmethod
    def _validate_cohesive_params(cls) -> None:
        artifact_names = set(cls._artifact_fields_)
        if unused_fields := artifact_names - (
            set(cls._build_sig_.parameters) | set(cls._map_sig_.parameters)
        ):
            raise ValueError(
                f"{cls.__name__} - the following fields aren't used in `.build` or `.map`: {unused_fields}"
            )
        for (fn, sig) in (
            ("build", cls._build_sig_),
            ("map", cls._map_sig_),
        ):
            if undefined_params := set(sig.parameters) - artifact_names:
                raise ValueError(
                    f"{cls.__name__}.{fn} - the following parameter(s) must be defined as a field: {undefined_params}"
                )
            for param in sig.parameters.values():
                field = cls.__fields__[param.name]
                # TODO: Handle proper per-method annotations:
                # - build: Replace with python type and match to a View + check if we can convert
                #          from the Artifact's format/storage.
                # - map:   Expect some ArtifactPartition[MyArtifact] type and match on wrapped type
                #
                # These checks should probably apply at class definition _and_ instantiation.
                if field.type_ != param.annotation:
                    raise ValueError(
                        f"{cls.__name__}.{fn} - `{param.name}` parameter type hint must match the field: {field.type_}"
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
            outputs = tuple(artifact() for artifact in self._build_sig_.return_annotation)
        passed_n, expected_n = len(outputs), len(self._build_sig_.return_annotation)
        if passed_n != expected_n:
            ret_str = _commas(self._build_sig_.return_annotation)
            raise ValueError(
                f"{self._class_key_}.out() - Expected {expected_n} arguments of ({ret_str}), but got: {outputs}"
            )

        def validate(artifact: Artifact, *, ord: int) -> Artifact:
            expected_type = self._build_sig_.return_annotation[ord]
            if not isinstance(artifact, expected_type):
                raise ValueError(
                    f"{self._class_key_}.out() - Expected the {ordinal(ord+1)} argument to be {expected_type}, got {type(artifact)}"
                )
            if artifact.producer is not None:
                raise ValueError(f"{artifact} is produced by {artifact.producer}!")
            return cast(Artifact, artifact.copy(update={"producer": self}))

        outputs = tuple(validate(artifact, ord=i) for i, artifact in enumerate(outputs))
        if len(outputs) == 1:
            return outputs[0]
        return outputs
