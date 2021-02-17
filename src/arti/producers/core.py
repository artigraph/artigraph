from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from inspect import Signature
from typing import Any, ClassVar

from arti.artifacts.core import Artifact
from arti.internal.type_hints import signature
from arti.internal.utils import ordinal

# IDEA: Allow Producer annotations, such as "point of contact", etc.


def _commas(vals: Iterable[Any]) -> str:
    return ", ".join([str(v) for v in vals])


class Producer:

    # User methods

    # Relax mypy "incompatible signature" warning for subclasses. Seems this must come before the
    # actual definitions, hence the None.
    build: Callable[..., Any]
    map: Callable[..., Any]

    # pylint: disable=function-redefined,no-self-use
    def build(self, **kwargs: Artifact) -> tuple[Artifact, ...]:  # type: ignore # pragma: no cover
        raise NotImplementedError(
            f"{type(self).__name__} - Producers must implement the `build` method."
        )

    # pylint: disable=function-redefined,no-self-use
    def map(self, **kwargs: Artifact) -> Any:  # type: ignore
        """ Map dependencies between input and output Artifact partitions.

            If there are multiple output Artifacts, they must have equivalent
            partitioning schemes.

            The method parameters must match the `build` method.
        """
        # TODO: if (input_is_partitioned, output_is_partitioned):
        # - (input_is,     output_is    ): Must be overridden (even w/ equivalent partitioning, we can't know if the columns actually align)
        # - (input_is,     output_is_not): Default to all input partitions -> the one output, but may be overridden to filter input partitions
        # - (input_is_not, output_is    ): Must be overridden (they must define the output partitions by inspecting the input statistics or whatever)
        # - (input_is_not, output_is_not): Default 1<->1, any override must return 1<->1 too
        raise ValueError("Implement the default")

    # Internal methods

    signature: ClassVar[Signature]

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls.signature = signature(cls.build)
        cls._validate_build_sig()
        cls._validate_map_sig()
        super().__init_subclass__()

    @classmethod
    def _validate_build_sig(cls) -> None:
        """ Validate the .build method
        """
        if cls.build is Producer.build:
            raise ValueError(f"{cls.__name__} - Producers must implement the `build` method.")
        # Validate the parameter definition
        for name, param in cls.signature.parameters.items():
            if param.annotation is param.empty:
                raise ValueError(f"{cls.__name__} - `{name}` must have a type hint.")
            # IDEA: Rather than an Artifact, we should make the Producers accept a View Annotated
            # with the Artifact.
            #
            # IDEA: To improve/ease hinting for simple Producer inputs, we may want to support
            # scalar python types (int, str, etc) in the build/map type annotations, which could be
            # converted with Artifact.cast into some "LiteralArtifact"s.
            if not issubclass(param.annotation, Artifact):
                raise ValueError(
                    f"{cls.__name__} - `{name}` type hint must be an Artifact subclass, got: {param}"
                )
            if param.default is not param.empty:
                raise ValueError(f"{cls.__name__} - `{name}` parameter must not have a default.")
            if param.kind not in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY):
                raise ValueError(
                    "{cls.__name__} - `{name}` parameter must be usable as a keyword argument."
                )
        # Validate the return definition
        return_annotation = cls.signature.return_annotation
        if return_annotation is cls.signature.empty:
            # NOTE: Should we support Producers without outputs? Would they just run
            # every build and be "side effect only"? Perhaps we could add an implicit
            # checkpoint for run-once semantics.
            raise ValueError(
                f"{cls.__name__} - A return value must be set with the output Artifact(s)."
            )
        for i, annotation in enumerate(return_annotation, 1):
            if annotation is None or not issubclass(annotation, Artifact):
                raise ValueError(
                    f"{cls.__name__} - The {ordinal(i)} return value must be an Artifact, got: {annotation}"
                )

    @classmethod
    def _validate_map_sig(cls) -> None:
        """ Validate partitioned Artifacts and the .map method
        """
        # TODO: Verify cls.signature output Artifacts (if multiple) have equivalent partitioning schemes
        sig = signature(cls.map)
        map_is_overridden = cls.map is not Producer.map
        map_params_match = cls.signature.parameters == sig.parameters
        if map_is_overridden and not map_params_match:
            map_param = _commas(sig.parameters.values())
            build_param = _commas(cls.signature.parameters.values())
            raise ValueError(
                f"{cls.__name__} - The parameters to `map` ({map_param}) must match `build` ({build_param})."
            )
        if not map_is_overridden:
            pass  # TODO: Error if the output is partitioned
        pass  # TODO: Verify map output hint matches TBD spec

    def __init__(self, **kwargs: Artifact) -> None:
        if type(self) is Producer:
            raise ValueError("Producer cannot be instantiated directly!")
        (self.input_artifacts,) = self._validate_build_args(**kwargs)
        self.output_artifacts = tuple(artifact() for artifact in self.signature.return_annotation)
        super().__init__()

    def _validate_build_args(self, **kwargs: Artifact) -> tuple[dict[str, Artifact]]:
        passed_params, expected_params = set(kwargs), set(self.signature.parameters)
        if unknown := passed_params - expected_params:
            raise ValueError(f"{type(self).__name__} - Unknown argument(s): {unknown}")
        if missing := expected_params - passed_params:
            # TODO: Support Optional input/output artifacts?
            raise ValueError(f"{type(self).__name__} - Missing argument(s): {missing}")
        for name, arg in kwargs.items():
            expected_type = self.signature.parameters[name].annotation
            if not isinstance(arg, expected_type):
                raise ValueError(
                    f"{type(self).__name__} - `{name}` expects an instance of {expected_type}, got: {arg}"
                )
        return ({k: v for k, v in kwargs.items() if isinstance(v, Artifact)},)

    @property
    def input_artifacts(self) -> dict[str, Artifact]:
        return self._input_artifacts

    @input_artifacts.setter
    def input_artifacts(self, values: dict[str, Artifact]) -> None:
        for artifact in getattr(self, "_input_artifacts", {}).values():
            artifact.consumers.remove(self)
        self._input_artifacts = values
        for artifact in self._input_artifacts.values():
            artifact.consumers.add(self)

    @property
    def output_artifacts(self) -> tuple[Artifact, ...]:
        return self._output_artifacts

    @output_artifacts.setter
    def output_artifacts(self, values: tuple[Artifact, ...]) -> None:
        passed_n, expected_n = len(values), len(self.signature.return_annotation)
        if passed_n != expected_n:
            ret_str = _commas(self.signature.return_annotation)
            raise ValueError(
                f"{type(self).__name__}.to() - Expected {expected_n} arguments of ({ret_str}), but got: {values}"
            )
        for i, arg in enumerate(values):
            expected_type = self.signature.return_annotation[i]
            if not isinstance(arg, expected_type):
                raise ValueError(
                    f"{type(self).__name__}.to() - Expected the {ordinal(i+1)} argument to be {expected_type}, got {type(arg)}"
                )
        for artifact in getattr(self, "_output_artifacts", ()):
            artifact.producer = None
        self._output_artifacts = values
        for artifact in self._output_artifacts:
            artifact.producer = self

    def to(self, *args: Artifact) -> Producer:
        """ Configure the Artifacts this Producer should build.

            The arguments are matched to the `Producer.build` return signature in order.
        """
        self.output_artifacts = args
        return self

    def __iter__(self) -> Iterator[Artifact]:
        return iter(self.output_artifacts)
