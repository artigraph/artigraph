from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from inspect import Signature
from typing import Any, ClassVar, Union

from arti.artifacts.core import Artifact
from arti.fingerprints.core import Fingerprint
from arti.internal.type_hints import signature
from arti.internal.utils import class_name, ordinal
from arti.versions.core import SemVer, Version


def _commas(vals: Iterable[Any]) -> str:
    return ", ".join([str(v) for v in vals])


class Producer:
    """A Producer is a task that builds one or more Artifacts."""

    # User fields/methods

    key: str = class_name()
    version: Version = SemVer(0, 0, 1)

    # Relax mypy "incompatible signature" warning for subclasses - we add some stricter checking
    # with the arti.internal.mypy_plugin. Once PEP 612[1] is available in 3.10+typing_extensions[2],
    # we can make Producer generic with something like: Generic[ParamSpec(P), TypeVar("R")] and
    # possibly remove the plugin.
    #
    # 1: https://www.python.org/dev/peps/pep-0612/
    # 2: https://github.com/python/mypy/issues/8645
    build: Callable[..., Any]
    map: Callable[..., Any]

    # pylint: disable=function-redefined,no-self-use
    def build(self, **kwargs: Artifact) -> tuple[Artifact, ...]:  # type: ignore # pragma: no cover
        raise NotImplementedError(
            f"{type(self).__name__} - Producers must implement the `build` method."
        )

    # pylint: disable=function-redefined,no-self-use
    def map(self, **kwargs: Artifact) -> Any:  # type: ignore
        """Map dependencies between input and output Artifact partitions.

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
        super().__init_subclass__(**kwargs)  # type: ignore # https://github.com/python/mypy/issues/4660
        cls.signature = signature(cls.build)
        cls._validate_build_sig()
        cls._validate_map_sig()

    @classmethod
    def _validate_build_sig(cls) -> None:
        """Validate the .build method"""
        if cls.build is Producer.build:
            raise ValueError(f"{cls.__name__} - Producers must implement the `build` method.")
        # Validate the parameter definition
        for name, param in cls.signature.parameters.items():
            if param.annotation is param.empty:
                raise ValueError(f"{cls.__name__} - `{name}` must have a type hint.")
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
            # TODO: "side effect" Producers: https://github.com/replicahq/artigraph/issues/11
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
        """Validate partitioned Artifacts and the .map method"""
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
        self._validate_build_args(kwargs)
        self._input_artifacts = kwargs
        super().__init__()

    def __iter__(self) -> Iterator[Artifact]:
        ret = self.out()
        if not isinstance(ret, tuple):
            ret = (ret,)
        return iter(ret)

    def _validate_build_args(self, values: dict[str, Artifact]) -> None:
        passed_params, expected_params = set(values), set(self.signature.parameters)
        if unknown := passed_params - expected_params:
            raise ValueError(f"{type(self).__name__} - Unknown argument(s): {unknown}")
        if missing := expected_params - passed_params:
            # TODO: Support Optional input/output artifacts?
            raise ValueError(f"{type(self).__name__} - Missing argument(s): {missing}")
        for name, arg in values.items():
            expected_type = self.signature.parameters[name].annotation
            if not isinstance(arg, expected_type):
                raise ValueError(
                    f"{type(self).__name__} - `{name}` expects an instance of {expected_type}, got: {arg}"
                )

    @property
    def fingerprint(self) -> Fingerprint:
        """Return a Fingerprint of the Producer key + version.

        The input and output Artifacts are ignored as the Producer instance may be used multiple
        times to produce different output partitions. The "entropy mixing" will be performed for
        *each* output with the static Producer.fingerprint + that output's specific input
        partition dependencies.
        """
        return Fingerprint.from_string(self.key).combine(self.version.fingerprint)

    out: Callable[..., Any]

    def out(self, *outputs: Artifact) -> Union[Artifact, tuple[Artifact, ...]]:  # type: ignore
        """Configure the output Artifacts this Producer will build.

        The arguments are matched to the `Producer.build` return signature in order.
        """
        if not outputs:
            outputs = tuple(artifact() for artifact in self.signature.return_annotation)
        passed_n, expected_n = len(outputs), len(self.signature.return_annotation)
        if passed_n != expected_n:
            ret_str = _commas(self.signature.return_annotation)
            raise ValueError(
                f"{type(self).__name__}.out() - Expected {expected_n} arguments of ({ret_str}), but got: {outputs}"
            )
        for i, artifact in enumerate(outputs):
            expected_type = self.signature.return_annotation[i]
            if not isinstance(artifact, expected_type):
                raise ValueError(
                    f"{type(self).__name__}.out() - Expected the {ordinal(i+1)} argument to be {expected_type}, got {type(artifact)}"
                )
            if artifact.producer is not None:
                raise ValueError(f"{artifact} is produced by {artifact.producer}!")
            artifact.producer = self
        if len(outputs) == 1:
            return outputs[0]
        return outputs
