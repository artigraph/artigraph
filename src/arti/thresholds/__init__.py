"""
Thresholds are "judgements" over Statistics, used to gate the quality of Artifact partitions.

Statistics are facts that don't change for a partition's contents, but the bounds we consider
acceptable may change over time. Keeping them separate means a Threshold can be revised (and
rechecked) without recomputing Statistics or rebuilding data.
"""

from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from pydantic import field_serializer, field_validator, model_validator
from pydantic_core import to_json

from arti.internal.models import Model
from arti.statistics import Statistic, validate_value
from arti.types import Set, Type

if TYPE_CHECKING:
    from arti.storage import StoragePartitionSnapshot

# The values a Threshold may be configured with: JSON compatible scalars and dates.
type ThresholdValue = bool | int | float | str | date | datetime | None


class Threshold(Model):
    """A Threshold judges whether the value of its `statistic` is acceptable."""

    _abstract_ = True

    statistic: Statistic

    def check_type(self, value_type: Type) -> None:
        """
        Validate this Threshold's configuration against the Statistic's `value_type`.

        Raise a ValueError if the Threshold can't apply, for example, if a bound has the wrong type.
        """

    @abstractmethod
    def check(self, value: Any) -> bool:
        """Return whether the Statistic `value` is acceptable."""
        raise NotImplementedError()


def _check_config_value(threshold: Threshold, name: str, value: Any, value_type: Type) -> None:
    try:
        validate_value(value, value_type)
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"{threshold!r} `{name}` is incompatible with the Statistic: {error}"
        ) from error


class Range(Threshold):
    """
    Require the value to be within the (inclusive) `min` and `max` bounds.

    At least one bound must be set. A null value (eg: the `Max` of an empty partition) fails.
    """

    min: ThresholdValue = None
    max: ThresholdValue = None

    @model_validator(mode="after")
    def _validate_bounds(self) -> Range:
        if self.min is None and self.max is None:
            raise ValueError("at least one of `min` or `max` must be set")
        if self.min is not None and self.max is not None:
            try:
                inverted = bool(self.min > self.max)  # pyright: ignore[reportOperatorIssue, reportUnknownArgumentType]
            except TypeError as error:
                raise ValueError(f"`min` and `max` must be comparable: {error}") from error
            if inverted:
                raise ValueError(
                    f"`min` ({self.min!r}) must not be greater than `max` ({self.max!r})"
                )
        return self

    def check_type(self, value_type: Type) -> None:
        for name in ("min", "max"):
            if (bound := getattr(self, name)) is not None:
                _check_config_value(
                    self, name, bound, value_type.model_copy(update={"nullable": False})
                )

    def check(self, value: Any) -> bool:
        if value is None:
            return False
        if self.min is not None and value < self.min:
            return False
        return not (self.max is not None and value > self.max)


class Equal(Threshold):
    """Require the value to equal `value`."""

    value: ThresholdValue

    def check_type(self, value_type: Type) -> None:
        _check_config_value(self, "value", self.value, value_type)

    def check(self, value: Any) -> bool:
        return bool(value == self.value)


class Subset(Threshold):
    """
    Require a set value (eg: `DistinctValues`) to be a subset of the allowed values in `of`.

    For example, to require that a field only has known categories.
    """

    of: frozenset[ThresholdValue]

    @field_validator("of", mode="before")
    @classmethod
    def _cast_values(cls, of: Any) -> Any:
        if isinstance(of, Iterable) and not isinstance(of, str | Mapping):
            return frozenset(cast(Iterable[Any], of))
        return cast(Any, of)  # coverage: ignore

    @field_serializer("of", when_used="json")
    def _serialize_of(self, of: frozenset[Any]) -> list[Any]:
        # frozenset order follows the process' hash seed, so sort for stable fingerprints.
        return sorted(of, key=to_json)

    def check_type(self, value_type: Type) -> None:
        if not isinstance(value_type, Set):
            raise ValueError(f"{self!r} requires a Set Statistic, got {value_type!r}")
        _check_config_value(self, "of", set(self.of), value_type)

    def check(self, value: Any) -> bool:
        return bool(value <= self.of)


class ThresholdFailure(NamedTuple):
    """A partition's Statistic value that a Threshold judged unacceptable."""

    partition: StoragePartitionSnapshot
    threshold: Threshold
    value: Any

    def __str__(self) -> str:
        partition_key = dict(self.partition.partition_key) or "(not partitioned)"
        return f"{partition_key}: {self.threshold!r} got {self.value!r}"


class ThresholdError(ValueError):
    """Raised when Artifact partitions fail their Thresholds."""

    def __init__(self, artifact_key: str, failures: list[ThresholdFailure]) -> None:
        self.artifact_key = artifact_key
        self.failures = failures
        lines = "\n".join(f"- {failure}" for failure in failures)
        super().__init__(
            f"`{artifact_key}` failed {len(failures)} Threshold check(s):\n{lines}\n"
            + "Fix the data, revise the Thresholds, or accept a partition with `Graph.override_threshold`."
        )


def check_thresholds(
    thresholds: tuple[Threshold, ...],
    statistics: Mapping[StoragePartitionSnapshot, Mapping[Statistic, Any]],
) -> list[ThresholdFailure]:
    """
    Check the `thresholds` against each partition's Statistic values, returning any failures.

    Every Threshold's Statistic must have a value for every partition.
    """
    return [
        ThresholdFailure(partition, threshold, value)
        for partition, values in statistics.items()
        for threshold in thresholds
        if not threshold.check(value := values[threshold.statistic])
    ]


__all__ = [
    "Equal",
    "Range",
    "Subset",
    "Threshold",
    "ThresholdError",
    "ThresholdFailure",
    "ThresholdValue",
    "check_thresholds",
]
