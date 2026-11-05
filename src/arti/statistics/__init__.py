"""
Statistics are computed "facts" about an Artifact partition, such as a row count or minimum value.

A Statistic describes *what* to measure and the Type of the measured value. Most Statistics are
measured from the data: implementations describe *how* to measure them for a specific in-memory View
(eg: a list of dicts or a pandas DataFrame) and are registered with `register_implementation`, much
like `arti.io.register_reader`. A `DerivedStatistic` is instead computed from other Statistics'
values (eg: a `Ratio` of null values to rows), without reading the data.

Statistic values are immutable facts about a partition's contents. Judgements about the values, such
as acceptable bounds, belong in `arti.thresholds`.
"""

from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from abc import abstractmethod
from collections.abc import Callable, Iterable, Mapping
from types import ModuleType
from typing import TYPE_CHECKING, Any, NamedTuple

from pydantic import TypeAdapter, ValidationError

from arti.internal.models import Model
from arti.internal.type_hints import signature
from arti.internal.utils import import_submodules, lookup_most_specific, register
from arti.types import (
    Date,
    Float64,
    Int64,
    List,
    Map,
    Set,
    String,
    Struct,
    Timestamp,
    Type,
    _Numeric,  # pyright: ignore[reportPrivateUsage]
)

if TYPE_CHECKING:
    # NOTE: arti.views imports arti.artifacts, which imports this module.
    from arti.views import View


class Statistic(Model):
    """
    A Statistic is a computed fact about the data in an Artifact partition.

    Subclasses define `value_type`, which also validates that the Statistic applies to an Artifact's
    Type (eg: a referenced field exists). Implementations for each View are registered separately
    with `register_implementation`, unless the Statistic is a `DerivedStatistic`.
    """

    _abstract_ = True

    @abstractmethod
    def value_type(self, artifact_type: Type) -> Type:
        """
        Return the Type of this Statistic's value for an Artifact of `artifact_type`.

        Raise a ValueError if the Statistic cannot be computed for `artifact_type`, including when
        its values can't be represented in Python.
        """
        raise NotImplementedError()


class DerivedStatistic(Statistic):
    """A DerivedStatistic is computed from the values of other Statistics, not from the data."""

    _abstract_ = True

    @property
    @abstractmethod
    def inputs(self) -> tuple[Statistic, ...]:
        """The Statistics this one is derived from."""
        raise NotImplementedError()

    @abstractmethod
    def derive(self, values: Mapping[Statistic, Any]) -> Any:
        """Derive the value from the `inputs`' `values`."""
        raise NotImplementedError()


def expand(statistics: Iterable[Statistic]) -> tuple[Statistic, ...]:
    """Return the `statistics` and (recursively) their inputs, with inputs first and no duplicates."""
    expanded = dict[Statistic, None]()
    for statistic in statistics:
        if isinstance(statistic, DerivedStatistic):
            expanded.update(dict.fromkeys(expand(statistic.inputs)))
        expanded[statistic] = None
    return tuple(expanded)


def _struct_fields(statistic: Statistic, artifact_type: Type) -> dict[str, Type]:
    if not (isinstance(artifact_type, List) and isinstance(artifact_type.element, Struct)):
        raise ValueError(
            f"{statistic!r} requires a Collection (or List of Structs) Type, got {artifact_type!r}"
        )
    return dict(artifact_type.element.fields)


class RowCount(Statistic):
    """The number of rows (or elements)."""

    def value_type(self, artifact_type: Type) -> Type:
        if not isinstance(artifact_type, List):
            raise ValueError(f"{self!r} requires a List or Collection Type, got {artifact_type!r}")
        return Int64()


class FieldStatistic(Statistic):
    """A FieldStatistic is a Statistic of one field (ie: column) in a Collection."""

    _abstract_ = True

    field: str

    def field_type(self, artifact_type: Type) -> Type:
        fields = _struct_fields(self, artifact_type)
        if self.field not in fields:
            raise ValueError(f"{self!r} references an unknown field, expected one of {set(fields)}")
        return fields[self.field]

    def scalar_field_type(self, artifact_type: Type) -> Type:
        if isinstance(field_type := self.field_type(artifact_type), List | Map | Set | Struct):
            raise ValueError(f"{self!r} requires a scalar field, got {field_type!r}")
        return field_type


class NullCount(FieldStatistic):
    """The number of null values in the field."""

    def value_type(self, artifact_type: Type) -> Type:
        self.field_type(artifact_type)
        return Int64()


class DistinctCount(FieldStatistic):
    """The number of distinct, non-null values in the field."""

    def value_type(self, artifact_type: Type) -> Type:
        self.scalar_field_type(artifact_type)
        return Int64()


class DistinctValues(FieldStatistic):
    """
    The set of distinct, non-null values in the field.

    The whole set is recorded, so prefer `DistinctCount` for fields with many distinct values.
    """

    def value_type(self, artifact_type: Type) -> Type:
        value_type = Set(element=self.scalar_field_type(artifact_type))
        _python_type(value_type)  # Check the values can be represented
        return value_type


_ORDERED_TYPES = (_Numeric, Date, String, Timestamp)


class _Extreme(FieldStatistic):
    _abstract_ = True

    def value_type(self, artifact_type: Type) -> Type:
        field_type = self.field_type(artifact_type)
        if not isinstance(field_type, _ORDERED_TYPES):
            raise ValueError(f"{self!r} requires an ordered field, got {field_type!r}")
        _python_type(field_type)  # Check the values can be represented
        # The value is null when the partition is empty or the field is entirely null.
        return field_type.model_copy(update={"nullable": True})


class Max(_Extreme):
    """The maximum non-null value of the field, or null if there are none."""


class Min(_Extreme):
    """The minimum non-null value of the field, or null if there are none."""


class Ratio(DerivedStatistic):
    """
    The ratio of two numeric Statistics, such as the fraction of null values in a field.

    The value is null if either input is null or the `denominator` is zero.
    """

    numerator: Statistic
    denominator: Statistic

    @property
    def inputs(self) -> tuple[Statistic, ...]:
        return (self.numerator, self.denominator)

    def value_type(self, artifact_type: Type) -> Type:
        for statistic in self.inputs:
            if not isinstance(input_type := statistic.value_type(artifact_type), _Numeric):
                raise ValueError(f"{self!r} requires numeric inputs, got {input_type!r}")
        return Float64(nullable=True)

    def derive(self, values: Mapping[Statistic, Any]) -> float | None:
        numerator, denominator = values[self.numerator], values[self.denominator]
        if numerator is None or denominator is None or denominator == 0:
            return None
        return float(numerator / denominator)


######################
# Value (de)encoding #
######################


def _python_type(type_: Type) -> Any:
    from arti.types.python import python_type_system

    try:
        return python_type_system.to_system(type_.model_copy(update={"nullable": False}), hints={})
    except NotImplementedError as error:
        raise ValueError(f"Statistic values must have a Python type: {error}") from error


def _value_adapter(type_: Type) -> TypeAdapter[Any]:
    python_type = _python_type(type_)
    return TypeAdapter[Any](python_type | None if type_.nullable else python_type)


def validate_value(value: Any, type_: Type) -> Any:
    """
    Validate that `value` is a plain Python value of `type_`, returning it or raising a TypeError.

    Validation is strict, so eg: `True` isn't an `Int64` and `numpy.int64(1)` isn't an `int`. Ints
    are accepted (and converted) for floats.
    """
    try:
        return _value_adapter(type_).validate_python(value, strict=True)
    except ValidationError as error:
        raise TypeError(f"invalid {type_!r} value {value!r}: {error}") from error


def encode_value(value: Any, type_: Type) -> Any:
    """Encode a Statistic value of `type_` into a JSON compatible value."""
    return _value_adapter(type_).dump_python(validate_value(value, type_), mode="json")


def decode_value(value: Any, type_: Type) -> Any:
    """Decode a Statistic value of `type_` from `encode_value`."""
    return _value_adapter(type_).validate_python(value)


###################
# Implementations #
###################


class ImplementationKey(NamedTuple):
    """The (Statistic, View) classes a Statistic implementation handles."""

    statistic: type[Statistic]
    view: type[View]

    def __str__(self) -> str:
        return f"statistic={self.statistic.__name__}, view={self.view.__name__}"


_implementations: dict[ImplementationKey, Callable[..., Any]] = {}
_submodules: dict[str, ModuleType] | None = None


def _discover() -> None:
    global _submodules
    if _submodules is None:
        _submodules = import_submodules(__path__, __name__)


def register_implementation[F: Callable[..., Any]](func: F) -> F:
    """
    Register a function computing a Statistic for its annotated Statistic and View classes.

    The function must have `statistic`, `data`, and `view` parameters, with `statistic` and `view`
    annotated with the most general classes it supports. It must return a plain Python value
    matching the Statistic's `value_type` (eg: `int`, not `numpy.int64`), see `validate_value`.
    """
    from arti.views import View

    params = ("statistic", "data", "view")
    sig = signature(func, remove_owner=False)
    if tuple(sig.parameters) != params or any(
        param.kind is not param.POSITIONAL_OR_KEYWORD for param in sig.parameters.values()
    ):
        raise TypeError(f"Expected `{func.__name__}` to have positional {list(params)} parameters")
    hints = {name: param.annotation for name, param in sig.parameters.items()}
    for name, base in (("statistic", Statistic), ("view", View)):
        if not (isinstance(hints[name], type) and issubclass(hints[name], base)):
            raise TypeError(
                f"Expected the `{func.__name__}.{name}` parameter to be a subclass of {base}, got {hints[name]}"
            )
    register(_implementations, ImplementationKey(hints["statistic"], hints["view"]), func)
    return func


def lookup_implementation(key: ImplementationKey) -> Callable[..., Any]:
    """Return the most specific implementation for `key`, raising if none or several match."""
    _discover()
    return lookup_most_specific(_implementations, key, name="Statistic")


def check_implementations(statistics: Iterable[Statistic], view: View) -> None:
    """Check that every Statistic measured from data (not derived) is implemented for the View."""
    for statistic in expand(statistics):
        if not isinstance(statistic, DerivedStatistic):
            lookup_implementation(ImplementationKey(type(statistic), type(view)))


def compute(statistic: Statistic, data: Any, view: View) -> Any:
    """
    Compute the `statistic` of in-memory `data`, as represented by `view`.

    The value is validated against the Statistic's `value_type` (for the View's Type).
    """
    if isinstance(statistic, DerivedStatistic):
        raise TypeError(f"{statistic!r} is derived from other Statistics, not computed from data")
    value_type = statistic.value_type(view.type)
    implementation = lookup_implementation(ImplementationKey(type(statistic), type(view)))
    value = implementation(statistic, data, view)
    try:
        return validate_value(value, value_type)
    except TypeError as error:
        raise TypeError(f"{implementation.__name__} returned an invalid value: {error}") from error


def compute_missing(
    statistics: Iterable[Statistic],
    *,
    artifact_type: Type,
    known: Mapping[Statistic, Any],
    load: Callable[[], tuple[Any, View]],
) -> dict[Statistic, Any]:
    """
    Compute the values of the `statistics` (and their inputs) that aren't `known`.

    `load` returns the data and its View, and is only called (once) if a Statistic must be computed
    from the data. Derived Statistics are computed from their inputs' values, so they never need the
    data if their inputs are known.
    """
    values = dict(known)
    computed = dict[Statistic, Any]()
    loaded: tuple[Any, View] | None = None
    for statistic in expand(statistics):
        if statistic in values:
            continue
        if isinstance(statistic, DerivedStatistic):
            value = validate_value(
                statistic.derive({input: values[input] for input in statistic.inputs}),
                statistic.value_type(artifact_type),
            )
        else:
            if loaded is None:
                loaded = load()
            value = compute(statistic, *loaded)
        values[statistic] = computed[statistic] = value
    return computed


__all__ = [
    "DerivedStatistic",
    "DistinctCount",
    "DistinctValues",
    "FieldStatistic",
    "ImplementationKey",
    "Max",
    "Min",
    "NullCount",
    "Ratio",
    "RowCount",
    "Statistic",
    "check_implementations",
    "compute",
    "compute_missing",
    "decode_value",
    "encode_value",
    "expand",
    "lookup_implementation",
    "register_implementation",
    "validate_value",
]
