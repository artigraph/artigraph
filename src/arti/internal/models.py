from collections.abc import Generator, Mapping, Sequence
from copy import deepcopy
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Literal,
    Optional,
    TypeVar,
    get_args,
    get_origin,
)

from pydantic import BaseModel, Extra, root_validator, validator
from pydantic.fields import ModelField, Undefined
from pydantic.json import pydantic_encoder as pydantic_json_encoder

from arti.internal.patches import patch_pydantic_ModelField__type_analysis
from arti.internal.type_hints import is_union, lenient_issubclass
from arti.internal.utils import class_name, classproperty, frozendict

if TYPE_CHECKING:
    from arti.fingerprints import Fingerprint
    from arti.types import Type

patch_pydantic_ModelField__type_analysis()


def _check_types(value: Any, type_: type) -> Any:  # noqa: C901
    mismatch_error = ValueError(f"expected an instance of {type_}, got: {value}")

    if type_ is Any:
        return value
    origin = get_origin(type_)
    if origin is not None:
        args = get_args(type_)
        if origin is Annotated:
            return _check_types(value, args[0])
        if origin is Literal:
            return _check_types(value, type(args[0]))
        # NOTE: Optional[t] -> Union[t, NoneType]
        if is_union(origin):
            for subtype in args:
                try:
                    return _check_types(value, subtype)
                except ValueError:
                    pass
            raise mismatch_error
        if issubclass(origin, (dict, Mapping)):
            value = _check_types(value, origin)
            for k, v in value.items():
                _check_types(k, args[0])
                _check_types(v, args[1])
            return value
        # Variadic tuples will be handled below
        if issubclass(origin, tuple) and ... not in args:
            value = _check_types(value, origin)
            if len(value) != len(args):
                raise mismatch_error
            for i, subtype in enumerate(args):
                _check_types(value[i], subtype)
            return value
        for t in (tuple, list, set, frozenset, Sequence):
            if issubclass(origin, t):
                value = _check_types(value, origin)
                for subvalue in value:
                    _check_types(subvalue, args[0])
                return value
        if issubclass(origin, type):
            if not lenient_issubclass(value, args[0]):
                raise ValueError(f"expected a subclass of {args[0]}, got: {value}")
            return value
        if set(args) == {Any}:
            return _check_types(value, origin)
        raise NotImplementedError(f"Missing handler for {type_} with {value}!")
    if isinstance(value, Mapping) and not isinstance(value, frozendict):
        value = frozendict(value)
    if not lenient_issubclass(type(value), type_):
        raise mismatch_error
    return value


_Model = TypeVar("_Model", bound="Model")


class Model(BaseModel):
    # A model can be marked _abstract_ to prevent direct instantiation, such as when it is intended
    # as a base class for other models with arbitrary data. As the subclasses of an _abstract_ model
    # have unknown fields (varying per subclass), we don't have targets to mark abstract with
    # abc.ABC nor typing.Protocol. See [1] for more context.
    #
    # 1: https://github.com/artigraph/artigraph/pull/60#discussion_r669089086
    _abstract_: ClassVar[bool] = True
    _class_key_: ClassVar[str] = class_name()
    _fingerprint_excludes_: ClassVar[Optional[frozenset[str]]] = None
    _fingerprint_includes_: ClassVar[Optional[frozenset[str]]] = None

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Default _abstract_ to False if not set explicitly on the class. __dict__ is read-only.
        setattr(cls, "_abstract_", cls.__dict__.get("_abstract_", False))
        field_names = set(cls.__fields__)
        if cls._fingerprint_excludes_ and (
            unknown_excludes := cls._fingerprint_excludes_ - field_names
        ):
            raise ValueError(f"Unknown `_fingerprint_excludes_` field(s): {unknown_excludes}")
        if cls._fingerprint_includes_ and (
            unknown_includes := cls._fingerprint_includes_ - field_names
        ):
            raise ValueError(f"Unknown `_fingerprint_includes_` field(s): {unknown_includes}")

    @root_validator(pre=True)
    @classmethod
    def _block_abstract_instance(cls, values: dict[str, Any]) -> dict[str, Any]:
        if cls._abstract_:
            raise ValueError(f"{cls} cannot be instantiated directly!")
        return values

    @validator("*", pre=True)
    @classmethod
    def _strict_types(cls, value: Any, field: ModelField) -> Any:
        """Check that the value is a stricter instance of the declared type annotation.

        Pydantic will attempt to *parse* values (eg: "5" -> 5), but we'd prefer stricter values for
        clarity and to avoid silent precision loss (eg: 5.3 -> 5).

        NOTE: (at least) one edge case exists with `Union[str, float]` style annotations [1] that
        causes a `5.0` input value to be output as `"5.0"`. This can be worked around by ordering
        the types in the Union from most to least specific (eg: `Union[float, str]`). Alternatively,
        there is an `each_item=True` arg to `@validator` that would let us validate/pick individual
        Union member with `_check_types`, but this mode doesn't let us validate dictionary keys.
        Giving priority to dicts in this case, as I expect they'll be more common.

        1: https://github.com/samuelcolvin/pydantic/issues/1423
        """
        # `field.type_` points to the *inner* type (eg: `int`->`int`; `tuple[int, ...]` -> `int`)
        # while `field.outer_type_` will (mostly) include the full spec and match the `value` we
        # received. The caveat is the `field.outer_type_` will never be wrapped in `Optional`
        # (though nested fields like `tuple[tuple[Optional[int]]]` would). Hence, we pull the
        # `field.outer_type_`, but add back the `Optional` wrapping if necessary.
        type_ = field.outer_type_
        if field.allow_none:
            type_ = Optional[type_]
        return _check_types(value, type_)

    # By default, pydantic just compares models by their dict representation, causing models of
    # different types but same fields (eg: Int8 and Int16) to be equivalent. This can be removed if
    # [1] is merged+released.
    #
    # 1: https://github.com/samuelcolvin/pydantic/pull/3066
    def __eq__(self, other: Any) -> bool:
        return self.__class__ == other.__class__ and tuple(self._iter()) == tuple(other._iter())

    # Omitting unpassed args in repr by default
    def __repr_args__(self) -> Sequence[tuple[Optional[str], Any]]:
        return [(k, v) for k, v in super().__repr_args__() if k in self.__fields_set__]

    def __str__(self) -> str:
        return repr(self)

    class Config:
        extra = Extra.forbid
        frozen = True
        keep_untouched = (cached_property, classproperty)
        validate_assignment = True  # Unused with frozen, unless that is overridden in subclass.

    def copy(self: _Model, *, deep: bool = False, validate: bool = True, **kwargs: Any) -> _Model:
        copy = super().copy(deep=deep, **kwargs)
        if validate:
            # NOTE: We set exclude_unset=False so that all existing defaulted fields are reused (as
            # is normal `.copy` behavior).
            #
            # To reduce `repr` noise, we'll reset .__fields_set__ to those of the pre-validation copy
            # (which includes those originally set + updated).
            fields_set = copy.__fields_set__
            copy = copy.validate(
                dict(copy._iter(to_dict=False, by_alias=False, exclude_unset=False))
            )
            # Use object.__setattr__ to bypass frozen model assignment errors
            object.__setattr__(copy, "__fields_set__", set(fields_set))
            # Copy over the private attributes, which are missing after validation (since we're only
            # passing the fields).
            for name in self.__private_attributes__:
                if (value := getattr(self, name, Undefined)) is not Undefined:
                    if deep:
                        value = deepcopy(value)
                    object.__setattr__(copy, name, value)
        return copy

    @staticmethod
    def _fingerprint_json_encoder(obj: Any) -> Any:
        from arti.fingerprints import Fingerprint

        if isinstance(obj, Fingerprint):
            return obj.key
        if isinstance(obj, Model):
            return obj.fingerprint
        return pydantic_json_encoder(obj)

    @property
    def fingerprint(self) -> "Fingerprint":
        from arti.fingerprints import Fingerprint

        # `.json` cannot be used, even with a custom encoder, because it calls `.dict`, which
        # converts the sub-models to dicts. Instead, we want to access `.fingerprint` (in the
        # decoder).
        data = dict(
            sorted(  # Sort to ensure stability
                self._iter(
                    exclude=self._fingerprint_excludes_,
                    include=self._fingerprint_includes_,
                ),
                key=lambda kv: kv[0],
            )
        )
        json_repr = self.__config__.json_dumps(
            data,
            default=self._fingerprint_json_encoder,
        )
        return Fingerprint.from_string(f"{self._class_key_}:{json_repr}")

    # Filter out non-fields from ._iter (and thus .dict, .json, etc), such as `@cached_property`
    # after access (which just gets cached in .__dict__).
    def _iter(self, *args: Any, **kwargs: Any) -> Generator[tuple[str, Any], None, None]:
        for key, value in super()._iter(*args, **kwargs):
            if key in self.__fields__:
                yield key, value

    @classmethod
    def _pydantic_type_system_post_field_conversion_hook_(
        cls, type_: "Type", *, name: str, required: bool
    ) -> "Type":
        return type_
