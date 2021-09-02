from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any, ClassVar, Literal, Optional, get_args, get_origin

from pydantic import BaseModel, Extra, root_validator, validator
from pydantic.fields import ModelField

from arti.internal.type_hints import is_union, lenient_issubclass
from arti.internal.utils import class_name


def _check_types(value: Any, type_: type) -> None:  # noqa: C901
    mismatch_error = ValueError(f"Expected an instance of {type_}, got: {value}")

    if type_ is Any:
        return
    origin = get_origin(type_)
    if origin is not None:
        args = get_args(type_)
        if origin is Annotated:
            _check_types(value, args[0])
            return
        if origin is Literal:
            _check_types(value, type(args[0]))
            return
        # NOTE: Optional[t] -> Union[t, NoneType]
        if is_union(origin):
            for subtype in args:
                try:
                    _check_types(value, subtype)
                except ValueError:
                    pass
                else:
                    return
            raise mismatch_error
        if issubclass(origin, dict):
            _check_types(value, origin)
            for k, v in value.items():
                _check_types(k, args[0])
                _check_types(v, args[1])
            return
        # Variadic tuples will be handled below
        if issubclass(origin, tuple) and ... not in args:
            _check_types(value, origin)
            if len(value) != len(args):
                raise mismatch_error
            for i, subtype in enumerate(args):
                _check_types(value[i], subtype)
            return
        for t in (tuple, list, set, frozenset, Sequence):
            if issubclass(origin, t):
                _check_types(value, origin)
                for subvalue in value:
                    _check_types(subvalue, args[0])
                return
        raise NotImplementedError(f"Missing handler for {type_} with {value}!")
    if not lenient_issubclass(type(value), type_):
        raise mismatch_error


class Model(BaseModel):
    # A model can be marked _abstract_ to prevent direct instantiation, such as when it is intended
    # as a base class for other models with arbitrary data. As the subclasses of an _abstract_ model
    # have unknown fields (varying per subclass), we don't have targets to mark abstract with
    # abc.ABC nor typing.Protocol. See [1] for more context.
    #
    # 1: https://github.com/replicahq/artigraph/pull/60#discussion_r669089086
    _abstract_: ClassVar[bool] = True
    _class_key_: ClassVar[str] = class_name()

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)  # type: ignore # https://github.com/python/mypy/issues/4660
        # Default _abstract_ to False if not set explicitly on the class. __dict__ is read-only.
        setattr(cls, "_abstract_", cls.__dict__.get("_abstract_", False))

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
        _check_types(value, type_)
        return value

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
        validate_assignment = True  # Unused with frozen, unless that is overridden in subclass.

    @classmethod
    def _pydantic_type_system_ignored_fields_hook_(cls) -> frozenset[str]:
        return frozenset()

    @classmethod
    def _pydantic_type_system_post_field_conversion_hook_(
        cls, type_: arti.types.Type, *, name: str, required: bool
    ) -> arti.types.Type:
        return type_


import arti.types  # noqa: E402 # # pylint: disable=wrong-import-position
