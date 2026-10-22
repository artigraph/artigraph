from __future__ import annotations

import inspect
import typing
from collections.abc import Callable, Iterable
from datetime import date, datetime
from types import GenericAlias, NoneType, UnionType
from typing import (
    Annotated,
    Any,
    Literal,
    TypeGuard,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)
from typing import is_typeddict as is_typeddict  # Expose along with the other `is_*` helpers below.


def _runtime_type(value: Any) -> type[Any]:
    return cast(type[Any], type(value))


def assert_not_none[T](v: T | None) -> T:
    assert v is not None
    return v


# TODO: Determine if we can replace this logic with `beartype` or related packages.
def _check_issubclass(klass: Any, check_type: Any) -> bool:
    # If a hint is Annotated, we want to unwrap the underlying type and discard the rest of the
    # metadata.
    klass = discard_Annotated(klass)
    klass_origin, klass_args = get_origin(klass), get_args(klass)
    if isinstance(klass, TypeVar):
        klass = cast(type, Any) if klass.__bound__ is None else klass.__bound__
        klass_origin, klass_args = get_origin(klass), get_args(klass)
    check_type_origin, check_type_args = get_origin(check_type), get_args(check_type)
    if check_type_origin is Annotated:
        check_type = check_type_args[0]
        check_type_origin, check_type_args = get_origin(check_type), get_args(check_type)
    if isinstance(check_type, TypeVar):
        check_type = cast(type, Any) if check_type.__bound__ is None else check_type.__bound__
        check_type_origin, check_type_args = get_origin(check_type), get_args(check_type)
    if is_typeddict(klass):
        if is_typeddict(check_type):
            klass_hints = get_type_hints(klass)
            check_type_hints = get_type_hints(check_type)
            return check_type_hints.keys() <= klass_hints.keys() and all(
                lenient_issubclass(klass_hints[name], field_type)
                for name, field_type in check_type_hints.items()
            )
        klass = dict
        klass_origin, klass_args = None, ()
    elif is_typeddict(check_type):
        return False
    if klass is Any:
        return check_type is Any
    if check_type is Any:
        return True
    if check_type is None:
        return klass is NoneType
    # eg: issubclass(tuple, tuple)
    if klass_origin is None and check_type_origin is None:
        return issubclass(klass, check_type)
    # eg: issubclass(tuple[int], tuple)
    if klass_origin is not None and check_type_origin is None:
        return issubclass(klass_origin, check_type)
    # eg: issubclass(tuple, tuple[int])
    if klass_origin is None and check_type_origin is not None:
        return issubclass(klass, check_type_origin) and (
            not check_type_args or all(arg is Any for arg in check_type_args)
        )
    # eg: issubclass(tuple[int], tuple[int])
    if klass_origin is not None and check_type_origin is not None:
        # Homogeneous tuples (eg: `tuple[int, ...]`) hold any number of their element type.
        if check_type_origin is tuple and check_type_args[1:] == (...,):
            return lenient_issubclass(klass_origin, tuple) and all(
                lenient_issubclass(klass_arg, check_type_args[0])
                for klass_arg in klass_args
                if klass_arg is not ...
            )
        if (
            lenient_issubclass(klass_origin, tuple)
            and not lenient_issubclass(check_type_origin, tuple)
            and klass_args[1:] == (...,)
        ):
            klass_args = klass_args[:1]  # eg: `tuple[int, ...]` is a `Sequence[int]`
        # NOTE: Considering all container types covariant for simplicity.
        #
        # The builtin mutable containers (list, dict, etc) are invariant (klass_args ==
        # check_type_args), but the interfaces (Mapping, Sequence, etc) and immutable containers are
        # covariant.
        if check_type_args and not (
            len(klass_args) == len(check_type_args)
            and all(
                # check subclass OR things like "..."
                lenient_issubclass(klass_arg, check_type_arg) or klass_arg is check_type_arg
                for (klass_arg, check_type_arg) in zip(klass_args, check_type_args, strict=False)
            )
        ):
            return False
        return lenient_issubclass(klass_origin, check_type_origin)
    # Shouldn't happen because the origin conditions cover all cases.
    raise NotImplementedError("The origin conditions don't cover all cases!")


def discard_Annotated(type_: Any) -> Any:
    return get_args(type_)[0] if is_annotated_hint(type_) else type_


def discard_Optional(type_: Any) -> Any:
    if not is_optional_hint(type_):
        return type_
    return Union[tuple(subtype for subtype in get_args(type_) if subtype is not NoneType)]  # noqa: UP007


def get_class_type_vars(klass: type) -> tuple[type, ...]:
    """
    Get the bound type variables from a class.

    NOTE: Only vars from the *first* Generic in the mro *with all variables bound* will be returned.
    """
    # get_origin and get_args don't work with Pydantic Generics[1], but the metadata is exposed in
    # `__pydantic_generic_metadata__`. This only works for the direct instance (eg: `MyModel[X]`),
    # but not for further subclasses (eg: `class Sub(MyModel[X])`).
    #
    # 1: https://github.com/pydantic/pydantic/issues/3559
    if args := getattr(klass, "__pydantic_generic_metadata__", {}).get("args"):
        return cast(tuple[type, ...], args)  # coverage: ignore
    bases = (
        (klass,)
        if is_generic_alias(klass)
        else cast(
            tuple[type, ...],
            getattr(klass, "__orig_bases__"),  # noqa: B009
        )
    )
    for base in bases:
        base_origin = get_origin(base)
        if base_origin is None:
            continue  # coverage: ignore
        args = get_args(base)
        if any(isinstance(arg, TypeVar) for arg in args):
            continue
        return args
    raise TypeError(f"{klass.__name__} must subclass a subscripted Generic")


@overload
def get_item_from_annotated[T](
    annotation: Any, klass: type[T], *, kind: Literal["class"], default: None = None
) -> type[T] | None: ...


@overload
def get_item_from_annotated[T, D](
    annotation: Any, klass: type[T], *, kind: Literal["class"], default: D
) -> type[T] | D: ...


@overload
def get_item_from_annotated[T](
    annotation: Any, klass: type[T], *, kind: Literal["object"], default: None = None
) -> T | None: ...


@overload
def get_item_from_annotated[T, D](
    annotation: Any, klass: type[T], *, kind: Literal["object"], default: D
) -> T | D: ...


def get_item_from_annotated(
    annotation: Any,
    klass: type[Any],
    *,
    kind: Literal["class", "object"],
    default: Any = None,
) -> Any:
    from arti.internal.utils import one_or_none

    if not is_annotated_hint(annotation):
        return default
    _, *hints = get_args(annotation)
    matches = (
        [hint for hint in hints if lenient_issubclass(hint, klass)]
        if kind == "class"
        else [hint for hint in hints if isinstance(hint, klass)]
    )
    return one_or_none(matches, default=default, item_name=klass.__name__)


def get_annotation_from_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool | bytes | date | datetime | float | int | str):
        return type(value)
    if isinstance(value, tuple | list | set | frozenset):
        values = tuple(cast(Iterable[Any], value))
        first, *tail = values
        first_type = _runtime_type(first)
        if all(isinstance(v, first_type) for v in tail):
            if isinstance(value, tuple):
                return tuple[first_type, ...]
            return GenericAlias(_runtime_type(value), (first_type,))
    if isinstance(value, dict):
        items = cast(dict[Any, Any], value).items()
        first_key, first_value = next(iter(items))
        first_key_type = _runtime_type(first_key)
        first_value_type = _runtime_type(first_value)
        if all(
            isinstance(k, first_key_type) and isinstance(v, first_value_type) for (k, v) in items
        ):
            return dict[first_key_type, first_value_type]
        # TODO: Implement with TypedDict to support Struct types...?
    raise NotImplementedError(f"Unable to determine type of {value}")


def lenient_issubclass[T: type | tuple[type, ...]](klass: Any, class_or_tuple: T) -> TypeGuard[T]:
    # A Union is a subclass if all of its members are (eg: `int | None` of `int | str | None`).
    if is_union_hint(klass):
        return all(lenient_issubclass(subtype, class_or_tuple) for subtype in get_args(klass))
    if not (
        isinstance(klass, type | GenericAlias | TypeVar) or is_annotated_hint(klass) or klass is Any
    ):
        return False
    if isinstance(class_or_tuple, tuple):
        return any(lenient_issubclass(klass, subtype) for subtype in class_or_tuple)
    check_type = class_or_tuple
    # NOTE: py 3.10 supports issubclass with Unions (eg: `issubclass(str, str | int)`)
    if is_union_hint(check_type):
        return any(lenient_issubclass(klass, subtype) for subtype in get_args(check_type))
    return _check_issubclass(klass, check_type)


def _tidy_return(return_annotation: Any, *, force_tuple_return: bool) -> Any:
    if not force_tuple_return:
        return return_annotation
    if lenient_issubclass(get_origin(return_annotation), tuple):
        return get_args(return_annotation)
    return (return_annotation,)


def tidy_signature(
    fn: Callable[..., Any],
    sig: inspect.Signature,
    *,
    force_tuple_return: bool = False,
    remove_owner: bool = False,
) -> inspect.Signature:
    type_hints = get_type_hints(fn, include_extras=True)
    sig = sig.replace(return_annotation=type_hints.get("return", sig.return_annotation))
    return sig.replace(
        parameters=[
            p.replace(annotation=type_hints.get(p.name, p.annotation))
            for p in sig.parameters.values()
            if (p.name not in ("cls", "self") if remove_owner else True)
        ],
        return_annotation=(
            sig.empty
            if sig.return_annotation is sig.empty
            else _tidy_return(sig.return_annotation, force_tuple_return=force_tuple_return)
        ),
    )


def signature(
    fn: Callable[..., Any],
    *,
    follow_wrapped: bool = True,
    force_tuple_return: bool = False,
    remove_owner: bool = True,
) -> inspect.Signature:
    """
    Wrap `inspect.signature` with Artigraph's annotation normalization.

    The returned Signature will have `cls`/`self` parameters removed if `remove_owner` is `True` and
    `tuple[...]` converted to `tuple(...)` in the `return_annotation`.
    """
    return tidy_signature(
        fn=fn,
        sig=inspect.signature(fn, follow_wrapped=follow_wrapped),
        force_tuple_return=force_tuple_return,
        remove_owner=remove_owner,
    )


#############################################
# Helpers for typing across python versions #
#############################################
#
# Focusing on 3.13+ (for now)


# TODO: Update the `is_*_hint` ones to use TypeForm[1] when available.
#
# 1: https://discuss.python.org/t/typeform-spelling-for-a-type-annotation-object-at-runtime/51435


def is_annotated(type_: Any) -> bool:
    return type_ is Annotated


def is_annotated_hint(type_: Any) -> bool:
    return is_annotated(get_origin(type_))


def is_generic_alias(type_: Any) -> bool:
    generic_alias_type = cast(type, getattr(typing, "_GenericAlias"))  # noqa: B009
    return isinstance(type_, generic_alias_type | GenericAlias)


def is_optional_hint(type_: Any) -> bool:
    # Optional[x] is represented as Union[x, NoneType]
    return is_union(get_origin(type_)) and NoneType in get_args(type_)


def is_union(type_: Any) -> bool:
    # `Union[int, str]` or `int | str`
    return type_ is Union or type_ is UnionType


def is_union_hint(type_: Any) -> bool:
    return is_union(get_origin(type_))
