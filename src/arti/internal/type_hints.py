from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable
from datetime import date, datetime
from types import GenericAlias, UnionType
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

_T = TypeVar("_T")
NoneType = cast(type, type(None))  # mypy otherwise treats type(None) as an object


def assert_not_none(v: _T | None) -> _T:
    assert v is not None
    return v


def assert_all_instances(values: Iterable[Any], *, type: type[_T]) -> Iterable[_T]:
    if not all(isinstance(v, type) for v in values):
        raise TypeError(f"Expected {type.__name__} instances")
    return values


# TODO: Determine if we can replace this logic with `beartype` or related packages.
def _check_issubclass(klass: Any, check_type: type) -> bool:
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
        return issubclass(klass, check_type_origin) and not check_type_args
    # eg: issubclass(tuple[int], tuple[int])
    if klass_origin is not None and check_type_origin is not None:
        # NOTE: Considering all container types covariant for simplicity (mypy may be more strict).
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
    # Shouldn't happen, but need to explicitly say "x is not None" to narrow mypy types.
    raise NotImplementedError("The origin conditions don't cover all cases!")


def discard_Annotated(type_: Any) -> Any:
    return get_args(type_)[0] if is_annotated_hint(type_) else type_


def get_class_type_vars(klass: type) -> tuple[type, ...]:
    """Get the bound type variables from a class

    NOTE: Only vars from the *first* Generic in the mro *with all variables bound* will be returned.
    """
    # get_origin and get_args don't work with Pydantic Generics[1], but the metadata is exposed in
    # `__pydantic_generic_metadata__`. This only works for the direct instance (eg: `MyModel[X]`),
    # but not for further subclasses (eg: `class Sub(MyModel[X])`).
    #
    # 1: https://github.com/pydantic/pydantic/issues/3559
    if args := getattr(klass, "__pydantic_generic_metadata__", {}).get("args"):
        return args  # coverage: ignore
    bases = (klass,) if is_generic_alias(klass) else klass.__orig_bases__  # type: ignore[attr-defined]
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
def get_item_from_annotated[T, D](
    annotation: Any, klass: type[T], *, kind: Literal["class"], default: D = None
) -> type[T] | D: ...


@overload
def get_item_from_annotated[T, D](
    annotation: Any, klass: type[T], *, kind: Literal["object"], default: D = None
) -> T | D: ...


def get_item_from_annotated[T, D](
    annotation: Any, klass: type[T], *, kind: Literal["class", "object"], default: D = None
) -> T | type[T] | D:
    from arti.internal.utils import one_or_none

    if not is_annotated_hint(annotation):
        return default
    _, *hints = get_args(annotation)
    checker = {"class": lenient_issubclass, "object": isinstance}[kind]
    return one_or_none(
        [hint for hint in hints if checker(hint, klass)], default=default, item_name=klass.__name__
    )


def get_annotation_from_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool | bytes | date | datetime | float | int | str):
        return type(value)
    if isinstance(value, tuple | list | set | frozenset):
        first, *tail = tuple(value)
        first_type = type(first)
        if all(isinstance(v, first_type) for v in tail):
            if isinstance(value, tuple):
                return tuple[first_type, ...]  # type: ignore[valid-type]
            return type(value)[first_type]  # type: ignore[index]
    if isinstance(value, dict):
        items = value.items()
        first_key_type, first_value_type = (type(v) for v in next(iter(items)))
        if all(
            isinstance(k, first_key_type) and isinstance(v, first_value_type) for (k, v) in items
        ):
            return dict[first_key_type, first_value_type]  # type: ignore[valid-type]
        # TODO: Implement with TypedDict to support Struct types...?
    raise NotImplementedError(f"Unable to determine type of {value}")


def lenient_issubclass[T: type | tuple[type, ...]](klass: Any, class_or_tuple: T) -> TypeGuard[T]:
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
    """Convenience wrapper around `inspect.signature`.

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
# Focusing on  3.12+ (for now)


# TODO: Update the `is_*_hint` ones to use TypeForm[1] when available.
#
# 1: https://discuss.python.org/t/typeform-spelling-for-a-type-annotation-object-at-runtime/51435


def is_annotated(type_: Any) -> bool:
    return type_ is Annotated


def is_annotated_hint(type_: Any) -> bool:
    return is_annotated(get_origin(type_))


def is_generic_alias(type_: Any) -> bool:
    from typing import _GenericAlias  # type: ignore[attr-defined]

    return isinstance(type_, _GenericAlias | GenericAlias)


def is_optional_hint(type_: Any) -> bool:
    # Optional[x] is represented as Union[x, NoneType]
    return is_union(get_origin(type_)) and NoneType in get_args(type_)


def is_union(type_: Any) -> bool:
    # `Union[int, str]` or `int | str`
    return type_ is Union or type_ is UnionType


def is_union_hint(type_: Any) -> bool:
    return is_union(get_origin(type_))
