import inspect
import sys
import types
from collections.abc import Callable
from typing import Any, Union, cast, get_args, get_origin, get_type_hints, no_type_check

NoneType = cast(type, type(None))  # mypy otherwise treats type(None) as an object


def _check_issubclass(klass: Any, check_type: type) -> bool:
    klass_origin, check_type_origin = get_origin(klass), get_origin(check_type)
    klass_args, check_type_args = get_args(klass), get_args(check_type)
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
        if check_type_args:
            if not (
                len(klass_args) == len(check_type_args)
                and all(
                    lenient_issubclass(klass_arg, check_type_arg)
                    for (klass_arg, check_type_arg) in zip(klass_args, check_type_args)
                )
            ):
                return False
        return lenient_issubclass(klass_origin, check_type_origin)
    # Shouldn't happen, but need to explicitly say "x is not None" to narrow mypy types.
    raise NotImplementedError("The origin conditions don't cover all cases!")


def get_class_type_vars(klass: type) -> tuple[type, ...]:
    base = klass.__orig_bases__[0]  # type: ignore
    base_origin = get_origin(base)
    if base_origin is None:
        raise TypeError(f"{klass.__name__} must subclass a subscripted Generic")
    assert lenient_issubclass(klass, base_origin)
    return get_args(base)


def lenient_issubclass(klass: Any, class_or_tuple: Union[type, tuple[type, ...]]) -> bool:
    if not isinstance(klass, type):
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
    type_hints = get_type_hints(fn)
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
# Focusing on  3.9+ (for now)


if sys.version_info < (3, 10):  # pragma: no cover

    def is_union(type_: Any) -> bool:
        return type_ is Union


else:  # pragma: no cover

    # mypy doesn't know of types.UnionType yet, but `type: ignore` would be "unused"
    # (and error) on other python versions.
    @no_type_check
    def is_union(type_: Any) -> bool:
        # `Union[int, str]` or `int | str`
        return type_ is Union or type_ is types.UnionType  # noqa: E721


def is_optional_hint(type_: Any) -> bool:
    # Optional[x] is represented as Union[x, NoneType]
    return is_union(get_origin(type_)) and NoneType in get_args(type_)


def is_union_hint(type_: Any) -> bool:
    return get_origin(type_) is Union
