from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from types import GenericAlias
from typing import Annotated, Any, ClassVar, Literal, assert_never, cast

from pydantic import AfterValidator, GetCoreSchemaHandler, RootModel, WrapSerializer
from pydantic_core import CoreSchema, core_schema

from arti.internal.type_hints import get_class_type_vars
from arti.internal.utils import get_module_name


class frozendict[K, V](Mapping[K, V]):
    def __init__(self, arg: Mapping[K, V] | Iterable[tuple[K, V]] = (), **kwargs: V) -> None:
        self._data = dict[K, V](arg, **kwargs)
        # Eagerly evaluate the hash to confirm elements are also frozen (via frozenset) at
        # creation time, not just when hashed.
        self._hash = hash(frozenset(self._data.items()))

    def __getitem__(self, key: K) -> V:
        return self._data[key]

    def __hash__(self) -> int:
        return self._hash

    def __iter__(self) -> Iterator[K]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __or__(self, other: Mapping[K, V]) -> frozendict[K, V]:
        return type(self)({**self, **other})

    __ror__ = __or__

    def __repr__(self) -> str:
        return repr(self._data)

    @classmethod
    def _get_functional_schema(cls) -> Any:
        return [
            AfterValidator(cls),
            WrapSerializer(
                lambda value, handler: handler(dict(value)),
                return_type=Mapping[K, V],
                when_used="unless-none",
            ),
        ]

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        # NOTE: This is only used when a frozendict (or subclass) is directly annotated as the type
        # hint on a model field. If the type hint is FrozenMapping (which allows input from
        # arbitrary mappings types), this is ignored and it instead uses FrozenMapping's Annotated
        # metadata.
        K, V = cast(tuple[Any, Any], get_class_type_vars(source_type))
        return handler(Annotated[Mapping[K, V], *cls._get_functional_schema()])


type FrozenMapping[K, V] = Annotated[Mapping[K, V], *frozendict._get_functional_schema()]


# TODO: This is janky, do something better... The important part is this object is shared across
# TypedBox sub-nodes, so we want to be able to mutate just this object to affect all.
class BoxStatus(RootModel[Literal["open", "closed"]]):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.root == other
        return super().__eq__(other)


type TypedNode[V] = V | TypedBox[V]
# Since we cast the values to the correct type (or error), we accept Any here. This relaxes Pydantic
# and type checkers to allow passing in values we expect to be able to cast.
type InputElement = Any | InputContainer
type InputContainer = Iterable[tuple[str, InputElement]] | Mapping[str, InputElement]


class InvalidKeyError(AttributeError):
    """Error raised when a TypedBox key is invalid.

    This does not subclass KeyError - the key is not "missing", but invalid.
    """

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Invalid key: {self.key}")


class TypedBox[V](Mapping[str, TypedNode[V]]):
    """TypedBox holds a collection of typed values.

    Subclasses must set the __target_type__ to a base class for the contained values.
    """

    __target_type__: ClassVar[type[V]]  # pyright: ignore[reportGeneralTypeIssues]

    @classmethod
    def __class_getitem__(cls, item: type[V]) -> GenericAlias:
        if isinstance(item, tuple):
            raise TypeError(f"{cls.__name__} expects a single value type")
        value_type = item
        members = {
            "__module__": get_module_name(depth=2),  # Set to our caller's module
            "__target_type__": value_type,
        }
        return GenericAlias(type(cls.__name__, (cls,), members), item)

    def __init__(
        self,
        arg: InputContainer = (),
        *,
        _namespace: tuple[str, ...] = (),
        _status: BoxStatus | None = None,
        **kwargs: InputElement,
    ) -> None:
        super().__setattr__("_init_", ...)  # Allow __setattr__ to set private attributes
        self._namespace = _namespace
        # Storing status configuration in a dict that we can share (and update) across nested boxes.
        self._status = BoxStatus("open") if _status is None else _status
        self._data: MutableMapping[str, TypedNode[V]] = {}
        super().__delattr__("_init_")
        # Populate only after all other fields are set to ensure any nested conversions can occur
        # with full context.
        for k, v in dict(arg, **kwargs).items():
            self[k] = v

    def _cast_value(self, key: str, value: Any) -> V:
        if isinstance(value, self.__target_type__):
            return value
        tgt_name = self.__target_type__.__name__
        if hasattr(self.__target_type__, "cast"):
            casted = cast(Any, self.__target_type__).cast(value)
            if isinstance(casted, self.__target_type__):
                return casted
            raise TypeError(
                f"Expected {tgt_name}.cast({value}) to return an instance of {tgt_name}, got: {casted}"
            )
        raise TypeError(f"Expected an instance of {tgt_name}, got: {value}")

    def _cast(self, key: str, value: Any) -> TypedNode[V]:
        if isinstance(value, Mapping):
            return type(self)(value, _namespace=(*self._namespace, key), _status=self._status)
        return self._cast_value(key, value)

    def _get_leaf_and_key(
        self, key: str, short_circuit_missing: bool = False
    ) -> tuple[TypedBox[V], str]:
        *head, tail = key.split(".")
        for part in head:
            if short_circuit_missing and part not in self:
                return self, part
            self = cast(TypedBox[V], self[part])  # NOTE: reassigning self
        if tail.startswith("_"):
            raise InvalidKeyError(key)
        return self, tail

    def __contains__(self, key: object) -> bool:  # Mapping defines key as object here
        # Override __contains__ to prevent lookups adding to the box via __getitem__.
        key = cast(str, key)
        self, key = self._get_leaf_and_key(
            key, short_circuit_missing=True
        )  # NOTE: reassigning self
        return key in self._data

    def __getitem__(self, key: str) -> TypedNode[V]:
        self, key = self._get_leaf_and_key(key)  # NOTE: reassigning self
        if key not in self._data and self._status == "open":
            self[key] = {}
        return self._data[key]

    def __getattr__(self, key: str) -> TypedNode[V]:
        if key.startswith("_"):
            try:
                return super().__getattribute__(key)
            except AttributeError as e:
                raise InvalidKeyError(key) from e.__cause__
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(
                f"type object '{type(self).__name__}' has no attribute {e.args[0]}"
            ) from e

    def __setitem__(self, key: str, value: Any) -> None:
        self, key = self._get_leaf_and_key(key)  # NOTE: reassigning self
        if self._status == "closed":
            raise ValueError(f"{type(self).__name__} is frozen.")
        if key in self._data:
            existing = self._data[key]
            # If the existing and new value are mappings, we want to merge.
            if isinstance(value, Mapping) and isinstance(existing, TypedBox):
                for k, v in value.items():
                    existing[k] = v
                return
            raise ValueError(f"{key} is already set")
        self._data[key] = self._cast(key, value)

    def __setattr__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            # Prevent setting unknown attributes outside of __init__.
            if not hasattr(self, "_init_") and not hasattr(self, key):
                raise InvalidKeyError(key)
            super().__setattr__(key, value)
            return
        self[key] = value

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return repr(self._data)

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __hash__(self) -> int:
        match self._status.root:
            case "closed":
                return hash(frozenset(self._data.items()))  # May error if not
            case "open":
                raise ValueError(f"{type(self).__name__} is still open and cannot be hashed.")
            case _:  # pragma: no cover
                assert_never(self._status.root)

    def walk(self, root: tuple[str, ...] = ()) -> Iterator[tuple[str, V]]:
        for k, v in self.items():
            subroot = (*root, k)
            if isinstance(v, self.__target_type__):
                yield ".".join(subroot), v
            else:
                assert isinstance(v, TypedBox)
                yield from v.walk(root=subroot)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        assert get_class_type_vars(source_type) == (cls.__target_type__,)
        # TODO: Narrow the input schema - ideally we could use InputContainer, but Pydantic doesn't
        # seem to resolve the recursive aliases. Instead, we'll just mark it as Any since the class
        # will validate and cast on its own.
        input_schema = handler.generate_schema(Any)
        serializer_schema = core_schema.plain_serializer_function_ser_schema(
            lambda b: dict(sorted(b.walk())),
            return_schema=handler.generate_schema(Mapping[str, cls.__target_type__]),
        )
        return core_schema.no_info_after_validator_function(
            cls, input_schema, serialization=serializer_schema
        )
