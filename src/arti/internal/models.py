from __future__ import annotations

from collections.abc import Iterable, Mapping
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    ClassVar,
    Self,
    cast,
    dataclass_transform,
    overload,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    PrivateAttr,
    SerializerFunctionWrapHandler,
    computed_field,
    model_serializer,
)
from pydantic_core import PydanticUndefined, PydanticUndefinedType

from arti.fingerprints import Fingerprint, SkipFingerprint
from arti.internal.type_hints import get_item_from_annotated
from arti.internal.utils import ClassProperty, class_name

if TYPE_CHECKING:
    from pydantic._internal._model_construction import ModelMetaclass as _PydanticModelMeta
else:
    _PydanticModelMeta = type(BaseModel)


# Create a `Model` base class that can be used for (most of) our internal classes. Notably, our
# models:
# - are frozen by default, which encourages (not guarantees!) more functional / testable code.
# - perform strict runtime type checking, which helps provide early call-site feedback for users.
#
# Pydantic models are not frozen by default, but we override `Model` (and thus subclasses) to be
# frozen with the `model_config`. However, type checkers do not understand `model_config` and
# instead infer hints from the `dataclass_transform` set on Pydantic's  metaclass. We can't just
# mark `Model` as frozen as subclass would continue getting the metaclass's default
# `dataclass_transform` hints. Instead, we need to "replace" the metaclass with one that has the
# correct defaults (or at least trick type checkers that we have). Teaching type checkers that
# our models are immutable allows:
# - hashability[1], meaning they can be dict/set elements.
# - overriding field types on subclasses[2], such as providing a more specific sub-type.
#
# 1: https://github.com/microsoft/pyright/issues/6481
# 2: https://github.com/microsoft/pyright/issues/6270
#
# Unfortunately, we cannot override a single transform, so copy others from Pydantic.
@dataclass_transform(
    field_specifiers=(Field, PrivateAttr), frozen_default=True, kw_only_default=True
)
class ModelMeta(_PydanticModelMeta):
    pass


ModelTypeSerializer = PlainSerializer(
    lambda m: m._arti_type_key_, return_type=str, when_used="json-unless-none"
)


class Model(BaseModel, metaclass=ModelMeta):
    _abstract_: ClassVar[bool] = True  # Prevent instantiation; defaults to False in subclasses
    _arti_type_key_: ClassVar[str] = class_name()
    # Defaulted in __pydantic_init_subclass__
    _arti_fingerprint_fields_: ClassVar[tuple[str, ...]] = ()

    # TODO: Support looking up the correct subclass when instantiating from serialized data.
    @computed_field(repr=False)
    def _arti_type_(self) -> Annotated[type[Self], ModelTypeSerializer]:
        """
        Include the type name in the serialized output by default.

        This avoids information loss and ambiguous serialization (different model, same fields, eg:
        Int32 and Int64).
        """
        return type(self)

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
        ignored_types=(ClassProperty,),
        # Serialize nested models as their concrete class, including fields added by subclasses
        # that are not in the class used in a type hint. For example, `format: Format = JSON()`
        # should serialize with any extra parameters defined in `JSON`, not just the ones defined
        # in `Format`.
        #
        # By default, Pydantic only serializes the fields defined in the hinted class to prevent
        # accidental serialization of sensitive fields. However, we won't be storing any sensitive
        # data in the models (or at least, shouldn't be). Setting this in the config (rather than
        # per field with `SerializeAsAny[...]`) also covers standalone dumps, such as with a
        # `pydantic.TypeAdapter`.
        polymorphic_serialization=True,
    )

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        # Default _abstract_ to False if not set explicitly on the class.
        cls._abstract_ = cls.__dict__.get("_abstract_", False)
        # Determine all of the Fingerprint fields.
        if not cls.model_config.get("frozen"):  # coverage: ignore
            raise ValueError(f"{cls._arti_type_key_} must be frozen to generate a fingerprint.")
        field_annotations = (
            # Pydantic strips `Annotated[T, ...]` hints into `annotation` (T) and `metadata` (...).
            (
                name,
                (
                    Annotated[cast(Any, field_info.annotation), *field_info.metadata]
                    if field_info.metadata
                    else field_info.annotation
                ),
            )
            for name, field_info in cls.model_fields.items()
            if not field_info.exclude
        )
        computed_annotations = (
            (name, computed_info.return_type)
            for name, computed_info in cls.model_computed_fields.items()
        )
        # Sort the fields to ensure a deterministic order for fingerprinting.
        cls._arti_fingerprint_fields_ = tuple(
            sorted(
                name
                for name, annotation in [*field_annotations, *computed_annotations]
                if not get_item_from_annotated(
                    annotation, SkipFingerprint, default=SkipFingerprint(False), kind="object"
                )
            )
        )

    if not TYPE_CHECKING:

        def __new__(cls, *args, **kwargs):
            if cls._abstract_:
                raise TypeError(f"{cls._arti_type_key_} cannot be instantiated directly.")
            return super().__new__(cls)

    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        # Verify the model is hashable, ie: approximately immutable. This might be a bit expensive
        # to check for every instance, but is simpler to check here than against the field types at
        # class definition time.
        hash(self)

    @model_serializer(mode="wrap")
    def _arti_serialize_model(
        self: Any, serializer: SerializerFunctionWrapHandler
    ) -> dict[str, Any]:
        data = serializer(self)
        # Sort the serialized data to reduce variability - but still allow excluding fields.
        return {field: data[field] for field in self._fingerprint_fields() if field in data}

    def _fingerprint_fields(self) -> tuple[str, ...]:
        """Return the (sorted) fields to serialize; subclasses may narrow them by field values."""
        return self._arti_fingerprint_fields_

    def model_copy(self, *, update: Mapping[str, Any] | None = None, deep: bool = False) -> Self:
        copy = super().model_copy(update=update, deep=deep)
        if update:
            # Pydantic copies `__dict__` as is, including `cached_property` values (eg:
            # `fingerprint`) derived from the old field values. Drop everything that isn't a field
            # and rerun `model_post_init` so derived state matches the updated fields.
            state = vars(copy)
            for name in state.keys() - type(copy).model_fields.keys():
                del state[name]
            copy.model_post_init(None)
        return copy

    # Fingerprinting is somewhat expensive, so we'll cache the result. This is safe as long as the
    # model and all fields are immutable (see `model_config.frozen` and `model_post_init` above).
    #
    # NOTE: We cannot use a `@pydantic.computed_field` here because other computed fields will not
    # be populated (and persisted) yet. While we *can* access the `computed_fields` from `self`,
    # that triggers the generation code but the returned value is not what will be stored in the
    # final model. For pure functions with simple values, this would be fine but we can't guarantee
    # determinism.
    @cached_property
    def fingerprint(self) -> Fingerprint:
        fingerprint = Fingerprint.from_string(self.model_dump_json())
        # NOTE: This shouldn't happen unless we somehow stumble upon Farmhash64(...) => 0
        if fingerprint.is_identity:  # coverage: ignore
            raise ValueError("Fingerprint is empty!")
        return fingerprint

    def __repr_args__(self) -> Iterable[tuple[str | None, Any]]:
        return [(k, v) for k, v in super().__repr_args__() if k in self.model_fields_set]

    def __str__(self) -> str:
        return repr(self)


@overload
def get_field_default(
    model: type[Model], field: str, *, fallback: PydanticUndefinedType = PydanticUndefined
) -> Any: ...


@overload
def get_field_default[T](model: type[Model], field: str, *, fallback: T) -> T: ...


def get_field_default[T](
    model: type[Model], field: str, *, fallback: T | PydanticUndefinedType = PydanticUndefined
) -> Any | T:
    default = model.model_fields[field].get_default(call_default_factory=True)
    if default is PydanticUndefined:
        if fallback is not PydanticUndefined:
            return fallback
        raise ValueError(f"No default value for field '{field}' in {model}")  # coverage: ignore
    return default
