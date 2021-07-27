from typing import Any, ClassVar, Literal, get_origin

from pydantic import BaseModel, Extra, root_validator, validator
from pydantic.fields import ModelField

from arti.internal.utils import class_name


def _check_types(value: Any, type_: type) -> None:
    if type_ is Any:
        return
    origin = get_origin(type_)
    if origin is not None:
        if origin is Literal:  # pragma: no cover
            return  # Let pydantic verify
        # Don't need to handle Union[...]: pydantic splits up validator
    if not isinstance(value, type_):
        raise ValueError(f"Expected an instance of {type_.__name__}, got: {value}")


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

    @validator("*", pre=True, each_item=True)
    @classmethod
    def _strict_types(cls, v: Any, field: ModelField) -> Any:
        _check_types(v, field.type_)
        return v

    def __str__(self) -> str:
        return repr(self)

    class Config:
        extra = Extra.forbid
        frozen = True
        validate_assignment = True  # Unused with frozen, unless that is overridden in subclass.
