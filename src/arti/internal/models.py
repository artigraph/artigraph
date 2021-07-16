from typing import Any, ClassVar

from pydantic import BaseModel, Extra, root_validator

from arti.internal.utils import class_name


class Model(BaseModel):
    # A model can be marked __abstract__ to prevent direct instantiation, such as when it is
    # intended as a base class for other models with arbitrary data. As the subclasses of an
    # __abstract__ model have unknown fields (varying per subclass), we don't have targets to mark
    # abstract with abc.ABC nor typing.Protocol. See [1] for more context.
    #
    # 1: https://github.com/replicahq/artigraph/pull/60#discussion_r669089086
    __abstract__: ClassVar[bool] = True
    __class_key__: ClassVar[str] = class_name()

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)  # type: ignore # https://github.com/python/mypy/issues/4660
        # Default __abstract__ to False if not set explicitly on the class. __dict__ is read-only.
        setattr(cls, "__abstract__", cls.__dict__.get("__abstract__", False))

    @root_validator(pre=True)
    @classmethod
    def _block_abstract_instance(cls, values: dict[str, Any]) -> dict[str, Any]:
        if cls.__abstract__:
            raise ValueError(f"{cls} cannot be instantiated directly!")
        return values

    class Config:
        extra = Extra.forbid
