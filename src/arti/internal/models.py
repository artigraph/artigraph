from typing import Any

from arti.internal.utils import class_name
from pydantic import BaseModel, Extra, root_validator

MODELS_REQUIRING_SUBCLASS = []


def requires_subclass(cls: type) -> type:
    MODELS_REQUIRING_SUBCLASS.append(cls)
    return cls


class Model(BaseModel):
    __class_key__: str = class_name()

    @root_validator(pre=True)
    @classmethod
    def _(cls, values: dict[str, Any]) -> dict[str, Any]:
        if cls in MODELS_REQUIRING_SUBCLASS:
            raise ValueError(f"{cls} cannot be instantiated directly!")
        return values

    class Config:
        extra = Extra.forbid
