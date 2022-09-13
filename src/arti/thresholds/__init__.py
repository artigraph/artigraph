__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from typing import Any, ClassVar

from arti.internal.models import Model
from arti.types import Type


class Threshold(Model):
    type: ClassVar[type[Type]]

    def check(self, value: Any) -> bool:
        raise NotImplementedError()
