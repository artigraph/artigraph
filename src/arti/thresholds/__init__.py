__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from typing import Any, ClassVar

from arti.types import Type


class Threshold:
    type: ClassVar[type[Type]]

    def check(self, value: Any) -> bool:
        raise NotImplementedError()
