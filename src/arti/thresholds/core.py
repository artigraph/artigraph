from __future__ import annotations

from typing import Any, ClassVar

from arti.types.core import Type


class Threshold:
    schema: ClassVar[type[Type]]

    def check(self, value: Any) -> bool:
        raise NotImplementedError()
