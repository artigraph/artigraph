from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

import threading
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from arti.graphs import Graph


class _Context(threading.local):
    def __init__(self) -> None:
        super().__init__()
        self.graph: Optional[Graph] = None


context = _Context()
