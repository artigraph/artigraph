from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

import abc

from arti.graphs import Graph
from arti.internal.models import Model


class Executor(Model):
    @abc.abstractmethod
    def build(self, graph: Graph) -> None:
        raise NotImplementedError()
