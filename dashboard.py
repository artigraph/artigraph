from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated

from arti import io
from arti.artifacts import Artifact
from arti.dashboards import GraphDashboard
from arti.formats.pickle import Pickle
from arti.graphs import Graph
from arti.partitions import CompositeKey
from arti.producers import Producer
from arti.storage.local import LocalFile, LocalFilePartition
from arti.types import Int64
from arti.views import View
from tests.arti.dummies import A1, A2, A3, A4, P1, P2


class Num(Artifact):
    type: Int64 = Int64()
    format: Pickle = Pickle()
    storage: LocalFile


class Increment(Producer):
    input_number: Num

    @staticmethod
    def build(input_number: int) -> Annotated[int, Num]:
        return input_number + 1


with TemporaryDirectory() as _dir:
    dir = Path(_dir)
    with Graph(name="test") as g:
        g.artifacts.a = Num(storage=LocalFile(path=str(dir / "a.pkl")))
        g.artifacts.b = Increment(input_number=g.artifacts.a).out(
            Num(storage=LocalFile(path=str(dir / "b.pkl")))
        )

    a, b = g.artifacts.a, g.artifacts.b
    # Bootstrap the initial artifact
    view = View.get_class_for(int)()
    io.write(
        0,
        type=a.type,
        format=a.format,
        storage_partition=LocalFilePartition(keys=CompositeKey(), path=a.storage.path),
        view=view,
    )

    GraphDashboard(graph=g).render()
