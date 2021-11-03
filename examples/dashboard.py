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


class Num(Artifact):
    type: Int64 = Int64()
    format: Pickle = Pickle()
    storage: LocalFile


class Add(Producer):
    input_0: Num
    input_1: Num

    @staticmethod
    def build(input_0: int, input_1: int) -> Annotated[int, Num]:
        return input_0 + input_1


class Multiply(Producer):
    input_0: Num
    input_1: Num

    @staticmethod
    def build(input_0: int, input_1: int) -> Annotated[int, Num]:
        return input_0 * input_1


def bootstrap_num(value: int, artifact: Artifact):
    io.write(
        value,
        type=artifact.type,
        format=artifact.format,
        storage_partition=LocalFilePartition(keys=CompositeKey(), path=artifact.storage.path),
        view=view,
    )


with TemporaryDirectory() as _dir:
    dir = Path(_dir)
    # Graph g - declaration
    with Graph(name="(a+b)*(c+d)") as g:
        # Input Artifacts - Nums
        g.artifacts.a = Num(storage=LocalFile(path=str(dir / "a.pkl")))
        g.artifacts.b = Num(storage=LocalFile(path=str(dir / "b.pkl")))
        g.artifacts.c = Num(storage=LocalFile(path=str(dir / "c.pkl")))
        g.artifacts.d = Num(storage=LocalFile(path=str(dir / "d.pkl")))

        # Intermediate outputs - Nums from Add Producers (2 instances)
        g.artifacts.a_plus_b = Add(input_0=g.artifacts.a, input_1=g.artifacts.b).out(
            Num(storage=LocalFile(path=str(dir / "a_plus_b.pkl")))
        )
        g.artifacts.c_plus_d = Add(input_0=g.artifacts.c, input_1=g.artifacts.d).out(
            Num(storage=LocalFile(path=str(dir / "c_plus_d.pkl")))
        )

        # Final output - Num from Multiply Producer
        g.artifacts.product = Multiply(
            input_0=g.artifacts.a_plus_b, input_1=g.artifacts.c_plus_d
        ).out(Num(storage=LocalFile(path=str(dir / "product.pkl"))))

    # Bootstrap the initial artifacts
    a, b, c, d = g.artifacts.a, g.artifacts.b, g.artifacts.c, g.artifacts.d
    view = View.get_class_for(int)()
    bootstrap_num(1, a)
    bootstrap_num(5, b)
    bootstrap_num(2, c)
    bootstrap_num(4, d)

    # Build dashboard for Graph g
    GraphDashboard(graph=g).render()
