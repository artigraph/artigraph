from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from arti import io
from arti.artifacts import Artifact
from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.formats.json import JSON
from arti.formats.pickle import Pickle
from arti.internal.utils import frozendict
from arti.partitions import CompositeKey, Int64Key
from arti.storage import StoragePartition
from arti.storage.local import LocalFile
from arti.types import Collection, Int64, Struct
from arti.views import View


class Num(Artifact):
    type: Int64 = Int64()


class PartitionedNum(Artifact):
    type: Collection = Collection(element=Struct(fields={"i": Int64()}), partition_by=("i",))


@pytest.mark.parametrize(
    ["format"],
    [
        (JSON(),),
        (Pickle(),),
    ],
)
def test_localfile_io(format: Format) -> None:
    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        a, n = Num(format=format, storage=LocalFile(path=str(dir / "a"))), 5

        io.write(
            n,
            a.type,
            a.format,
            a.storage.generate_partition(
                keys=CompositeKey(),
                input_fingerprint=Fingerprint.empty(),
                with_content_fingerprint=False,
            ),
            view=View.get_class_for(int)(),
        )
        partitions = a.discover_storage_partitions()
        assert len(partitions) == 1
        assert (
            io.read(
                a.type,
                a.format,
                partitions,
                view=View.get_class_for(int)(),
            )
            == n
        )
        with pytest.raises(ValueError, match="Multiple partitions can only be read into a list"):
            io.read(
                a.type,
                a.format,
                partitions * 2,
                view=View.get_class_for(int)(),
            )


@pytest.mark.parametrize(
    ["format"],
    [
        (JSON(),),
        (Pickle(),),
    ],
)
def test_localfile_io_partitioned(format: Format) -> None:
    with TemporaryDirectory() as _dir:
        dir = Path(_dir)
        a = PartitionedNum(format=format, storage=LocalFile(path=str(dir / "{i.key}")))
        data: dict[frozendict[str, int], StoragePartition] = {
            frozendict(i=i): a.storage.generate_partition(
                keys=CompositeKey(i=Int64Key(key=i)),
                input_fingerprint=Fingerprint.empty(),
                with_content_fingerprint=False,
            )
            for i in [1, 2]
        }
        for record, partition in data.items():
            io.write(
                [record],
                a.type,
                a.format,
                partition,
                view=View.get_class_for(list)(),
            )
        assert set(data.values()) == set(a.discover_storage_partitions())
        for record, partition in data.items():
            assert (
                io.read(
                    a.type,
                    a.format,
                    [partition],
                    view=View.get_class_for(list)(),
                )
                == [record]
            )
