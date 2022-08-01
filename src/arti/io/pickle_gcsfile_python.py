import pickle
from collections.abc import Sequence
from itertools import chain
from typing import Any

from gcsfs import GCSFileSystem

from arti.formats.pickle import Pickle
from arti.io import register_reader, register_writer
from arti.storage.google.cloud.storage import GCSFilePartition
from arti.types import Type, is_partitioned
from arti.views.python import PythonBuiltin

# TODO: Do I need to inject the partition keys into the returned data? Likely useful...
# Maybe a View option?


def _read_pickle_file(path: str) -> Any:
    # TODO: GCSFileSystem needs to be injected somehow
    with GCSFileSystem().open(path, "rb") as file:
        return pickle.load(file)  # nosec # User opted into pickle, ignore bandit check


@register_reader
def _read_pickle_gcsfile_python(
    type_: Type,
    format: Pickle,
    storage_partitions: Sequence[GCSFilePartition],
    view: PythonBuiltin,
) -> Any:
    if is_partitioned(type_):
        return list(
            chain.from_iterable(
                _read_pickle_file(storage_partition.qualified_path)
                for storage_partition in storage_partitions
            )
        )
    else:
        assert len(storage_partitions) == 1  # Better error handled in base read
        return _read_pickle_file(storage_partitions[0].qualified_path)


@register_writer
def _write_pickle_gcsfile_python(
    data: Any, type_: Type, format: Pickle, storage_partition: GCSFilePartition, view: PythonBuiltin
) -> None:
    with GCSFileSystem().open(storage_partition.qualified_path, "wb") as file:
        pickle.dump(data, file)
