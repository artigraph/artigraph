__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from collections.abc import Sequence
from types import ModuleType
from typing import Any, Optional

from arti.formats import Format
from arti.internal.dispatch import multipledispatch
from arti.internal.utils import import_submodules
from arti.storage import StoragePartition, StoragePartitionVar
from arti.types import Type, is_partitioned
from arti.views import View

_submodules: Optional[dict[str, ModuleType]] = None


def _discover() -> None:
    global _submodules
    if _submodules is None:
        _submodules = import_submodules(__path__, __name__)


@multipledispatch("io.read", discovery_func=_discover)
def _read(
    type_: Type, format: Format, storage_partitions: Sequence[StoragePartition], view: View
) -> Any:
    raise NotImplementedError(
        f"Reading {type(storage_partitions[0])} storage in {type(format)} format to {type(view)} view is not implemented."
    )


register_reader = _read.register


def read(
    type_: Type, format: Format, storage_partitions: Sequence[StoragePartition], view: View
) -> Any:
    if not storage_partitions:
        # NOTE: Aside from simplifying this check up front, multiple dispatch with unknown list
        # element type can be ambiguous/error.
        raise FileNotFoundError("No data")
    if len(storage_partitions) > 1 and not is_partitioned(type_):
        raise ValueError(
            f"Multiple partitions can only be read into a partitioned Collection, not {type_}"
        )
    # TODO Checks that the returned data matches the Type/View
    #
    # Likely add a View method that can handle this type + schema checking, filtering to column/row subsets if necessary, etc
    return _read(type_, format, storage_partitions, view)


@multipledispatch("io.write", discovery_func=_discover)  # type: ignore[arg-type]
def _write(
    data: Any, type_: Type, format: Format, storage_partition: StoragePartitionVar, view: View
) -> Optional[StoragePartitionVar]:
    raise NotImplementedError(
        f"Writing {type(view)} view into {type(format)} format in {type(storage_partition)} storage is not implemented."
    )


register_writer = _write.register


def write(
    data: Any, type_: Type, format: Format, storage_partition: StoragePartitionVar, view: View
) -> StoragePartitionVar:
    if (updated := _write(data, type_, format, storage_partition, view)) is not None:
        return updated
    return storage_partition
