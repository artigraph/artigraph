__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from collections.abc import Callable, Sequence
from functools import partial
from typing import Any, cast

from arti.formats import Format
from arti.internal.utils import dispatch, import_submodules
from arti.storage import StoragePartition
from arti.types import Type
from arti.views import View

# TODO write/read partitioned data, column subset

# TODO: Auto import io/ submodules (like View) - perhaps add decorator on read/write to
# lazily trigger the import on first use.

# import_submodules(__path__, __name__)

_import_submodules = cast(Callable[[], None], partial(import_submodules, __path__, __name__))


@dispatch(once_before=_import_submodules)
def read(
    type: Type, format: Format, storage_partitions: Sequence[StoragePartition], view: View
) -> Any:
    raise NotImplementedError(
        f"Read into {view} view from {format} format in {storage_partitions} storage not implemented."
    )


@dispatch(once_before=_import_submodules)
def write(
    data: Any, type: Type, format: Format, storage_partition: StoragePartition, view: View
) -> None:
    raise NotImplementedError(
        f"Write from {view} view into {format} format in {storage_partition} storage not implemented."
    )
