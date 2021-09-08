from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from typing import Any

from arti.formats import Format
from arti.internal.utils import dispatch
from arti.storage import Storage
from arti.views import View

# TODO write/read partitioned data, column subset


@dispatch
def read(*, format: Format, storage: Storage, view: View) -> Any:
    raise NotImplementedError(
        f"Read into {view} view from {format} format in {storage} storage not implemented."
    )


@dispatch
def write(data: Any, *, format: Format, storage: Storage, view: View) -> None:
    raise NotImplementedError(
        f"Write from {view} view into {format} format in {storage} storage not implemented."
    )
