from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from collections.abc import Callable, Sequence
from types import ModuleType
from typing import Any, NamedTuple, get_args, get_origin

from arti.formats import Format
from arti.internal.type_hints import signature
from arti.internal.utils import import_submodules, register
from arti.storage import (
    StoragePartition,
    StoragePartitionSnapshot,
    StoragePartitionSnapshots,
)
from arti.types import Type, is_partitioned
from arti.views import View


class IOKey(NamedTuple):
    """The (Type, Format, StoragePartition, View) classes an IO implementation handles."""

    type_: type[Type]
    format: type[Format]
    storage_partition: type[StoragePartition]
    view: type[View]

    def __str__(self) -> str:
        return ", ".join(
            f"{name}={cls.__name__}" for name, cls in zip(self._fields, self, strict=True)
        )


_readers: dict[IOKey, Callable[..., Any]] = {}
_writers: dict[IOKey, Callable[..., Any]] = {}
_submodules: dict[str, ModuleType] | None = None


def _discover() -> None:
    global _submodules
    if _submodules is None:
        _submodules = import_submodules(__path__, __name__)


def _register[F: Callable[..., Any]](
    registry: dict[IOKey, Callable[..., Any]],
    func: F,
    params: tuple[str, ...],
    *,
    sequence_param: str | None,
) -> F:
    sig = signature(func, remove_owner=False)
    if tuple(sig.parameters) != params or any(
        param.kind is not param.POSITIONAL_OR_KEYWORD for param in sig.parameters.values()
    ):
        raise TypeError(f"Expected `{func.__name__}` to have positional {list(params)} parameters")
    hints = {name: param.annotation for name, param in sig.parameters.items()}
    # Readers take a Sequence of partitions, so key on the element type.
    if sequence_param is not None:
        if get_origin(hints[sequence_param]) is not Sequence:
            raise TypeError(
                f"Expected the `{func.__name__}.{sequence_param}` parameter to be a Sequence[...], got {hints[sequence_param]}"
            )
        (hints[sequence_param],) = get_args(hints[sequence_param])
    names = params[-4:]
    for name, base in zip(names, (Type, Format, StoragePartition, View), strict=True):
        if not (isinstance(hints[name], type) and issubclass(hints[name], base)):
            raise TypeError(
                f"Expected the `{func.__name__}.{name}` parameter to be a subclass of {base}, got {hints[name]}"
            )
    register(registry, IOKey(*(hints[name] for name in names)), func)
    return func


def _lookup(registry: dict[IOKey, Callable[..., Any]], key: IOKey, name: str) -> Callable[..., Any]:
    _discover()
    matches = {k for k in registry if all(map(issubclass, key, k))}
    # Keep the most specific matches, those without a more specific match on every axis.
    best = [k for k in matches if not any(o != k and all(map(issubclass, o, k)) for o in matches)]
    if not best:
        raise ValueError(f"No `{name}` implementation found for: {key}")
    if len(best) > 1:
        funcs = sorted(registry[k].__name__ for k in best)
        raise ValueError(f"Multiple `{name}` implementations match {key}: {funcs}")
    return registry[best[0]]


def register_reader[F: Callable[..., Any]](func: F) -> F:
    """
    Register a function to read data for its annotated Type, Format, StoragePartition, and View.

    The function must have `type_`, `format`, `storage_partitions` (a `Sequence[...]`), and `view`
    parameters, annotated with the most general classes it supports.
    """
    return _register(
        _readers,
        func,
        ("type_", "format", "storage_partitions", "view"),
        sequence_param="storage_partitions",
    )


def register_writer[F: Callable[..., Any]](func: F) -> F:
    """
    Register a function to write data for its annotated Type, Format, StoragePartition, and View.

    The function must have `data`, `type_`, `format`, `storage_partition`, and `view` parameters,
    annotated with the most general classes it supports. It may return an updated StoragePartition.
    """
    return _register(
        _writers,
        func,
        ("data", "type_", "format", "storage_partition", "view"),
        sequence_param=None,
    )


def lookup_reader(key: IOKey) -> Callable[..., Any]:
    """Return the most specific registered reader for `key`, raising if none or several match."""
    return _lookup(_readers, key, "io.read")


def lookup_writer(key: IOKey) -> Callable[..., Any]:
    """Return the most specific registered writer for `key`, raising if none or several match."""
    return _lookup(_writers, key, "io.write")


def read(
    type_: Type, format: Format, storage_partition_snapshots: StoragePartitionSnapshots, view: View
) -> Any:
    if not storage_partition_snapshots:
        raise FileNotFoundError("No data")
    if len(storage_partition_snapshots) > 1 and not is_partitioned(type_):
        raise ValueError(
            f"Multiple partitions can only be read into a partitioned Collection, not {type_}"
        )
    storage_partitions = tuple(
        snapshot.storage_partition for snapshot in storage_partition_snapshots
    )
    if len(partition_types := {type(partition) for partition in storage_partitions}) > 1:
        raise ValueError(
            f"All partitions must share one StoragePartition type, got: {sorted(t.__name__ for t in partition_types)}"
        )
    reader = lookup_reader(
        IOKey(type(type_), type(format), type(storage_partitions[0]), type(view))
    )
    # TODO: Verify the content_fingerprints before (and/or after) reading data.
    #
    # TODO: Check that the returned data matches the Type/View. Likely add a View method that can
    # handle this type + schema checking, filtering to column/row subsets if necessary, etc
    return reader(type_, format, storage_partitions, view)


def write(
    data: Any, type_: Type, format: Format, storage_partition: StoragePartition, view: View
) -> StoragePartitionSnapshot:
    writer = lookup_writer(IOKey(type(type_), type(format), type(storage_partition), type(view)))
    if (updated := writer(data, type_, format, storage_partition, view)) is not None:
        storage_partition = updated
    return storage_partition.snapshot()
