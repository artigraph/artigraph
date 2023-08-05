from __future__ import annotations

import importlib.metadata

__path__ = __import__("pkgutil").extend_path(__path__, __name__)
__version__ = importlib.metadata.version("arti")

import threading
from typing import Optional

from arti.annotations import Annotation
from arti.artifacts import Artifact
from arti.backends import Backend, BackendConnection
from arti.executors import Executor
from arti.fingerprints import Fingerprint
from arti.formats import Format
from arti.graphs import Graph, GraphSnapshot
from arti.io import read, register_reader, register_writer, write
from arti.partitions import InputFingerprints, PartitionField, PartitionKey, PartitionKeyTypes
from arti.producers import PartitionDependencies, Producer, producer
from arti.statistics import Statistic
from arti.storage import Storage, StoragePartition, StoragePartitions
from arti.thresholds import Threshold
from arti.types import Type, TypeAdapter, TypeSystem
from arti.versions import Version
from arti.views import View

# Export all interfaces.
__all__ = [
    "Annotation",
    "Artifact",
    "Backend",
    "BackendConnection",
    "Executor",
    "Fingerprint",
    "Format",
    "Graph",
    "GraphSnapshot",
    "InputFingerprints",
    "PartitionDependencies",
    "PartitionField",
    "PartitionKey",
    "PartitionKeyTypes",
    "Producer",
    "Statistic",
    "Storage",
    "StoragePartition",
    "StoragePartitions",
    "Threshold",
    "Type",
    "TypeAdapter",
    "TypeSystem",
    "Version",
    "View",
    "producer",
    "read",
    "register_reader",
    "register_writer",
    "write",
]


class _Context(threading.local):
    def __init__(self) -> None:
        super().__init__()
        self.graph: Optional[Graph] = None


context = _Context()
