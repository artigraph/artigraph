from __future__ import annotations

import inspect
import json
from collections.abc import Iterable
from typing import Any

from arti.artifacts import Artifact
from arti.backends import Backend, BackendConnection
from arti.fingerprints import Fingerprint
from arti.graphs import Graph, GraphSnapshot
from arti.internal.models import Model
from arti.producers import Producer
from arti.storage import StoragePartitionSnapshot

type JSON = dict[str, Any]

# JavaScript numbers are doubles, so larger ints (eg: int64 Fingerprints) would silently lose
# precision in the browser. Send them as strings instead.
_MAX_SAFE_INT = 2**53 - 1


class NotFoundError(LookupError):
    """Raised when the requested Graph, version, or snapshot is not known."""


def _json_safe(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value if abs(value) <= _MAX_SAFE_INT else str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}  # pyright: ignore[reportUnknownVariableType]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]  # pyright: ignore[reportUnknownVariableType]
    return value


def _dump(model: Model) -> JSON:
    return _json_safe(model.model_dump(mode="json"))


def _describe_class(value: object, *, base: type) -> JSON:
    """Name a value's class and its docstring, unless inherited from the `base` interface."""
    cls = type(value)
    doc = cls.__dict__.get("__doc__")
    if cls is base or doc is None:
        doc = None
    return {
        "class": cls.__name__,
        "module": cls.__module__,
        "description": inspect.cleandoc(doc) if doc else None,
    }


def _artifact(key: str | None, artifact: Artifact) -> JSON:
    return {
        "id": _artifact_id(key, artifact),
        "key": key,
        **_describe_class(artifact, base=Artifact),
        "fingerprint": str(artifact.fingerprint),
        "produced": artifact.producer_output is not None,
        "type": {
            "summary": artifact.type.friendly_key,
            "description": artifact.type.description,
            "spec": _dump(artifact.type),
        },
        "format": _dump(artifact.format),
        "storage": _dump(artifact.storage),
        "annotations": [_dump(annotation) for annotation in artifact.annotations],
        "statistics": [_dump(statistic) for statistic in artifact.statistics],
    }


def _artifact_id(key: str | None, artifact: Artifact) -> str:
    # Producer inputs are usually Graph Artifacts, but may be Artifacts that were never assigned
    # to the Graph. Show those as "external" nodes rather than dropping the edge.
    return f"artifact:{key}" if key is not None else f"external:{artifact.fingerprint}"


def _producer_id(producer: Producer) -> str:
    return f"producer:{producer.fingerprint}"


def _producer(producer: Producer, graph: Graph, outputs: tuple[Artifact, ...]) -> JSON:
    description = _describe_class(producer, base=Producer)
    if description["description"] is None and (doc := inspect.getdoc(type(producer).build)):
        description["description"] = doc
    return {
        "id": _producer_id(producer),
        **description,
        "fingerprint": str(producer.fingerprint),
        # The (fingerprint) serialization may omit fields, eg: SemVer >=1 only includes `major`.
        "version": {"summary": repr(producer.version), "spec": _dump(producer.version)},
        "annotations": [_dump(annotation) for annotation in producer.annotations],
        "inputs": {
            name: _artifact_id(graph.artifact_to_key.get(artifact), artifact)
            for name, artifact in producer.inputs.items()
        },
        "outputs": [
            _artifact_id(graph.artifact_to_key[artifact], artifact) for artifact in outputs
        ],
    }


def _graph(graph: Graph) -> JSON:
    artifacts = {key: _artifact(key, artifact) for key, artifact in sorted(graph.artifacts.walk())}
    producers = list[JSON]()
    edges = list[JSON]()
    external = dict[str, JSON]()
    for producer, outputs in sorted(
        graph.producer_outputs.items(), key=lambda item: graph.artifact_to_key[item[1][0]]
    ):
        producers.append(_producer(producer, graph, outputs))
        for name, artifact in producer.inputs.items():
            key = graph.artifact_to_key.get(artifact)
            if key is None:
                node = _artifact(None, artifact)
                external.setdefault(node["id"], node)
            edges.append(
                {
                    "source": _artifact_id(key, artifact),
                    "target": _producer_id(producer),
                    "label": name,
                }
            )
        for artifact in outputs:
            edges.append(
                {
                    "source": _producer_id(producer),
                    "target": _artifact_id(graph.artifact_to_key[artifact], artifact),
                    "label": None,
                }
            )
    return {
        "name": graph.name,
        "fingerprint": str(graph.fingerprint),
        "path_tags": dict(graph.path_tags),
        "artifacts": [*artifacts.values(), *external.values()],
        "producers": producers,
        "edges": edges,
    }


def _partition(partition: StoragePartitionSnapshot) -> JSON:
    return {
        "fingerprint": str(partition.fingerprint),
        "content_fingerprint": str(partition.content_fingerprint),
        "input_fingerprint": (
            None if partition.input_fingerprint is None else str(partition.input_fingerprint)
        ),
        "partition_key": {
            name: _dump(field) for name, field in sorted(partition.partition_key.items())
        },
        "storage_partition": _dump(partition.storage_partition),
    }


def _partition_sort_key(partition: JSON) -> tuple[str, str]:
    # Backends return partitions in any order, so sort for a stable display.
    return json.dumps(partition["partition_key"], sort_keys=True), partition["fingerprint"]


def _parse_fingerprint(value: str) -> Fingerprint:
    try:
        return Fingerprint.from_int(int(value))
    except ValueError as error:
        raise NotFoundError(f"Invalid fingerprint: {value!r}") from error


class DashboardData:
    """
    Read-only JSON documents describing the metadata recorded in a Backend.

    Every call reads the Backend again, so the documents reflect new snapshots and tags without a
    restart. `definitions` are Graphs defined in code (eg: the Graph the dashboard was started
    with), which are shown even if they were never written to the Backend.
    """

    def __init__(self, backend: Backend[Any], definitions: Iterable[Graph] = ()) -> None:
        self.backend = backend
        self.definitions = {(graph.name, graph.fingerprint): graph for graph in definitions}

    def _defined(self, name: str) -> dict[Fingerprint, Graph]:
        return {
            fingerprint: graph
            for (graph_name, fingerprint), graph in self.definitions.items()
            if graph_name == name
        }

    @staticmethod
    def _tags_by_snapshot(conn: BackendConnection, name: str) -> dict[Fingerprint, list[str]]:
        tags = dict[Fingerprint, list[str]]()
        for tag, snapshot in conn.read_snapshot_tags(name).items():
            tags.setdefault(snapshot.fingerprint, []).append(tag)
        return tags

    def graphs(self) -> JSON:
        """Summarize every known Graph name."""
        with self.backend.connect() as conn:
            names = conn.read_graph_names() | {name for name, _ in self.definitions}
            graphs = list[JSON]()
            for name in sorted(names):
                versions = {graph.fingerprint for graph in conn.read_graphs(name)}
                graphs.append(
                    {
                        "name": name,
                        "versions": len(versions | set(self._defined(name))),
                        "snapshots": len(conn.read_snapshots(name)),
                        "tags": list(conn.read_snapshot_tags(name)),
                        "defined": bool(self._defined(name)),
                    }
                )
        return {"graphs": graphs}

    def graph(self, name: str) -> JSON:
        """List the versions, snapshots, and tags of the named Graph."""
        defined = self._defined(name)
        with self.backend.connect() as conn:
            persisted = {graph.fingerprint: graph for graph in conn.read_graphs(name)}
            snapshots = conn.read_snapshots(name)
            tags_by_snapshot = self._tags_by_snapshot(conn, name)
        if not persisted and not defined:
            raise NotFoundError(f"Unknown Graph: {name!r}")
        versions = persisted | defined
        return {
            "name": name,
            "versions": [
                {
                    "fingerprint": str(fingerprint),
                    "persisted": fingerprint in persisted,
                    "defined": fingerprint in defined,
                    "artifacts": len(list(versions[fingerprint].artifacts.walk())),
                    "producers": len(versions[fingerprint].producers),
                }
                for fingerprint in sorted(versions)
            ],
            "snapshots": [
                {
                    "fingerprint": str(snapshot.fingerprint),
                    "id": str(snapshot.id),
                    "graph_fingerprint": str(snapshot.graph.fingerprint),
                    "tags": tags_by_snapshot.get(snapshot.fingerprint, []),
                }
                for snapshot in snapshots
            ],
        }

    def graph_version(self, name: str, fingerprint: str) -> JSON:
        """Describe the Artifacts, Producers, and dependencies of one version of a Graph."""
        parsed = _parse_fingerprint(fingerprint)
        if (graph := self.definitions.get((name, parsed))) is None:
            with self.backend.connect() as conn:
                try:
                    graph = conn.read_graph(name, parsed)
                except KeyError as error:
                    raise NotFoundError(f"Unknown Graph version: {name!r} {fingerprint}") from error
        return _graph(graph)

    def snapshot(self, name: str, fingerprint: str) -> JSON:
        """Describe a GraphSnapshot, including the partitions of every Artifact in it."""
        parsed = _parse_fingerprint(fingerprint)
        with self.backend.connect() as conn:
            try:
                snapshot: GraphSnapshot = conn.read_snapshot(name, parsed)
            except KeyError as error:
                raise NotFoundError(f"Unknown GraphSnapshot: {name!r} {fingerprint}") from error
            partitions = {
                key: sorted(
                    (
                        _partition(partition)
                        for partition in conn.read_snapshot_partitions(snapshot, key, artifact)
                    ),
                    key=_partition_sort_key,
                )
                for key, artifact in sorted(snapshot.artifacts.walk())
            }
            tags = self._tags_by_snapshot(conn, name).get(snapshot.fingerprint, [])
        return {
            **_graph(snapshot.graph),
            "snapshot": {
                "fingerprint": str(snapshot.fingerprint),
                "id": str(snapshot.id),
                "tags": tags,
            },
            "partitions": partitions,
        }
