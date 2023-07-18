from __future__ import annotations

import importlib
import json
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from graphlib import TopologicalSorter
from operator import attrgetter
from typing import Any, Literal, Optional, TypeVar

import py2neo
from box import Box

from arti import (
    Artifact,
    Backend,
    BackendConnection,
    Fingerprint,
    Graph,
    GraphSnapshot,
    InputFingerprints,
    StoragePartition,
    StoragePartitions,
)
from arti.internal.models import Model
from arti.internal.type_hints import lenient_issubclass

_type_converters: dict[type, Callable[[Any], Any]] = defaultdict(lambda: lambda x: x)


try:
    import interchange.time  # type: ignore[import]

    _type_converters[interchange.time.Date] = interchange.time.Date.to_native
except ImportError:
    pass

_Model = TypeVar("_Model", bound=Model)


def _fingerprint_to_int(fingerprint: Fingerprint) -> int:
    if fingerprint.key is None:
        raise ValueError("Fingerprint is empty.")
    return int(fingerprint.key)


class Neo4jConnection(BackendConnection):
    def __init__(self, *, backend: Neo4jBackend, neo_graph: py2neo.Graph):
        self.backend = backend
        self.neo_graph = neo_graph

    def _neo4j_types_to_python(self, value: Any) -> Any:
        return _type_converters[type(value)](value)

    def run_cypher(self, query: str, **kwargs: Any) -> Any:
        return self.neo_graph.run(query, **kwargs)

    def get_model_class_name(self, model: Model) -> str:
        return f"{type(model).__module__}:{type(model).__qualname__}"

    def connect_nodes(
        self,
        left_fingerprint: Fingerprint,
        right_fingerprint: Fingerprint,
        relation_type: str,
        cardinality: Literal["one", "dict", "list"],
        relationship_attrs: Optional[dict[str, Any]] = None,
    ) -> Any:
        relationship_attrs = relationship_attrs or {}
        relationship_attrs["cardinality"] = cardinality
        relationship_expression = (
            f"[r:{relation_type} {{{', '.join([f'{key}: ${key}' for key in relationship_attrs])}}}]"
            if relationship_attrs
            else f"[r:{relation_type}]"
        )
        # TODO use node_type to build relationships
        return self.neo_graph.run(
            f"match (a), (b) where a.fingerprint = $left_fingerprint and b.fingerprint = $right_fingerprint merge (a)-{relationship_expression}->(b) return r",
            left_fingerprint=_fingerprint_to_int(left_fingerprint),
            right_fingerprint=_fingerprint_to_int(right_fingerprint),
            **relationship_attrs,
        )

    def node_to_arti(
        self,
        node: py2neo.Node,
        relation_map: dict[tuple[py2neo.Node, str], py2neo.Relationship],
        node_model_map: dict[py2neo.Node, _Model],
    ) -> _Model:
        model_data: dict[str, Any] = {}
        mod_name, cls_name = node["class_name"].split(":")
        module = importlib.import_module(mod_name)
        model: type[_Model] = attrgetter(cls_name)(module)
        for key, value in model.__fields__.items():
            if issubclass(value.type_, Fingerprint):
                model_data[key] = Fingerprint.from_int(node[key])
            else:
                try:
                    relations = list(relation_map[(node, key)])
                    cardinality = relations[0]["cardinality"]
                    if cardinality == "one":
                        if len(relations) > 1:
                            raise ValueError(
                                f"Expected cardinality of one for node: {node} and key: {key}, got multiple relations: {relations}"
                            )
                        model_data[key] = node_model_map[relations[0].end_node]
                    elif cardinality == "dict":
                        if not all(relation["cardinality"] == "dict" for relation in relations):
                            raise ValueError(
                                f'Cardinality mismatch for node: {node} with key: {key}! Cardinality of type dict was expected for all relations. Got: {{relation["cardinality"] for relation in relations}}.'
                            )
                        model_data[key] = {
                            relation["name"]: node_model_map[relation.end_node]
                            for relation in relations
                        }
                    elif cardinality == "list":
                        if not all(relation["cardinality"] == "list" for relation in relations):
                            raise ValueError(
                                f'Cardinality mismatch for node: {node} with key: {key}! Cardinality of type list was expected for all relations. Got: {{relation["cardinality"] for relation in relations}}.'
                            )
                        model_data[key] = [
                            node_model_map[relation.end_node] for relation in relations
                        ]
                    else:
                        raise ValueError(
                            f"Unknown cardinality: {cardinality} on node: {node} with key: {key}!"
                        )
                except KeyError:
                    if lenient_issubclass(value.outer_type_, Mapping):
                        try:
                            model_data[key] = json.loads(node[key])
                        except TypeError:
                            model_data[key] = {}
                    elif lenient_issubclass(value.outer_type_, tuple):
                        if node[key] is not None:
                            model_data[key] = tuple(
                                [
                                    self._neo4j_types_to_python(value)
                                    for value in json.loads(node[key])
                                ]
                            )
                        else:
                            model_data[key] = ()
                    else:
                        model_data[key] = self._neo4j_types_to_python(node[key])
        return model(**model_data)

    def read_model(
        self,
        model_type: type[_Model],
        fingerprint: Fingerprint,
        node_model_map: Optional[dict[py2neo.Node, _Model]] = None,
    ) -> tuple[_Model, dict[py2neo.Node, _Model]]:
        node_model_map = node_model_map if node_model_map is not None else {}
        assert fingerprint.key is not None
        data = self.neo_graph.run(
            f"match (n:{model_type.__name__} {{fingerprint: $fingerprint}})-[r *1..]->() return r",
            fingerprint=_fingerprint_to_int(fingerprint),
        ).data()

        if not data:
            raise ValueError(f"No {model_type.__name__} node with fingerprint {fingerprint} found!")

        relation_map = defaultdict(set)
        deps = defaultdict(set)
        for result in data:
            for relation in result["r"]:
                relation_map[(relation.start_node, relation.__class__.__name__)].add(relation)
                deps[relation.start_node].add(relation.end_node)

        frozen_relation_map = dict(relation_map)
        for start_node in TopologicalSorter(deps).static_order():
            if start_node in node_model_map:
                continue
            model = self.node_to_arti(
                node=start_node, relation_map=frozen_relation_map, node_model_map=node_model_map
            )
            node_model_map[start_node] = model
        return model, node_model_map

    def read_models(
        self, model_type: type[_Model], fingerprints: list[Fingerprint]
    ) -> tuple[list[_Model], dict[py2neo.Node, _Model]]:
        models = []
        node_model_map: dict[py2neo.Node, _Model] = {}
        for fingerprint in fingerprints:
            model, model_map = self.read_model(model_type, fingerprint, node_model_map)
            node_model_map.update(model_map)
            models.append(model)
        return models, node_model_map

    def _map_model_types(
        self,
        key: str,
        value: Any,
        model: _Model,
        nodes_to_connect: dict[str, list[tuple[Fingerprint, dict[str, Any]]]],
        sub_key: Optional[str] = None,
        in_iterable: bool = False,
    ) -> Any:
        if isinstance(value, Fingerprint):
            return value.key if value.key is None else _fingerprint_to_int(value)
        elif isinstance(value, Box):
            if all(lenient_issubclass(type(v[1]), Model) for v in value.walk()):
                [
                    self._map_model_types(
                        key, v, model, nodes_to_connect=nodes_to_connect, sub_key=k
                    )
                    for k, v in value.walk()
                ]
                return None
            else:
                return json.dumps(value, sort_keys=True)
        elif lenient_issubclass(type(value), Mapping):
            if all(lenient_issubclass(type(sub_value), Model) for sub_value in value.values()):
                {
                    k: self._map_model_types(
                        key, v, model, nodes_to_connect=nodes_to_connect, sub_key=k
                    )
                    for k, v in value.items()
                }
                return None
            else:
                return json.dumps(value, sort_keys=True)
        elif lenient_issubclass(type(value), tuple) and not isinstance(value, str):
            if all(lenient_issubclass(type(sub_value), Model) for sub_value in value):
                [
                    self._map_model_types(
                        key, v, model, nodes_to_connect=nodes_to_connect, in_iterable=True
                    )
                    for v in value
                ]
                return None
            else:
                return json.dumps(value, sort_keys=True)
        elif isinstance(value, Model):
            if key not in nodes_to_connect:
                nodes_to_connect[key] = []
            if sub_key:
                nodes_to_connect[key].append(
                    (
                        self.write_model(value),
                        {"cardinality": "dict", "relationship_attrs": {"name": sub_key}},
                    )
                )
                return None
            elif in_iterable:
                nodes_to_connect[key].append((self.write_model(value), {"cardinality": "list"}))
                return None
            else:
                nodes_to_connect[key].append((self.write_model(value), {"cardinality": "one"}))
                return None
        else:
            return value

    def write_model(self, model: _Model) -> Fingerprint:
        assert not isinstance(model, Fingerprint)
        assert model.fingerprint.key is not None
        existing = self.neo_graph.run(
            f"match (n:{model.__class__.__name__} {{fingerprint: $fingerprint}}) return n.fingerprint as fingerprint",
            fingerprint=_fingerprint_to_int(model.fingerprint),
        ).data()
        if existing:
            return Fingerprint.from_int(existing[0]["fingerprint"])
        else:
            props = {
                "class_name": self.get_model_class_name(model),
                "fingerprint": _fingerprint_to_int(model.fingerprint),
            }
            nodes_to_connect: dict[str, list[tuple[Fingerprint, dict[str, Any]]]] = {}
            for key, value in model._iter():
                props[key] = self._map_model_types(
                    key, value, model, nodes_to_connect=nodes_to_connect
                )

            labels = [model.__class__.__name__] + [
                base.__name__ for base in model.__class__.__bases__
            ]
            # TODO merge
            query = f"create (n:{':'.join(labels)} {{{', '.join([f'{key}: ${key}' for key in props])}}}) return n.fingerprint as fingerprint"
            new_node_fingerprint = Fingerprint.from_int(
                self.neo_graph.run(query, **props).data()[0]["fingerprint"]
            )
            for key, values in nodes_to_connect.items():
                for fingerprint, connection_attrs in values:
                    assert fingerprint is not None
                    assert isinstance(props["fingerprint"], int)
                    self.connect_nodes(
                        Fingerprint.from_int(props["fingerprint"]),
                        fingerprint,
                        key,
                        **connection_attrs,
                    )
            return new_node_fingerprint

    def read_artifact_partitions(
        self, artifact: Artifact, input_fingerprints: InputFingerprints = InputFingerprints()
    ) -> StoragePartitions:
        where_query = " or ".join(
            [
                f"sp.input_fingerprint = {_fingerprint_to_int(fingerprint)}"
                for fingerprint in input_fingerprints.values()
            ]
        )
        assert artifact.fingerprint.key is not None
        query = f"""match (a:Artifact {{fingerprint: $fingerprint}})
        -[sar:storage]
        ->(s:Storage)
        <-[spr:storage]
        -(sp:StoragePartition)
        where {where_query} return sp.fingerprint as fingerprint
        """
        sp_fingerprints = [
            Fingerprint.from_int(row["fingerprint"])
            for row in self.neo_graph.run(
                query, fingerprint=_fingerprint_to_int(artifact.fingerprint)
            ).data()
        ]

        partitions, _ = self.read_models(StoragePartition, sp_fingerprints)  # type: ignore[type-abstract]
        return tuple(partitions)

    def write_artifact_partitions(self, artifact: Artifact, partitions: StoragePartitions) -> None:
        self.write_model(artifact)
        [self.write_model(partition) for partition in partitions]

    def write_snapshot_partitions(
        self,
        snapshot: GraphSnapshot,
        artifact_key: str,
        artifact: Artifact,
        partitions: StoragePartitions,
    ) -> None:
        assert snapshot.fingerprint.key is not None

        for partition in partitions:
            self.write_model(partition)
            self.connect_nodes(
                left_fingerprint=snapshot.fingerprint,
                right_fingerprint=partition.fingerprint,
                relation_type="partition",
                cardinality="dict",
                relationship_attrs={"name": artifact_key},
            )

    def read_snapshot_partitions(
        self, snapshot: GraphSnapshot, artifact_key: str, artifact: Artifact
    ) -> StoragePartitions:
        query = "match (gs:GraphSnapshot {fingerprint: $gs_fingerprint})-[rp:partition {name: $artifact_key}]->(sp:StoragePartition) return sp.fingerprint as fingerprint"
        sp_fingerprints = [
            Fingerprint.from_int(row["fingerprint"])
            for row in self.neo_graph.run(
                query,
                gs_fingerprint=_fingerprint_to_int(snapshot.fingerprint),
                artifact_key=artifact_key,
            ).data()
        ]

        partitions: list[StoragePartition]
        partitions, _ = self.read_models(StoragePartition, sp_fingerprints)  # type: ignore[type-abstract]
        return tuple(partitions)

    def read_snapshot_tag(self, graph_name: str, tag: str) -> GraphSnapshot:
        raise NotImplementedError()

    def write_snapshot_tag(
        self, snapshot: GraphSnapshot, tag: str, overwrite: bool = False
    ) -> None:
        raise NotImplementedError()

    def read_snapshot(self, name: str, snapshot_id: Fingerprint) -> GraphSnapshot:
        return self.read_model(GraphSnapshot, snapshot_id)[0]

    def write_snapshot(self, snapshot: GraphSnapshot) -> None:
        self.write_model(snapshot)

    def read_graph(self, name: str, fingerprint: Fingerprint) -> Graph:
        return self.read_model(Graph, fingerprint)[0].copy(update={"backend": self.backend})

    def write_graph(self, graph: Graph) -> None:
        self.write_model(graph)


class Neo4jBackend(Backend[Neo4jConnection]):
    database: str = "artigraph"
    host: str
    port: int
    username: str
    password: str

    @contextmanager
    def connect(self) -> Iterator[Neo4jConnection]:
        uri = f"bolt://{self.host}:{self.port}"
        graph = py2neo.Graph(uri, auth=(self.username, self.password), name=self.database)
        try:
            yield Neo4jConnection(backend=self, neo_graph=graph)
        finally:
            graph.service.connector.close()
