from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from datetime import datetime
from typing import List, Union

import streamlit as st  # type: ignore
from streamlit_agraph import Config, Edge, Node, agraph  # type: ignore

from arti.artifacts import Artifact
from arti.artifacts import Statistic as Statistic  # noqa: F401
from arti.backends.memory import MemoryBackend
from arti.graphs import Graph
from arti.internal.models import Model
from arti.producers import Producer

# Replica colors
ARTIFACT_COLOR = "#ffcb23"
PRODUCER_COLOR = "#ff5c37"
EDGE_COLOR = "#6a4582"


class GraphDashboard(Model):
    """A GraphDashboard is a dashboard associated with a particular Graph."""

    graph: Graph

    def render(self) -> None:
        """Render Streamlit dashboard for a Graph"""

        pages = {
            "Dependency Graph": self.render_dependency_graph,
            "Artifact Details": self.render_artifact_details,
        }
        st.title(f"Artigraph Dashboard for {self.graph.name}")
        selection = st.sidebar.radio("Go to", list(pages.keys()))

        # TODO - switch to build-dry-run or sync once a real backend is implemented
        if isinstance(self.graph.backend, MemoryBackend):
            # First build graph for MemoryBackend implementation
            self.graph.build()
        else:
            raise NotImplementedError("Only MemoryBackend is supported at this time.")

        pages[selection]()

    def render_dependency_graph(self) -> None:
        """Render a panel showing Graph dependencies"""
        st.subheader("Dependency Graph")

        nodes: List[Node] = []  # Artifacts
        edges: List[Edge] = []  # Producers
        for node, deps in self.graph.dependencies.items():
            if isinstance(node, Producer):
                node_label = type(node).__name__
                node_id = self.producer_node_id(node)
                nodes.append(
                    Node(
                        id=node_id,
                        label=node_label,
                        size=1600,
                        color=PRODUCER_COLOR,
                        symbolType="square",
                        labelPosition="center",
                    )
                )
                edges.extend(
                    Edge(
                        source=self.graph.artifact_to_names[dep][0],
                        target=node_id,
                        label=[name for name, artifact in node.inputs.items() if artifact == dep][
                            0
                        ],
                        strokeWidth=2.5,
                        color=EDGE_COLOR,
                    )
                    for dep in deps
                )
            else:
                node_label = self.graph.artifact_to_names[node][0]
                node_id = node_label
                nodes.append(
                    Node(
                        id=node_id,
                        label=node_label,
                        size=1200,
                        color=ARTIFACT_COLOR,
                        labelPosition="center",
                    )
                )
                if node.producer_output:
                    edges.extend(
                        Edge(
                            source=self.producer_node_id(dep),
                            target=node_id,
                            label=f"Output [{node.producer_output.position}]",
                            strokeWidth=2.5,
                            color=EDGE_COLOR,
                        )
                        for dep in deps
                    )
        config = Config(
            directed=True,
            node={"labelProperty": "label"},
            link={"labelProperty": "label", "renderLabel": True},
            maxZoom=2,
            minZoom=0.1,
            height=500,
            width=500,
            staticGraphWithDragAndDrop=False,
            staticGraph=False,
            initialZoom=1,
            graphviz_layout="dot",
            graphviz_config={"rankdir": "BT"},
        )
        st.write(
            agraph(nodes=nodes, edges=edges, config=config),
        )

    def render_artifact_details(self) -> None:
        """Render a widget panel showing Graph dependencies"""
        st.subheader("Artifact Details")
        artifact_name_to_partitions = {}
        for artifact, names in self.graph.artifact_to_names.items():
            partitions = list(artifact.discover_storage_partitions())
            artifact_name_to_partitions[names[0]] = {
                partition.keys: partition.path for partition in partitions
            }
        artifact_chosen = st.selectbox(
            "Select an Artifact:", list(artifact_name_to_partitions.keys())
        )
        st.subheader("Available Partitions")
        partition_chosen = st.selectbox(
            "Select a Partition:", artifact_name_to_partitions[artifact_chosen]
        )
        st.subheader("Artifact-level Statistics")
        st.write(f"Stats for Artifact: {artifact_chosen}")
        st.subheader("Partition-level Statistics")
        st.write(f"Stats for Partition: {artifact_chosen}_{partition_chosen}")
        st.write(
            f"Partition path = {artifact_name_to_partitions[artifact_chosen][partition_chosen]}"
        )

    @staticmethod
    def producer_node_id(producer: Union[Artifact, Producer]) -> str:
        """Unique identifier for Producers

        While the class name is an appropriate label, there might be duplicates, so it cannot be used for the ID.
        """
        return f"{type(producer).__name__}_{str(producer.inputs.items())}"
