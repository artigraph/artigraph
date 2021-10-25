from __future__ import annotations

__path__ = __import__("pkgutil").extend_path(__path__, __name__)  # type: ignore

from datetime import datetime
from typing import List

import streamlit as st  # type: ignore

# from graphlib import TopologicalSorter
from streamlit_agraph import Config, Edge, Node, agraph  # type: ignore

from arti.artifacts import Statistic as Statistic  # noqa: F401
from arti.backends.memory import MemoryBackend
from arti.graphs import Graph
from arti.internal.models import Model
from arti.producers import Producer


class GraphDashboard(Model):
    """A GraphDashboard is a dashboard associated with a particular Graph."""

    graph: Graph

    def render(self) -> None:
        """Render Streamlit dashboard for a Graph"""

        # TODO - switch to build-dry-run or sync once a real backend is implemented
        if isinstance(self.graph.backend, MemoryBackend):
            # First build graph for MemoryBackend implementation
            self.graph.build()
        else:
            raise NotImplementedError("Only MemoryBackend is supported at this time.")

        st.title(f"Artigraph Dashboard for {self.graph.name}")
        st.subheader("Dependency Graph")
        st.write(f"Rendered on: {datetime.now().strftime('%m/%d/%Y, %H:%M:%S')}")

        nodes: List[Node] = []  # Artifacts
        edges: List[Edge] = []  # Producers

        for node, deps in self.graph.dependencies.items():
            if isinstance(node, Producer):
                nodes.append(Node(id=str(node), size=1200, color="#ACDBC9", symbolType="square"))
                edges.extend(
                    Edge(
                        source=str(dep),
                        label=[name for name, artifact in node.inputs.items() if artifact == dep][
                            0
                        ],
                        target=str(node),
                    )
                    for dep in deps
                )
            else:
                nodes.append(Node(id=str(node), size=800, color="#FFEB78"))
                if node.producer_output:
                    edges.extend(
                        Edge(
                            source=str(dep),
                            label=f"Output {node.producer_output.position}",
                            target=str(node),
                        )
                        for dep in deps
                    )
        config = Config(
            width=800,
            height=600,
            directed=True,
            nodeHighlightBehavior=True,
            highlightColor="#F7A7A6",  # or "blue"
            collapsible=True,
            node={"labelProperty": "label"},
            link={"labelProperty": "label", "renderLabel": True},
        )

        return_value = agraph(nodes=nodes, edges=edges, config=config)
        st.write(return_value)
        # self.render_dependencies_widget()
        #
        # for artifact in self.graph.artifacts:
        #     self.render_artifact_stats(artifact)

    # def render_dependencies_widget(self):
    #     """Render a widget panel showing Graph dependencies"""

    # @staticmethod
    # def render_artifact_stats(artifact: A):
    #     """Render a widget panel showing Artifact stats/thresholds"""
