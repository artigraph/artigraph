import dagre from "@dagrejs/dagre";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  type Edge as FlowEdge,
  type Node as FlowNode,
  type NodeProps,
} from "@xyflow/react";
import { useEffect, useMemo, useRef } from "react";
import type { ArtifactNode, GraphVersion, Partition, ProducerNode } from "./api";

const SIZES = {
  artifact: { width: 230, height: 68 },
  producer: { width: 190, height: 48 },
};

type ArtifactData = { artifact: ArtifactNode; partitions?: Partition[] };
type ProducerData = { producer: ProducerNode };
type ArtifactFlowNode = FlowNode<ArtifactData, "artifact">;
type ProducerFlowNode = FlowNode<ProducerData, "producer">;
type GraphFlowNode = ArtifactFlowNode | ProducerFlowNode;

function ArtifactCard({ data, selected }: NodeProps<ArtifactFlowNode>) {
  const { artifact, partitions } = data;
  const kind = artifact.key === null ? "external" : artifact.produced ? "produced" : "raw";
  return (
    <div className={`node artifact ${kind}${selected ? " selected" : ""}`} title={artifact.id}>
      <Handle type="target" position={Position.Left} />
      <div className="node-title">{artifact.key ?? "(not in graph)"}</div>
      <div className="node-meta">
        <span>{artifact.class}</span>
        <span className="node-type">{artifact.type.summary}</span>
      </div>
      {partitions !== undefined && (
        <div className={`node-badge${partitions.length ? "" : " empty"}`}>
          {partitions.length} {partitions.length === 1 ? "partition" : "partitions"}
        </div>
      )}
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

function ProducerCard({ data, selected }: NodeProps<ProducerFlowNode>) {
  const { producer } = data;
  return (
    <div className={`node producer${selected ? " selected" : ""}`} title={producer.id}>
      <Handle type="target" position={Position.Left} />
      <div className="node-title">{producer.class}</div>
      <div className="node-meta" title={producer.version.summary}>
        {producer.version.summary}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

const nodeTypes = { artifact: ArtifactCard, producer: ProducerCard };

function layout(graph: GraphVersion, partitions?: Record<string, Partition[]>) {
  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: "LR", nodesep: 28, ranksep: 70, marginx: 20, marginy: 20 });
  g.setDefaultEdgeLabel(() => ({}));
  for (const artifact of graph.artifacts) g.setNode(artifact.id, { ...SIZES.artifact });
  for (const producer of graph.producers) g.setNode(producer.id, { ...SIZES.producer });
  for (const edge of graph.edges) g.setEdge(edge.source, edge.target);
  dagre.layout(g);

  const position = (id: string, size: { width: number; height: number }) => {
    const { x, y } = g.node(id);
    return { x: x - size.width / 2, y: y - size.height / 2 };
  };
  const nodes: GraphFlowNode[] = [
    ...graph.artifacts.map(
      (artifact): ArtifactFlowNode => ({
        id: artifact.id,
        type: "artifact",
        position: position(artifact.id, SIZES.artifact),
        data: {
          artifact,
          partitions:
            partitions && artifact.key !== null ? (partitions[artifact.key] ?? []) : undefined,
        },
      }),
    ),
    ...graph.producers.map(
      (producer): ProducerFlowNode => ({
        id: producer.id,
        type: "producer",
        position: position(producer.id, SIZES.producer),
        data: { producer },
      }),
    ),
  ];
  const edges: FlowEdge[] = graph.edges.map((edge) => ({
    id: `${edge.source}->${edge.target}:${edge.label ?? ""}`,
    source: edge.source,
    target: edge.target,
    label: edge.label ?? undefined,
    markerEnd: { type: MarkerType.ArrowClosed },
  }));
  return { nodes, edges };
}

export function GraphView({
  graph,
  partitions,
  selected,
  onSelect,
}: {
  graph: GraphVersion;
  partitions?: Record<string, Partition[]>;
  selected: string | null;
  onSelect: (id: string | null) => void;
}) {
  const { nodes, edges } = useMemo(() => layout(graph, partitions), [graph, partitions]);
  const shown = useMemo(
    () => nodes.map((node) => ({ ...node, selected: node.id === selected })),
    [nodes, selected],
  );
  // The details panel narrows the canvas, so refit the graph when it opens or closes.
  const flow = useRef<{ fitView: (options: { duration: number }) => Promise<boolean> } | null>(null);
  const detailsOpen = selected !== null;
  useEffect(() => {
    const timer = setTimeout(() => flow.current?.fitView({ duration: 300 }), 0);
    return () => clearTimeout(timer);
  }, [detailsOpen]);
  return (
    <ReactFlow
      // Remount (and refit) when switching between Graph versions or snapshots.
      key={graph.fingerprint}
      nodes={shown}
      edges={edges}
      nodeTypes={nodeTypes}
      colorMode="system"
      fitView
      onInit={(instance) => {
        flow.current = instance;
      }}
      minZoom={0.1}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable
      onNodeClick={(_, node) => onSelect(node.id)}
      onPaneClick={() => onSelect(null)}
    >
      <Background />
      <Controls showInteractive={false} />
    </ReactFlow>
  );
}
