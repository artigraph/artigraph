import type { ReactNode } from "react";
import {
  formatPartitionKey,
  short,
  type ArtifactNode,
  type GraphVersion,
  type Partition,
  type ProducerNode,
  type Spec,
} from "./api";

function Json({ value }: { value: unknown }) {
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>;
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <>
      <dt>{label}</dt>
      <dd>{children}</dd>
    </>
  );
}

function Fingerprint({ value }: { value: string | null }) {
  return value === null ? <span className="muted">none</span> : <code title={value}>{value}</code>;
}

function SpecDetails({ label, spec }: { label: string; spec: Spec }) {
  return (
    <Field label={label}>
      <details>
        <summary>{spec._arti_type_ ?? "spec"}</summary>
        <Json value={spec} />
      </details>
    </Field>
  );
}

/** Pick a human friendly location (eg: a file path) for a partition, if its Storage has one. */
function location(partition: Partition): string {
  const { path, id } = partition.storage_partition;
  if (typeof path === "string") return path;
  if (typeof id === "string") return id;
  return partition.storage_partition._arti_type_ ?? "";
}

/** Keep the end of long paths, which identifies the partition (eg: `…/2026-01-01/clean.parquet`). */
function tail(location: string, segments = 3): string {
  const parts = location.split("/");
  return parts.length > segments ? `…/${parts.slice(-segments).join("/")}` : location;
}

function Partitions({ partitions }: { partitions: Partition[] }) {
  if (!partitions.length) {
    return <p className="muted">No partitions are linked to this Artifact in this snapshot.</p>;
  }
  return (
    <table className="partitions">
      <thead>
        <tr>
          <th>Partition key</th>
          <th>Location</th>
          <th>Content</th>
          <th>Input</th>
        </tr>
      </thead>
      <tbody>
        {partitions.map((partition) => (
          <tr key={partition.fingerprint}>
            <td className="wrap">{formatPartitionKey(partition.partition_key)}</td>
            <td className="wrap" title={location(partition)}>
              {tail(location(partition))}
            </td>
            <td title={partition.content_fingerprint}>
              <code>{short(partition.content_fingerprint)}</code>
            </td>
            <td title={partition.input_fingerprint ?? "raw data"}>
              {partition.input_fingerprint === null ? (
                <span className="muted">raw</span>
              ) : (
                <code>{short(partition.input_fingerprint)}</code>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function ArtifactDetails({
  artifact,
  graph,
  partitions,
}: {
  artifact: ArtifactNode;
  graph: GraphVersion;
  partitions?: Partition[];
}) {
  const producer = graph.producers.find((p) => p.outputs.includes(artifact.id));
  const consumers = graph.producers.filter((p) => Object.values(p.inputs).includes(artifact.id));
  return (
    <>
      <h2>{artifact.key ?? "Artifact outside the Graph"}</h2>
      <p className="subtitle">
        {artifact.module}.{artifact.class}
      </p>
      {artifact.description && <p className="description">{artifact.description}</p>}
      <dl>
        <Field label="Kind">
          {artifact.key === null ? "external input" : artifact.produced ? "produced" : "raw"}
        </Field>
        <Field label="Produced by">{producer ? producer.class : <span className="muted">-</span>}</Field>
        <Field label="Used by">
          {consumers.length ? (
            consumers.map((p) => p.class).join(", ")
          ) : (
            <span className="muted">-</span>
          )}
        </Field>
        <Field label="Type">
          {artifact.type.summary}
          {artifact.type.description && <div className="muted">{artifact.type.description}</div>}
        </Field>
        <SpecDetails label="Type spec" spec={artifact.type.spec} />
        <SpecDetails label="Format" spec={artifact.format} />
        <SpecDetails label="Storage" spec={artifact.storage} />
        <Field label="Annotations">
          {artifact.annotations.length ? <Json value={artifact.annotations} /> : <span className="muted">none</span>}
        </Field>
        <Field label="Statistics">
          {artifact.statistics.length ? <Json value={artifact.statistics} /> : <span className="muted">none</span>}
        </Field>
        <Field label="Fingerprint">
          <Fingerprint value={artifact.fingerprint} />
        </Field>
      </dl>
      {partitions !== undefined && (
        <>
          <h3>Partitions in this snapshot ({partitions.length})</h3>
          <Partitions partitions={partitions} />
        </>
      )}
    </>
  );
}

function ProducerDetails({ producer, graph }: { producer: ProducerNode; graph: GraphVersion }) {
  const name = (id: string) => graph.artifacts.find((a) => a.id === id)?.key ?? id;
  return (
    <>
      <h2>{producer.class}</h2>
      <p className="subtitle">
        {producer.module}.{producer.class}
      </p>
      {producer.description && <p className="description">{producer.description}</p>}
      <dl>
        <Field label="Version">{producer.version.summary}</Field>
        <Field label="Inputs">
          <ul>
            {Object.entries(producer.inputs).map(([param, id]) => (
              <li key={param}>
                <code>{param}</code> ← {name(id)}
              </li>
            ))}
          </ul>
        </Field>
        <Field label="Outputs">
          <ul>
            {producer.outputs.map((id) => (
              <li key={id}>{name(id)}</li>
            ))}
          </ul>
        </Field>
        <Field label="Annotations">
          {producer.annotations.length ? <Json value={producer.annotations} /> : <span className="muted">none</span>}
        </Field>
        <Field label="Fingerprint">
          <Fingerprint value={producer.fingerprint} />
        </Field>
      </dl>
    </>
  );
}

export function Details({
  graph,
  partitions,
  selected,
  onClose,
}: {
  graph: GraphVersion;
  partitions?: Record<string, Partition[]>;
  selected: string;
  onClose: () => void;
}) {
  const artifact = graph.artifacts.find((a) => a.id === selected);
  const producer = graph.producers.find((p) => p.id === selected);
  return (
    <aside className="details">
      <button className="close" onClick={onClose} aria-label="Close details">
        ×
      </button>
      {artifact && (
        <ArtifactDetails
          artifact={artifact}
          graph={graph}
          partitions={partitions && artifact.key !== null ? (partitions[artifact.key] ?? []) : undefined}
        />
      )}
      {producer && <ProducerDetails producer={producer} graph={graph} />}
    </aside>
  );
}
