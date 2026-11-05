// Types for the JSON documents served by `arti.dashboard` (see `src/arti/dashboard/_data.py`).
//
// Fingerprints are int64 values, which JavaScript numbers can't represent exactly, so the API sends
// them (and any other large ints) as strings.

export type Spec = { _arti_type_?: string; [field: string]: unknown };

export type GraphSummary = {
  name: string;
  versions: number;
  snapshots: number;
  tags: string[];
  defined: boolean;
};

export type GraphVersionSummary = {
  fingerprint: string;
  persisted: boolean;
  defined: boolean;
  artifacts: number;
  producers: number;
};

export type SnapshotSummary = {
  fingerprint: string;
  id: string;
  graph_fingerprint: string;
  tags: string[];
};

export type GraphDetail = {
  name: string;
  versions: GraphVersionSummary[];
  snapshots: SnapshotSummary[];
};

export type ArtifactNode = {
  id: string;
  key: string | null;
  class: string;
  module: string;
  description: string | null;
  fingerprint: string;
  produced: boolean;
  type: { summary: string; description: string | null; spec: Spec };
  format: Spec;
  storage: Spec;
  annotations: Spec[];
  statistics: Spec[];
};

export type ProducerNode = {
  id: string;
  class: string;
  module: string;
  description: string | null;
  fingerprint: string;
  version: { summary: string; spec: Spec };
  annotations: Spec[];
  inputs: Record<string, string>;
  outputs: string[];
};

export type Edge = { source: string; target: string; label: string | null };

export type GraphVersion = {
  name: string;
  fingerprint: string;
  path_tags: Record<string, string>;
  artifacts: ArtifactNode[];
  producers: ProducerNode[];
  edges: Edge[];
};

export type Partition = {
  fingerprint: string;
  content_fingerprint: string;
  input_fingerprint: string | null;
  partition_key: Record<string, Spec>;
  storage_partition: Spec;
};

export type Snapshot = GraphVersion & {
  snapshot: { fingerprint: string; id: string; tags: string[] };
  partitions: Record<string, Partition[]>;
};

export function isSnapshot(view: GraphVersion | Snapshot): view is Snapshot {
  return "snapshot" in view;
}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(`api/${path}`, { headers: { Accept: "application/json" } });
  const body = await response.json().catch(() => ({ error: response.statusText }));
  if (!response.ok) {
    throw new Error(body.error ?? `${response.status} ${response.statusText}`);
  }
  return body as T;
}

const segment = encodeURIComponent;

export const api = {
  graphs: () => get<{ graphs: GraphSummary[] }>("graphs").then((body) => body.graphs),
  graph: (name: string) => get<GraphDetail>(`graphs/${segment(name)}`),
  version: (name: string, fingerprint: string) =>
    get<GraphVersion>(`graphs/${segment(name)}/versions/${segment(fingerprint)}`),
  snapshot: (name: string, fingerprint: string) =>
    get<Snapshot>(`graphs/${segment(name)}/snapshots/${segment(fingerprint)}`),
};

/** Format a partition key as `name=value` pairs, eg: `date=2026-01-01`. */
export function formatPartitionKey(key: Record<string, Spec>): string {
  const parts = Object.entries(key).map(([name, field]) => {
    const values = Object.entries(field)
      .filter(([k]) => k !== "_arti_type_")
      .map(([, v]) => String(v));
    return `${name}=${values.join(",")}`;
  });
  return parts.length ? parts.join(" ") : "(not partitioned)";
}

/** Shorten a fingerprint for display; the full value is kept in titles and details. */
export function short(fingerprint: string): string {
  return fingerprint.length > 10 ? `${fingerprint.slice(0, 10)}…` : fingerprint;
}
