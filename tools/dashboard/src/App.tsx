import { useCallback, useEffect, useState } from "react";
import { Details } from "./Details";
import { GraphView } from "./GraphView";
import {
  api,
  isSnapshot,
  short,
  type GraphDetail,
  type GraphSummary,
  type GraphVersion,
  type Snapshot,
} from "./api";

// The location hash holds the selection, eg: `#/spend/snapshots/123`, so views can be bookmarked
// and shared, and the server only needs to serve one page.
type Route = { graph?: string; kind?: "versions" | "snapshots"; fingerprint?: string };

function parseHash(hash: string): Route {
  const [graph, kind, fingerprint] = hash.replace(/^#\/?/, "").split("/").map(decodeURIComponent);
  return {
    graph: graph || undefined,
    kind: kind === "versions" || kind === "snapshots" ? kind : undefined,
    fingerprint: fingerprint || undefined,
  };
}

function toHash({ graph, kind, fingerprint }: Route): string {
  return "#/" + [graph, kind, fingerprint].filter(Boolean).map((s) => encodeURIComponent(s!)).join("/");
}

function useRoute(): [Route, (route: Route) => void] {
  const [route, setRoute] = useState(() => parseHash(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parseHash(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  const navigate = useCallback((next: Route) => {
    window.location.hash = toHash(next);
  }, []);
  return [route, navigate];
}

type Loaded<T> = { status: "loading" } | { status: "error"; error: string } | { status: "ok"; value: T };

/** Load `fetcher()` whenever `key` changes, keeping errors visible rather than swallowing them. */
function useLoad<T>(key: string | null, fetcher: () => Promise<T>, refresh: number): Loaded<T> | null {
  const [state, setState] = useState<Loaded<T> | null>(null);
  useEffect(() => {
    if (key === null) {
      setState(null);
      return;
    }
    let current = true;
    setState((previous) => (previous?.status === "ok" ? previous : { status: "loading" }));
    fetcher().then(
      (value) => current && setState({ status: "ok", value }),
      (error: unknown) => current && setState({ status: "error", error: String(error instanceof Error ? error.message : error) }),
    );
    return () => {
      current = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- `key` identifies the `fetcher`.
  }, [key, refresh]);
  return state;
}

function Problem({ state }: { state: Loaded<unknown> | null }) {
  if (state?.status === "loading") return <p className="muted pad">Loading…</p>;
  if (state?.status === "error") return <p className="error pad">{state.error}</p>;
  return null;
}

function Tags({ tags }: { tags: string[] }) {
  return (
    <>
      {tags.map((tag) => (
        <span key={tag} className="tag">
          {tag}
        </span>
      ))}
    </>
  );
}

function GraphList({
  graphs,
  selected,
  onSelect,
}: {
  graphs: GraphSummary[];
  selected?: string;
  onSelect: (name: string) => void;
}) {
  if (!graphs.length) {
    return <p className="muted pad">This Backend has no Graphs yet. Snapshot a Graph to see it here.</p>;
  }
  return (
    <ul className="list">
      {graphs.map((graph) => (
        <li key={graph.name}>
          <button className={graph.name === selected ? "active" : ""} onClick={() => onSelect(graph.name)}>
            <span className="list-title">{graph.name}</span>
            <span className="list-meta">
              {graph.versions} {graph.versions === 1 ? "version" : "versions"} · {graph.snapshots}{" "}
              {graph.snapshots === 1 ? "snapshot" : "snapshots"}
              {graph.defined && " · defined in code"}
            </span>
            <span>
              <Tags tags={graph.tags} />
            </span>
          </button>
        </li>
      ))}
    </ul>
  );
}

/** Pick what to show when a Graph is opened without a selection. */
function defaultSelection(detail: GraphDetail): Route {
  const defined = detail.versions.find((v) => v.defined);
  if (defined) return { graph: detail.name, kind: "versions", fingerprint: defined.fingerprint };
  const snapshot = detail.snapshots.find((s) => s.tags.length) ?? detail.snapshots[0];
  if (snapshot) return { graph: detail.name, kind: "snapshots", fingerprint: snapshot.fingerprint };
  return { graph: detail.name, kind: "versions", fingerprint: detail.versions[0]?.fingerprint };
}

function GraphMenu({ detail, route, navigate }: { detail: GraphDetail; route: Route; navigate: (r: Route) => void }) {
  const isActive = (kind: Route["kind"], fingerprint: string) =>
    route.kind === kind && route.fingerprint === fingerprint ? "active" : "";
  return (
    <>
      <h3 className="section">Snapshots</h3>
      {detail.snapshots.length ? (
        <ul className="list">
          {detail.snapshots.map((snapshot) => (
            <li key={snapshot.fingerprint}>
              <button
                className={isActive("snapshots", snapshot.fingerprint)}
                title={`Snapshot ${snapshot.fingerprint} of Graph version ${snapshot.graph_fingerprint}`}
                onClick={() => navigate({ graph: detail.name, kind: "snapshots", fingerprint: snapshot.fingerprint })}
              >
                <span className="list-title">
                  <code>{short(snapshot.fingerprint)}</code> <Tags tags={snapshot.tags} />
                </span>
                <span className="list-meta">version {short(snapshot.graph_fingerprint)}</span>
              </button>
            </li>
          ))}
        </ul>
      ) : (
        <p className="muted pad">No snapshots.</p>
      )}
      <h3 className="section">Versions</h3>
      <ul className="list">
        {detail.versions.map((version) => (
          <li key={version.fingerprint}>
            <button
              className={isActive("versions", version.fingerprint)}
              title={`Graph version ${version.fingerprint}`}
              onClick={() => navigate({ graph: detail.name, kind: "versions", fingerprint: version.fingerprint })}
            >
              <span className="list-title">
                <code>{short(version.fingerprint)}</code>
              </span>
              <span className="list-meta">
                {version.artifacts} artifacts · {version.producers} producers
                {version.defined && " · defined in code"}
                {!version.persisted && " · not persisted"}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}

export function App() {
  const [route, navigate] = useRoute();
  const [refresh, setRefresh] = useState(0);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);

  const graphs = useLoad("graphs", api.graphs, refresh);
  const detail = useLoad(route.graph ?? null, () => api.graph(route.graph!), refresh);
  const viewKey = route.graph && route.kind && route.fingerprint ? toHash(route) : null;
  const view = useLoad<GraphVersion | Snapshot>(
    viewKey,
    () =>
      route.kind === "snapshots"
        ? api.snapshot(route.graph!, route.fingerprint!)
        : api.version(route.graph!, route.fingerprint!),
    refresh,
  );

  // Open a sensible default when a Graph is chosen without a version or snapshot.
  useEffect(() => {
    if (detail?.status === "ok" && detail.value.name === route.graph && !route.fingerprint) {
      const next = defaultSelection(detail.value);
      if (next.fingerprint) navigate(next);
    }
  }, [detail, route, navigate]);
  useEffect(() => setSelectedNode(null), [viewKey]);

  const shown = view?.status === "ok" && viewKey === toHash(route) ? view.value : null;
  const partitions = shown && isSnapshot(shown) ? shown.partitions : undefined;

  return (
    <div className="app">
      <header>
        <a className="brand" href="#/">
          Artigraph
        </a>
        <nav className="crumbs">
          {route.graph && <span>{route.graph}</span>}
          {route.kind && route.fingerprint && (
            <span>
              {route.kind === "snapshots" ? "snapshot" : "version"} <code>{route.fingerprint}</code>
              {shown && isSnapshot(shown) && <Tags tags={shown.snapshot.tags} />}
            </span>
          )}
        </nav>
        <button className="refresh" onClick={() => setRefresh((n) => n + 1)} title="Reload from the Backend">
          Refresh
        </button>
      </header>
      <nav className="sidebar">
        <h3 className="section">Graphs</h3>
        <Problem state={graphs} />
        {graphs?.status === "ok" && (
          <GraphList graphs={graphs.value} selected={route.graph} onSelect={(graph) => navigate({ graph })} />
        )}
        {route.graph && <Problem state={detail} />}
        {route.graph && detail?.status === "ok" && <GraphMenu detail={detail.value} route={route} navigate={navigate} />}
      </nav>
      <main>
        {!route.graph && <p className="muted pad">Choose a Graph to see its Artifacts and Producers.</p>}
        {viewKey && !shown && <Problem state={view} />}
        {shown && (
          <>
            {view?.status === "error" && <p className="error banner">{view.error}</p>}
            <GraphView graph={shown} partitions={partitions} selected={selectedNode} onSelect={setSelectedNode} />
            <div className="legend">
              <span className="swatch raw" /> raw
              <span className="swatch produced" /> produced
              <span className="swatch producer" /> producer
              {partitions && <span className="muted"> · badges count the partitions in this snapshot</span>}
            </div>
          </>
        )}
      </main>
      {shown && selectedNode && (
        <Details graph={shown} partitions={partitions} selected={selectedNode} onClose={() => setSelectedNode(null)} />
      )}
    </div>
  );
}
