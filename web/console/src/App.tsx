import { useCallback, useEffect, useRef, useState } from "react";
import { fetchNodes, fetchStatus, metric, parseMetrics, type NodeInfo, type Status } from "./api";
import { Overview } from "./Overview";
import { SqlShell } from "./SqlShell";
import { Statements } from "./Statements";

type Tab = "overview" | "sql" | "statements";
const TABS: { id: Tab; label: string }[] = [
  { id: "overview", label: "Cluster" },
  { id: "sql", label: "SQL" },
  { id: "statements", label: "Statements" },
];

function readHash(): Tab {
  const h = location.hash.replace("#", "");
  return h === "sql" || h === "statements" ? h : "overview";
}

export interface Live {
  status: Status | null;
  nodes: NodeInfo[] | null;
  error: string | null;
  at: number | null;
}

/** Poll /status and /api/nodes every two seconds while the tab is visible. */
function useLive(): Live {
  const [live, setLive] = useState<Live>({ status: null, nodes: null, error: null, at: null });
  useEffect(() => {
    let stop = false;
    let timer: number | undefined;
    const tick = async () => {
      try {
        const [status, nodes] = await Promise.all([fetchStatus(), fetchNodes()]);
        if (!stop) setLive({ status, nodes, error: null, at: Date.now() });
      } catch (e) {
        if (!stop) setLive((l) => ({ ...l, error: e instanceof Error ? e.message : String(e) }));
      }
      if (!stop) timer = window.setTimeout(tick, document.hidden ? 10000 : 2000);
    };
    void tick();
    return () => {
      stop = true;
      window.clearTimeout(timer);
    };
  }, []);
  return live;
}

function useNow(ms: number) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = window.setInterval(() => setNow(Date.now()), ms);
    return () => window.clearInterval(t);
  }, [ms]);
  return now;
}

export default function App() {
  const [tab, setTab] = useState<Tab>(readHash);
  const live = useLive();
  const now = useNow(1000);
  const tabRefs = useRef<(HTMLButtonElement | null)[]>([]);

  useEffect(() => {
    const on = () => setTab(readHash());
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  const go = useCallback((t: Tab) => {
    history.replaceState(null, "", `#${t}`);
    setTab(t);
  }, []);

  const onTabKey = (e: React.KeyboardEvent, i: number) => {
    if (e.key !== "ArrowRight" && e.key !== "ArrowLeft") return;
    const j = (i + (e.key === "ArrowRight" ? 1 : TABS.length - 1)) % TABS.length;
    go(TABS[j].id);
    tabRefs.current[j]?.focus();
  };

  const age = live.at ? (now - live.at) / 1000 : null;
  const stale = age !== null && age > 6;

  return (
    <div className="sheet">
      <div className="tractor left" aria-hidden="true" />
      <div className="content">
        <header className="carriage">
          <div className="wordmark">
            <b>Stowage</b>
            <span>{live.status ? `via n${live.status.node}` : "console"}</span>
          </div>
          <nav className="tabs" role="tablist" aria-label="Console views">
            {TABS.map((t, i) => (
              <button
                key={t.id}
                ref={(el) => {
                  tabRefs.current[i] = el;
                }}
                role="tab"
                className="tab"
                aria-selected={tab === t.id}
                tabIndex={tab === t.id ? 0 : -1}
                onClick={() => go(t.id)}
                onKeyDown={(e) => onTabKey(e, i)}
              >
                {t.label}
              </button>
            ))}
          </nav>
          <NodeCells live={live} />
          <div className={`age${stale || live.error ? " stale" : ""}`} aria-live="polite">
            {live.error ? `unreachable · ${live.error}` : age === null ? "reading…" : `read ${age.toFixed(0)} s ago`}
          </div>
        </header>
        <main>
          {tab === "overview" && <Overview live={live} />}
          {tab === "sql" && <SqlShell />}
          {tab === "statements" && <Statements />}
        </main>
      </div>
      <div className="tractor right" aria-hidden="true" />
    </div>
  );
}

function NodeCells({ live }: { live: Live }) {
  if (!live.nodes) return <div className="nodes" />;
  return (
    <div className="nodes" aria-label="Nodes">
      {live.nodes.map((n) => {
        const m = n.metrics ? parseMetrics(n.metrics) : [];
        const leaders = metric(m, "stowage_range_leaders");
        return (
          <div key={n.id} className={`nodecell${n.up ? "" : " down"}`} title={`${n.addr || "this node"} · rtt: time to fetch its /metrics from the node serving this page`}>
            <b>n{n.id}</b>
            <span className="st">{n.up ? "up" : "down"}</span>
            <small>
              {n.up ? (
                <>
                  <span className="lg">{`${leaders ?? "–"} leading · ${n.ms.toFixed(0)} ms rtt`}</span>
                  <span className="sm">{`${leaders ?? "–"} L · ${n.ms.toFixed(0)} ms`}</span>
                </>
              ) : (
                "no answer"
              )}
            </small>
          </div>
        );
      })}
    </div>
  );
}
