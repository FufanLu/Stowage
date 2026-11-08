import { Fragment, useMemo, useState } from "react";
import { describeKey, fmtBytes, rid, ridTitle, histogramQuantile, metric, parseMetrics, type Range, type Region } from "./api";
import type { Live } from "./App";

const REGION_NAME: Record<Region, string> = {
  start: "start",
  catalog: "catalog 0x01",
  sql: "sql rows 0x02",
  doc: "documents 0x03",
  ledger: "gsi ledger 0x04",
  end: "end",
  other: "other",
};

export function Overview({ live }: { live: Live }) {
  const [picked, setPicked] = useState<number | null>(null);
  const ranges = useMemo(
    () => [...(live.status?.ranges ?? [])].sort((a, b) => a.start_hex.localeCompare(b.start_hex)),
    [live.status],
  );

  if (!live.status) {
    return (
      <section className="block">
        {live.error ? (
          <div className="banner">
            <b>Cannot read /status.</b> {live.error}. The console talks to the stowd admin listener that served it
            (<code>--http-listen</code>); check that the node is running.
          </div>
        ) : (
          <div className="skeleton" aria-label="Loading cluster state" />
        )}
      </section>
    );
  }

  const leaderless = ranges.filter((r) => r.leader === null);
  const down = (live.nodes ?? []).filter((n) => !n.up);

  return (
    <>
      {(leaderless.length > 0 || down.length > 0) && (
        <div className="banner" role="status">
          {leaderless.length > 0 && (
            <>
              <b>
                {leaderless.length} of {ranges.length} ranges have no leader
              </b>{" "}
              ({leaderless.map((r) => rid(r.id)).join(", ")}): writes to them wait for an election.{" "}
            </>
          )}
          {down.length > 0 && (
            <>
              <b>n{down.map((n) => n.id).join(", n")} not answering.</b> Ranges keep a quorum while two of three
              replicas are up. See <code>docs/runbooks/node-down.md</code>.
            </>
          )}
        </div>
      )}

      <section className="block" aria-labelledby="ks">
        <div className="block-head">
          <h2 id="ks">Key space</h2>
          <span className="source">
            {ranges.length} ranges · {fmtBytes(ranges.reduce((s, r) => s + r.bytes, 0))} · widths by size · <code>/status</code> from n{live.status.node}
          </span>
        </div>
        <KeyStrip ranges={ranges} picked={picked} onPick={setPicked} />
      </section>

      <section className="block" aria-labelledby="ld">
        <div className="block-head">
          <h2 id="ld">Leaders by node</h2>
          <span className="source">
            replica sets from <code>/status</code> · counters from each node's <code>/metrics</code>, since it started
          </span>
        </div>
        <LeaderMatrix ranges={ranges} live={live} picked={picked} onPick={setPicked} />
      </section>

      <hr className="perforation" />

      <section className="block" aria-labelledby="rl">
        <div className="block-head">
          <h2 id="rl">Ranges</h2>
          <span className="source">select a row for its span and replicas</span>
        </div>
        <RangeListing ranges={ranges} picked={picked} onPick={setPicked} />
      </section>
    </>
  );
}

/** Strip widths follow size (square root, so a 400 B range stays visible next to 100 KiB). */
function weight(r: Range) {
  return Math.sqrt(Math.max(r.bytes, 256));
}

function KeyStrip({ ranges, picked, onPick }: { ranges: Range[]; picked: number | null; onPick: (id: number | null) => void }) {
  // Group consecutive ranges by the region their start key falls in.
  const groups: { region: Region; weight: number; count: number }[] = [];
  for (const r of ranges) {
    const region = describeKey(r.start_hex).region;
    const g = groups.at(-1);
    if (g && g.region === region) {
      g.weight += weight(r);
      g.count++;
    } else groups.push({ region, weight: weight(r), count: 1 });
  }
  return (
    <div className="strip-wrap">
      <div className="regions" aria-hidden="true">
        {groups.map((g, i) => (
          <div key={i} className="region" style={{ flex: `${g.weight} 1 0`, minWidth: g.count * 74 }}>
            {REGION_NAME[g.region]}
          </div>
        ))}
      </div>
      <div className="strip" role="group" aria-label="Ranges in key order">
        {ranges.map((r) => {
          const k = describeKey(r.start_hex);
          return (
            <button
              key={r.id}
              className={`span${r.leader === null ? " fault" : ""}`}
              style={{ flex: `${weight(r)} 1 0` }}
              aria-pressed={picked === r.id}
              onClick={() => onPick(picked === r.id ? null : r.id)}
              title={`${ridTitle(r.id)} · [${r.start_hex || "−∞"}, ${r.end_hex || "+∞"}) · ${fmtBytes(r.bytes)}`}
            >
              <span className="rid">{rid(r.id)}</span>
              <span className={`who${r.leader === null ? " none" : ""}`}>{r.leader === null ? "no leader" : `n${r.leader}`}</span>
              <span className="what">{k.text}</span>
              <span className="size">{fmtBytes(r.bytes)}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function LeaderMatrix({
  ranges,
  live,
  picked,
  onPick,
}: {
  ranges: Range[];
  live: Live;
  picked: number | null;
  onPick: (id: number | null) => void;
}) {
  const ids = new Set<number>();
  for (const r of ranges) r.replicas.forEach((n) => ids.add(n));
  for (const n of live.nodes ?? []) ids.add(n.id);
  const nodeIds = [...ids].sort((a, b) => a - b);
  const byId = new Map((live.nodes ?? []).map((n) => [n.id, n]));

  return (
    <>
      <div className="matrix-wrap">
        <table className="matrix">
          <thead>
            <tr>
              <th scope="col" />
              {ranges.map((r) => (
                <th key={r.id} scope="col" title={ridTitle(r.id)} style={picked === r.id ? { color: "var(--ink)", fontWeight: 700 } : undefined}>
                  {rid(r.id)}
                </th>
              ))}
              <th className="stat" scope="col">leading</th>
              <th className="stat" scope="col">replicas</th>
              <th className="stat" scope="col">queued</th>
              <th className="stat" scope="col">fsync p99</th>
              <th className="stat" scope="col">kv requests</th>
            </tr>
          </thead>
          <tbody>
            {nodeIds.map((id) => {
              const n = byId.get(id);
              const m = n?.metrics ? parseMetrics(n.metrics) : [];
              const p99 = histogramQuantile(m, "stowage_raft_log_sync_seconds", 0.99);
              const leading = ranges.filter((r) => r.leader === id).length;
              const up = n?.up ?? false;
              return (
                <tr key={id} className={up ? "" : "down"}>
                  <th scope="row">
                    n{id}
                    {!up && " · down"}
                  </th>
                  {ranges.map((r) => {
                    const has = r.replicas.includes(id);
                    const leads = r.leader === id;
                    return (
                      <td key={r.id} className="cell" onClick={() => onPick(r.id)}>
                        {leads ? (
                          <span className="cellmark lead" aria-label={`leads ${rid(r.id)}`}>L</span>
                        ) : has ? (
                          <span className={`cellmark ${r.leader === null ? "orphan" : "rep"}`} aria-label={`replica of ${rid(r.id)}`}>
                            ·
                          </span>
                        ) : null}
                      </td>
                    );
                  })}
                  <td className="stat">{leading}</td>
                  <td className="stat">{ranges.filter((r) => r.replicas.includes(id)).length}</td>
                  <td className="stat">{up ? metric(m, "stowage_kv_queued_requests") ?? "–" : "–"}</td>
                  <td className="stat" title="upper bound of the histogram bucket, since the node started">
                    {p99 === null ? "–" : p99 === Infinity ? "> 10 s" : `≤ ${(p99 * 1000).toFixed(p99 < 0.001 ? 1 : 0)} ms`}
                  </td>
                  <td className="stat">{up ? (metric(m, "stowage_kv_requests_total") ?? 0).toLocaleString() : "–"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <div className="legend">
        <span>
          <span className="cellmark lead">L</span>leader: serves reads, orders writes
        </span>
        <span>
          <span className="cellmark rep">·</span>follower replica
        </span>
        <span>
          <span className="cellmark orphan">·</span>replica of a range with no leader
        </span>
      </div>
    </>
  );
}

function RangeListing({ ranges, picked, onPick }: { ranges: Range[]; picked: number | null; onPick: (id: number | null) => void }) {
  return (
    <div className="listing-wrap">
      <table className="listing pairs">
        <thead>
          <tr>
            <th className="num">range</th>
            <th>start</th>
            <th className="hide-sm">end</th>
            <th>leader</th>
            <th className="num">term</th>
            <th className="hide-sm">replicas</th>
            <th className="num hide-sm">generation</th>
            <th className="num">size</th>
          </tr>
        </thead>
        <tbody>
          {ranges.map((r) => {
            const open = picked === r.id;
            return (
              <Fragment key={r.id}>
                <tr
                  className="row"
                  aria-expanded={open}
                  tabIndex={0}
                  onClick={() => onPick(open ? null : r.id)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      onPick(open ? null : r.id);
                    }
                  }}
                >
                  <td className="num" title={ridTitle(r.id)}>{rid(r.id)}</td>
                  <td className="text">{describeKey(r.start_hex).text}</td>
                  <td className="text hide-sm">{r.end_hex ? describeKey(r.end_hex).text : "+∞"}</td>
                  <td>{r.leader === null ? <span className="fault-ink">none</span> : <span className="lead-ink">n{r.leader}</span>}</td>
                  <td className="num">{r.term}</td>
                  <td className="hide-sm">{r.replicas.map((n) => `n${n}`).join(" ")}</td>
                  <td className="num hide-sm">{r.generation}</td>
                  <td className="num">{fmtBytes(r.bytes)}</td>
                </tr>
                {open && (
                  <tr className="detail">
                    <td colSpan={8}>
                      <dl className="mono">
                        <dt>range id</dt>
                        <dd>
                          {r.id}
                          {r.id >= 2 ** 40 && <span className="dim"> · {ridTitle(r.id).replace(/^range \d+: /, "")}</span>}
                        </dd>
                        <dt>start key</dt>
                        <dd>{r.start_hex ? spaced(r.start_hex) : "(empty: the beginning of the key space)"}</dd>
                        <dt>end key</dt>
                        <dd>{r.end_hex ? spaced(r.end_hex) : "(empty: the end of the key space)"}</dd>
                        <dt>replicas</dt>
                        <dd className="replicas">
                          {r.replicas.map((n) => (
                            <span key={n} className={n === r.leader ? "lead-ink" : undefined}>
                              n{n}
                              {n === r.leader ? " (leader)" : ""}
                            </span>
                          ))}
                        </dd>
                        <dt>term</dt>
                        <dd>
                          {r.term} <span className="dim">· elections this range has seen, roughly</span>
                        </dd>
                        <dt>generation</dt>
                        <dd>
                          {r.generation} <span className="dim">· bumps on every split and replica change</span>
                        </dd>
                      </dl>
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function spaced(hex: string) {
  return hex.match(/../g)?.join(" ") ?? hex;
}
