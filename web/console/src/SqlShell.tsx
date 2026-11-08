import { useEffect, useRef, useState } from "react";
import { HttpError, fmtMs, login, parsePlan, runSql, type Credentials, type PlanNode, type SqlResponse, type SqlResult } from "./api";

const DESTRUCTIVE = /^\s*(drop|delete|update|truncate|alter|revoke)\b/im;
const EXAMPLE = "SELECT table_name, column_name, data_type\nFROM information_schema.columns\nORDER BY table_name;";

function load<T>(key: string, fallback: T): T {
  try {
    const v = sessionStorage.getItem(key);
    return v === null ? fallback : (JSON.parse(v) as T);
  } catch {
    return fallback;
  }
}
function save(key: string, v: unknown) {
  try {
    sessionStorage.setItem(key, JSON.stringify(v));
  } catch {
    /* private mode: keep it in memory only */
  }
}

interface Run {
  sql: string;
  response: SqlResponse | null;
  failure: string | null;
}

export function SqlShell() {
  const [sql, setSql] = useState<string>(() => load("stowage.sql", EXAMPLE));
  const [cred, setCred] = useState<Credentials>(() => login.current ?? { user: load("stowage.user", ""), password: "" });
  useEffect(() => {
    login.current = cred.user ? cred : null;
  }, [cred]);
  const [needAuth, setNeedAuth] = useState(false);
  const [busy, setBusy] = useState(false);
  const [run, setRun] = useState<Run | null>(null);
  const [armed, setArmed] = useState(false);
  const [history, setHistory] = useState<string[]>(() => load("stowage.history", []));
  const area = useRef<HTMLTextAreaElement>(null);

  useEffect(() => save("stowage.sql", sql), [sql]);
  useEffect(() => setArmed(false), [sql]);
  useEffect(() => {
    if (!armed) return;
    const t = window.setTimeout(() => setArmed(false), 4000);
    return () => window.clearTimeout(t);
  }, [armed]);

  const destructive = DESTRUCTIVE.test(sql);

  async function execute(text: string, force = false) {
    if (!text.trim() || busy) return;
    if (DESTRUCTIVE.test(text) && !force && !armed) {
      setArmed(true);
      return;
    }
    setArmed(false);
    setBusy(true);
    try {
      const response = await runSql(text, cred.user ? cred : null);
      setRun({ sql: text, response, failure: null });
      setNeedAuth(false);
      const h = [text.trim(), ...history.filter((x) => x !== text.trim())].slice(0, 12);
      setHistory(h);
      save("stowage.history", h);
    } catch (e) {
      if (e instanceof HttpError && e.status === 401) setNeedAuth(true);
      setRun({ sql: text, response: null, failure: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy(false);
    }
  }

  const explain = (analyze: boolean) => {
    const body = sql.trim().replace(/^explain\s+(analyze\s+|verbose\s+)?/i, "");
    void execute(`EXPLAIN ${analyze ? "ANALYZE " : ""}${body}`, true);
  };

  return (
    <section className="block" aria-labelledby="sqlh">
      <div className="block-head">
        <h2 id="sqlh">SQL</h2>
        <span className="source">
          <code>POST /api/sql</code> · one session per run, autocommit
        </span>
      </div>
      <div className="editor">
        <label htmlFor="sql" className="sr-only">
          SQL statements
        </label>
        <textarea
          id="sql"
          ref={area}
          value={sql}
          spellCheck={false}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={(e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
              e.preventDefault();
              void execute(sql);
            }
          }}
        />
        <div className="toolbar">
          <button className={`btn ${armed ? "armed" : "primary"}`} onClick={() => void execute(sql)} disabled={busy || !sql.trim()}>
            {busy ? "Running…" : armed ? "Press again to run" : "Run"}
          </button>
          <button className="btn" onClick={() => explain(false)} disabled={busy || !sql.trim()}>
            Explain
          </button>
          <button className="btn" onClick={() => explain(true)} disabled={busy || !sql.trim() || destructive}>
            Explain analyze
          </button>
          <span className="kbd">⌘/Ctrl + Enter</span>
          {armed && <span className="guard">This changes or removes data. Run again within 4 s to confirm.</span>}
          <span className="spacer" />
          <div className="cred">
            <label htmlFor="u">user</label>
            <input
              id="u"
              autoComplete="username"
              value={cred.user}
              placeholder="none"
              onChange={(e) => {
                setCred({ ...cred, user: e.target.value });
                save("stowage.user", e.target.value);
              }}
            />
            <label htmlFor="p">password</label>
            <input
              id="p"
              type="password"
              autoComplete="current-password"
              value={cred.password}
              onChange={(e) => setCred({ ...cred, password: e.target.value })}
              aria-invalid={needAuth}
            />
          </div>
        </div>
      </div>

      {needAuth && (
        <div className="error" role="alert">
          <code>28P01</code>This cluster has users, so the console needs one. Enter a user and password that can log in
          (the same ones <code>psql</code> uses) and run again.
        </div>
      )}
      {run?.failure && !needAuth && (
        <div className="error" role="alert">
          {run.failure}
        </div>
      )}
      {run?.response && <Response r={run.response} />}

      {history.length > 0 && (
        <div className="history">
          <div className="block-head">
            <h2>Recent</h2>
            <span className="source">this browser tab only</span>
          </div>
          {history.map((h) => (
            <button key={h} onClick={() => setSql(h)} title={h}>
              {h.replace(/\s+/g, " ")}
            </button>
          ))}
        </div>
      )}
    </section>
  );
}

function Response({ r }: { r: SqlResponse }) {
  return (
    <>
      {(r.results ?? []).map((res, i) => (
        <ResultBlock key={i} res={res} ms={i === (r.results?.length ?? 0) - 1 ? r.ms : undefined} />
      ))}
      {r.error && (
        <div className="error" role="alert">
          <code>{r.error.code}</code>
          {r.error.message}
        </div>
      )}
    </>
  );
}

function ResultBlock({ res, ms }: { res: SqlResult; ms?: number }) {
  if (res.command) {
    return (
      <div className="result">
        <div className="result-head">
          <b>{res.command}</b>
          {ms !== undefined && <span>{fmtMs(ms)}</span>}
        </div>
      </div>
    );
  }
  const cols = res.columns ?? [];
  const rows = res.rows ?? [];
  if (cols.length === 1 && cols[0].name.toUpperCase() === "QUERY PLAN") {
    const plan = parsePlan(rows.map((r) => r[0] ?? ""));
    if (plan) return <PlanTree lines={rows.map((r) => r[0] ?? "")} plan={plan} ms={ms} />;
  }
  return (
    <div className="result">
      <div className="result-head">
        <b>
          {rows.length} {rows.length === 1 ? "row" : "rows"}
        </b>
        {ms !== undefined && <span>{fmtMs(ms)}</span>}
      </div>
      <div className="listing-wrap">
        <table className="listing">
          <thead>
            <tr>
              {cols.map((c, i) => (
                <th key={i} className={isNumeric(c.type) ? "num" : undefined} title={c.type}>
                  {c.name}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i}>
                {row.map((v, j) => (
                  <td key={j} className={`${isNumeric(cols[j]?.type) ? "num" : ""}${v === null ? " null" : ""}`}>
                    {v === null ? "NULL" : v}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {rows.length === 0 && <div className="empty">No rows matched.</div>}
    </div>
  );
}

function isNumeric(t?: string) {
  return !!t && /^(Int|Float|Decimal|BigInt|SmallInt|Double)/i.test(t);
}

function PlanTree({ plan, ms, lines }: { plan: ReturnType<typeof parsePlan> & object; ms?: number; lines: string[] }) {
  const [raw, setRaw] = useState(false);
  const top = Math.max(...plan.roots.map((n) => n.ms ?? 0), 0);
  return (
    <div className="result">
      <div className="result-head">
        <b>Plan</b>
        {ms !== undefined && <span>{fmtMs(ms)} round trip</span>}
        <span className="spacer" style={{ flex: 1 }} />
        <button className="btn" style={{ padding: "2px 9px", fontSize: 12 }} onClick={() => setRaw(!raw)}>
          {raw ? "Tree" : "Text"}
        </button>
      </div>
      {raw ? (
        <pre className="plan" style={{ margin: 0, padding: "10px 12px", overflowX: "auto" }}>
          {lines.join("\n")}
        </pre>
      ) : (
        <div className="plan">
          {plan.roots.map((n, i) => (
            <PNode key={i} n={n} top={top} />
          ))}
          {plan.footer.map((f, i) => (
            <div key={i} className="footer-line">
              {f}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function PNode({ n, top }: { n: PlanNode; top: number }) {
  const off = n.estimate !== null && n.actual !== null && n.estimate > 0 ? n.actual / n.estimate : null;
  const misestimate = off !== null && (off > 2 || off < 0.5);
  return (
    <div className="pnode">
      <div className="prow">
        <div>
          <span className="pop">{n.op}</span>
          {n.title && <span className="ptitle">{n.title}</span>}
        </div>
        {n.details.map((d, i) => (
          <div key={i} className="pdetail">
            {d}
          </div>
        ))}
        <div className="pstats">
          {n.actual !== null ? (
            <span className="row">
              {n.actual.toLocaleString()} rows <span className="est">/ est {n.estimate?.toLocaleString()}</span>
              {misestimate && <span className="off"> ×{off! >= 1 ? off!.toFixed(1) : `1/${(1 / off!).toFixed(1)}`}</span>}
            </span>
          ) : (
            n.estimate !== null && <span className="row est">est {n.estimate.toLocaleString()} rows</span>
          )}
          {n.ms !== null && (
            <span className="row">
              {fmtMs(n.ms)}
              {n.kv !== null && <span className="est"> · {n.kv} kv</span>}
            </span>
          )}
          {n.ms !== null && top > 0 && <span className="timebar" style={{ width: `${Math.max(2, (n.ms / top) * 120)}px` }} />}
        </div>
      </div>
      {n.children.map((c, i) => (
        <PNode key={i} n={c} top={top} />
      ))}
    </div>
  );
}
