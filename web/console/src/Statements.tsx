import { useCallback, useEffect, useState } from "react";
import { HttpError, fmtMs, login, runSql, type SqlResult } from "./api";

const QUERY = "SELECT query, calls, total_ms, mean_ms, max_ms, rows, errors FROM stowage_stat_statements ORDER BY total_ms DESC";

export function Statements() {
  const [res, setRes] = useState<SqlResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await runSql(QUERY, login.current);
      if (r.error) setError(`${r.error.code} ${r.error.message}`);
      else {
        setRes(r.results?.[0] ?? null);
        setError(null);
      }
    } catch (e) {
      setError(
        e instanceof HttpError && e.status === 401
          ? "This cluster requires a login. Sign in on the SQL tab, then come back."
          : e instanceof Error
            ? e.message
            : String(e),
      );
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const rows = res?.rows ?? [];
  const total = rows.reduce((s, r) => s + Number(r[2] ?? 0), 0);

  return (
    <section className="block" aria-labelledby="sth">
      <div className="block-head">
        <h2 id="sth">Statements</h2>
        <span className="source">
          <code>stowage_stat_statements</code> on this gateway node · <span style={{ whiteSpace: "nowrap" }}>literals folded to <code>?</code></span>
        </span>
        <span style={{ flex: 1 }} />
        <button className="btn" onClick={() => void load()} disabled={loading}>
          {loading ? "Reading…" : "Refresh"}
        </button>
      </div>
      <p className="lede">
        Every statement this node has run since it started, grouped by shape. Share is of total time; a statement that is
        cheap but called constantly can outweigh one slow report.
      </p>
      {error && (
        <div className="error" role="alert">
          {error}
        </div>
      )}
      {loading && !res && <div className="skeleton" style={{ marginTop: 14 }} aria-label="Loading statements" />}
      {res && rows.length === 0 && (
        <div className="empty">
          Nothing recorded yet. Statements appear here after they run through this node, from <code>psql</code>, a driver or
          the SQL tab.
        </div>
      )}
      {rows.length > 0 && (
        <div className="listing-wrap" style={{ marginTop: 14 }}>
          <table className="listing">
            <thead>
              <tr>
                <th>statement</th>
                <th className="num dcol">calls</th>
                <th className="dcol">share of time</th>
                <th className="num dcol">total</th>
                <th className="num dcol">mean</th>
                <th className="num dcol">max</th>
                <th className="num dcol">rows</th>
                <th className="num dcol">errors</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, i) => {
                const share = total > 0 ? Number(r[2]) / total : 0;
                const errors = Number(r[6] ?? 0);
                return (
                  <tr key={i}>
                    <td title={r[0] ?? ""} className="qcell">
                      <span className="qtext">{r[0]}</span>
                      <span className="mstats">
                        <span className="share" style={{ width: `${Math.max(1, share * 80)}px` }} />
                        {(share * 100).toFixed(1)}% · {fmtMs(Number(r[2]))} total · {Number(r[1]).toLocaleString()} {Number(r[1]) === 1 ? "call" : "calls"} ·{" "}
                        {fmtMs(Number(r[3]))} mean{errors ? ` · ${errors} errors` : ""}
                      </span>
                    </td>
                    <td className="num dcol">{Number(r[1]).toLocaleString()}</td>
                    <td className="dcol">
                      <span className="share" style={{ width: `${Math.max(1, share * 120)}px` }} />
                      {(share * 100).toFixed(1)}%
                    </td>
                    <td className="num dcol">{fmtMs(Number(r[2]))}</td>
                    <td className="num dcol">{fmtMs(Number(r[3]))}</td>
                    <td className="num dcol">{fmtMs(Number(r[4]))}</td>
                    <td className="num dcol">{Number(r[5]).toLocaleString()}</td>
                    <td className={`num dcol${errors ? " fault-ink" : " dim"}`}>{errors}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
