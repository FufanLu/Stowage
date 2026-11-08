// Everything the console reads comes from the stowd admin listener that served it.

export interface Range {
  id: number;
  start: string;
  end: string;
  start_hex: string;
  end_hex: string;
  generation: number;
  replicas: number[];
  leader: number | null;
  term: number;
  bytes: number;
}

export interface Status {
  node: number;
  ranges: Range[];
}

export interface NodeInfo {
  id: number;
  addr: string;
  self: boolean;
  up: boolean;
  ms: number;
  metrics: string | null;
}

export interface Column {
  name: string;
  type: string;
}

export interface SqlResult {
  columns?: Column[];
  rows?: (string | null)[][];
  command?: string;
}

export interface SqlResponse {
  results?: SqlResult[];
  error?: { code: string; message: string };
  ms?: number;
}

export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function getJson<T>(path: string): Promise<T> {
  const r = await fetch(path, { cache: "no-store" });
  if (!r.ok) throw new HttpError(r.status, `${path} answered ${r.status}`);
  return (await r.json()) as T;
}

export const fetchStatus = () => getJson<Status>("/status");
export const fetchNodes = () => getJson<{ nodes: NodeInfo[] }>("/api/nodes").then((r) => r.nodes);

export interface Credentials {
  user: string;
  password: string;
}

/** The console's login for this page load; the password never touches storage. */
export const login: { current: Credentials | null } = { current: null };

export async function runSql(sql: string, cred: Credentials | null): Promise<SqlResponse> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (cred && cred.user) {
    const raw = new TextEncoder().encode(`${cred.user}:${cred.password}`);
    headers.Authorization = `Basic ${btoa(String.fromCharCode(...raw))}`;
  }
  const r = await fetch("/api/sql", { method: "POST", headers, body: JSON.stringify({ sql }) });
  if (r.status === 401) throw new HttpError(401, "password authentication failed");
  if (!r.ok && r.status !== 200) {
    const body = (await r.json().catch(() => null)) as SqlResponse | null;
    throw new HttpError(r.status, body?.error?.message ?? `/api/sql answered ${r.status}`);
  }
  return (await r.json()) as SqlResponse;
}

// ---- Prometheus text ------------------------------------------------------------------------

export interface Sample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

export function parseMetrics(text: string): Sample[] {
  const out: Sample[] = [];
  for (const line of text.split("\n")) {
    if (!line || line.startsWith("#")) continue;
    const m = /^([a-zA-Z_:][\w:]*)(\{(.*)\})?\s+(\S+)$/.exec(line);
    if (!m) continue;
    const labels: Record<string, string> = {};
    if (m[3]) {
      for (const lm of m[3].matchAll(/(\w+)="((?:[^"\\]|\\.)*)"/g)) labels[lm[1]] = lm[2];
    }
    out.push({ name: m[1], labels, value: Number(m[4]) });
  }
  return out;
}

export function metric(samples: Sample[], name: string, labels: Record<string, string> = {}): number | null {
  let total: number | null = null;
  for (const s of samples) {
    if (s.name !== name) continue;
    if (Object.entries(labels).some(([k, v]) => s.labels[k] !== v)) continue;
    total = (total ?? 0) + s.value;
  }
  return total;
}

/** Upper bound of the bucket holding quantile q of a cumulative histogram (lifetime). */
export function histogramQuantile(samples: Sample[], name: string, q: number): number | null {
  const buckets = samples
    .filter((s) => s.name === `${name}_bucket`)
    .map((s) => ({ le: s.labels.le === "+Inf" ? Infinity : Number(s.labels.le), n: s.value }))
    .sort((a, b) => a.le - b.le);
  const total = buckets.at(-1)?.n ?? 0;
  if (!total) return null;
  return buckets.find((b) => b.n >= q * total)?.le ?? null;
}

// ---- keys -----------------------------------------------------------------------------------

export type Region = "start" | "catalog" | "sql" | "doc" | "ledger" | "end" | "other";

export interface KeyLabel {
  region: Region;
  text: string;
}

/** What a range boundary key means in Stowage's key layout. */
export function describeKey(hex: string): KeyLabel {
  if (!hex) return { region: "start", text: "−∞" };
  const b = hex.match(/../g)?.map((x) => parseInt(x, 16)) ?? [];
  const u32 = (i: number) => (b.length >= i + 4 ? ((b[i] << 24) | (b[i + 1] << 16) | (b[i + 2] << 8) | b[i + 3]) >>> 0 : null);
  switch (b[0]) {
    case 0x01: {
      const ascii = String.fromCharCode(...b.slice(1).filter((c) => c >= 0x20 && c < 0x7f));
      return { region: "catalog", text: `catalog ${ascii}`.trim() };
    }
    case 0x02: {
      const t = u32(1);
      const i = u32(5);
      return { region: "sql", text: t === null ? "sql" : `table ${t}${i === null ? "" : ` · index ${i}`}` };
    }
    case 0x03: {
      const t = u32(1);
      const i = u32(5);
      return { region: "doc", text: t === null ? "documents" : `ddb table ${t}${i ? ` · gsi ${i}` : ""}` };
    }
    case 0x04:
      return { region: "ledger", text: "gsi ledger" };
    default:
      return { region: "other", text: `0x${hex.slice(0, 16)}` };
  }
}

export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1 << 20) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1 << 30) return `${(n / (1 << 20)).toFixed(1)} MiB`;
  return `${(n / (1 << 30)).toFixed(2)} GiB`;
}

export function fmtMs(ms: number): string {
  if (ms < 1) return `${ms.toFixed(3)} ms`;
  if (ms < 100) return `${ms.toFixed(1)} ms`;
  return `${Math.round(ms).toLocaleString()} ms`;
}

// ---- EXPLAIN --------------------------------------------------------------------------------

export interface PlanNode {
  op: string;
  title: string;
  details: string[];
  estimate: number | null;
  actual: number | null;
  ms: number | null;
  kv: number | null;
  children: PlanNode[];
}

export interface Plan {
  roots: PlanNode[];
  footer: string[];
}

function parseStats(s: string) {
  const m = /\(([^()]*rows≈[^()]*)\)\s*$/.exec(s);
  if (!m) return { rest: s, estimate: null, actual: null, ms: null, kv: null };
  const body = m[1];
  const num = (re: RegExp) => {
    const x = re.exec(body);
    return x ? Number(x[1]) : null;
  };
  return {
    rest: s.slice(0, m.index).trimEnd(),
    estimate: num(/rows≈(\d+)/),
    actual: num(/actual (\d+) rows/),
    ms: num(/([\d.]+) ms/),
    kv: num(/(\d+) kv requests?/),
  };
}

/** Parse Stowage's indented EXPLAIN text into a tree. Returns null for anything else. */
export function parsePlan(lines: string[]): Plan | null {
  const roots: PlanNode[] = [];
  const footer: string[] = [];
  const stack: { depth: number; node: PlanNode }[] = [];
  let last: PlanNode | null = null;
  for (const raw of lines) {
    if (!raw.trim()) continue;
    const indent = raw.length - raw.trimStart().length;
    let text = raw.trim();
    if (/^(Execution|Planning|Timing)\b/.test(text) || /^\w[\w ]*: /.test(text) && !text.includes("(rows≈")) {
      footer.push(text);
      continue;
    }
    if (text.startsWith("·")) {
      if (!last) return null;
      const st = parseStats(text.slice(1).trim());
      last.details.push(st.rest);
      if (st.estimate !== null) Object.assign(last, { estimate: st.estimate, actual: st.actual, ms: st.ms, kv: st.kv });
      continue;
    }
    const child = text.startsWith("→");
    if (child) text = text.slice(1).trim();
    const st = parseStats(text);
    const parts = st.rest.split(" · ");
    const head = parts[0];
    const sp = head.indexOf(" ");
    const node: PlanNode = {
      op: sp < 0 ? head : head.slice(0, sp),
      title: sp < 0 ? "" : head.slice(sp + 1),
      details: parts.slice(1),
      estimate: st.estimate,
      actual: st.actual,
      ms: st.ms,
      kv: st.kv,
      children: [],
    };
    while (stack.length && stack[stack.length - 1].depth >= indent) stack.pop();
    if (stack.length && child) stack[stack.length - 1].node.children.push(node);
    else roots.push(node);
    stack.push({ depth: indent, node });
    last = node;
  }
  return roots.length ? { roots, footer } : null;
}

const SEQ = 2 ** 40;
/** Range ids are `(node << 40) | seq`: shown as r<node>.<seq>, the first range as r1. */
export function rid(id: number): string {
  return id < SEQ ? `r${id}` : `r${Math.floor(id / SEQ)}.${id % SEQ}`;
}
export function ridTitle(id: number): string {
  return id < SEQ ? `range ${id}` : `range ${id}: created by node ${Math.floor(id / SEQ)}, its range #${id % SEQ}`;
}
