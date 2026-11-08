//! A small process-wide metrics registry in the Prometheus text format.
//!
//! ```text
//! metrics::inc("stowage_sql_statements_total", &[("kind", "select")], 1.0);
//! metrics::observe("stowage_sql_statement_seconds", &[], 0.0031);
//! metrics::set("stowage_ranges", &[], 12.0);
//! metrics::render()   // → "# TYPE stowage_ranges gauge\nstowage_ranges 12\n…"
//! ```
//!
//! Histograms use fixed latency buckets from 0.5 ms to 10 s.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Mutex, OnceLock};

const BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Debug, Clone)]
enum Metric {
    Counter(f64),
    Gauge(f64),
    Histogram {
        counts: [u64; BUCKETS.len()],
        sum: f64,
        count: u64,
    },
}

type Key = (String, String);

fn registry() -> &'static Mutex<BTreeMap<Key, Metric>> {
    static R: OnceLock<Mutex<BTreeMap<Key, Metric>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn labels(l: &[(&str, &str)]) -> String {
    if l.is_empty() {
        return String::new();
    }
    let inner: Vec<String> = l
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect();
    format!("{{{}}}", inner.join(","))
}

fn with(name: &str, l: &[(&str, &str)], f: impl FnOnce(Option<&mut Metric>) -> Option<Metric>) {
    let mut r = registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let key = (name.to_owned(), labels(l));
    let m = r.get_mut(&key);
    if let Some(new) = f(m) {
        r.insert(key, new);
    }
}

/// Add to a counter.
pub fn inc(name: &str, l: &[(&str, &str)], by: f64) {
    with(name, l, |m| match m {
        Some(Metric::Counter(c)) => {
            *c += by;
            None
        }
        _ => Some(Metric::Counter(by)),
    });
}

/// Set a gauge.
pub fn set(name: &str, l: &[(&str, &str)], v: f64) {
    with(name, l, |_| Some(Metric::Gauge(v)));
}

/// Add to a gauge (connections opening and closing).
pub fn add(name: &str, l: &[(&str, &str)], by: f64) {
    with(name, l, |m| match m {
        Some(Metric::Gauge(g)) => {
            *g += by;
            None
        }
        _ => Some(Metric::Gauge(by)),
    });
}

/// Record one observation, in seconds.
pub fn observe(name: &str, l: &[(&str, &str)], seconds: f64) {
    with(name, l, |m| {
        let mut h = match m {
            Some(Metric::Histogram { counts, sum, count }) => {
                for (i, b) in BUCKETS.iter().enumerate() {
                    if seconds <= *b {
                        counts[i] += 1;
                    }
                }
                *sum += seconds;
                *count += 1;
                return None;
            }
            _ => ([0u64; BUCKETS.len()], 0.0, 0u64),
        };
        for (i, b) in BUCKETS.iter().enumerate() {
            if seconds <= *b {
                h.0[i] += 1;
            }
        }
        h.1 += seconds;
        h.2 += 1;
        Some(Metric::Histogram {
            counts: h.0,
            sum: h.1,
            count: h.2,
        })
    });
}

/// Everything, in the Prometheus text exposition format.
pub fn render() -> String {
    let r = registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut out = String::new();
    let mut last = "";
    for ((name, l), m) in r.iter() {
        if name != last {
            let t = match m {
                Metric::Counter(_) => "counter",
                Metric::Gauge(_) => "gauge",
                Metric::Histogram { .. } => "histogram",
            };
            let _ = writeln!(out, "# TYPE {name} {t}");
            last = name;
        }
        match m {
            Metric::Counter(v) | Metric::Gauge(v) => {
                let _ = writeln!(out, "{name}{l} {v}");
            }
            Metric::Histogram { counts, sum, count } => {
                let inner = l.trim_start_matches('{').trim_end_matches('}');
                let sep = if inner.is_empty() { "" } else { "," };
                for (i, b) in BUCKETS.iter().enumerate() {
                    let _ = writeln!(out, "{name}_bucket{{{inner}{sep}le=\"{b}\"}} {}", counts[i]);
                }
                let _ = writeln!(out, "{name}_bucket{{{inner}{sep}le=\"+Inf\"}} {count}");
                let _ = writeln!(out, "{name}_sum{l} {sum}");
                let _ = writeln!(out, "{name}_count{l} {count}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposition() {
        inc("t_requests_total", &[("op", "get")], 2.0);
        inc("t_requests_total", &[("op", "get")], 1.0);
        set("t_ranges", &[], 4.0);
        observe("t_latency_seconds", &[("op", "get")], 0.003);
        let out = render();
        assert!(
            out.contains("# TYPE t_requests_total counter\nt_requests_total{op=\"get\"} 3\n"),
            "{out}"
        );
        assert!(out.contains("t_ranges 4\n"));
        assert!(out.contains("t_latency_seconds_bucket{op=\"get\",le=\"0.0025\"} 0\n"));
        assert!(out.contains("t_latency_seconds_bucket{op=\"get\",le=\"0.005\"} 1\n"));
        assert!(out.contains("t_latency_seconds_count{op=\"get\"} 1\n"));
    }
}
