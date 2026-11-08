//! Statement statistics (like Postgres' `pg_stat_statements`) and the slow
//! query log.
//!
//! Statements are grouped by fingerprint: the text with every literal replaced
//! by `?`, so `WHERE id = 1` and `WHERE id = 2` count together. The numbers
//! are per node, since each gateway counts what it ran. They are readable
//! as `SELECT * FROM stowage_stat_statements`.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Entry {
    pub calls: u64,
    pub errors: u64,
    pub rows: u64,
    pub total_ms: f64,
    pub max_ms: f64,
}

fn table() -> &'static Mutex<HashMap<String, Entry>> {
    static T: OnceLock<Mutex<HashMap<String, Entry>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(HashMap::new()))
}

static SLOW_LOG: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

/// Also write slow statements to `path`.
pub fn init_slow_log(path: &std::path::Path) -> std::io::Result<()> {
    let f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let _ = SLOW_LOG.set(Mutex::new(f));
    Ok(())
}

/// The statement with literals replaced by `?`.
pub fn fingerprint(sql: &str) -> String {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' {
            i += 1;
            while i < b.len() && !(b[i] == b'\'' && b.get(i + 1) != Some(&b'\'')) {
                i += if b[i] == b'\'' { 2 } else { 1 };
            }
            i += 1;
            out.push('?');
        } else if c.is_ascii_digit()
            && (i == 0
                || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_' || b[i - 1] == b'$'))
        {
            while i < b.len()
                && (b[i].is_ascii_digit() || b[i] == b'.' || b[i] == b'e' || b[i] == b'E')
            {
                i += 1;
            }
            out.push('?');
        } else {
            let start = i;
            while i < b.len()
                && b[i] != b'\''
                && !(b[i].is_ascii_digit()
                    && (i == start
                        || !(b[i - 1].is_ascii_alphanumeric()
                            || b[i - 1] == b'_'
                            || b[i - 1] == b'$')))
            {
                i += 1;
            }
            if i == start {
                i += 1;
            }
            out.push_str(&sql[start..i]);
        }
    }
    // Collapse `(?, ?, ?)` lists so multi-row INSERTs group together.
    let mut s = out;
    while s.contains("?, ?") {
        s = s.replace("?, ?", "?");
    }
    s.chars().take(500).collect()
}

pub fn record(sql: &str, ms: f64, rows: u64, ok: bool) {
    let mut t = table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if t.len() >= 5000 && !t.contains_key(sql) {
        return; // bounded, like pg_stat_statements.max
    }
    let e = t.entry(fingerprint(sql)).or_default();
    e.calls += 1;
    e.rows += rows;
    e.total_ms += ms;
    e.max_ms = e.max_ms.max(ms);
    if !ok {
        e.errors += 1;
    }
}

/// Every fingerprint, busiest first.
pub fn snapshot() -> Vec<(String, Entry)> {
    let t = table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut v: Vec<(String, Entry)> = t.iter().map(|(k, e)| (k.clone(), e.clone())).collect();
    v.sort_by(|a, b| b.1.total_ms.total_cmp(&a.1.total_ms));
    v
}

pub fn reset() {
    table()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

/// Log a statement slower than its session's `slow_query_ms`.
pub fn slow(user: &str, sql: &str, ms: f64, stages: &[(&'static str, f64)]) {
    let line = serde_json::json!({
        "event": "slow_query",
        "user": user,
        "ms": (ms * 1000.0).round() / 1000.0,
        "stages": stages.iter().map(|(k, v)| serde_json::json!({"stage": k, "ms": (v * 1000.0).round() / 1000.0})).collect::<Vec<_>>(),
        "statement": sql.chars().take(2000).collect::<String>(),
    })
    .to_string();
    tracing::warn!(target: "slow_query", "{line}");
    if let Some(f) = SLOW_LOG.get() {
        let mut f = f.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = writeln!(f, "{line}");
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn fingerprints_group_literals() {
        assert_eq!(
            super::fingerprint("SELECT * FROM t WHERE id = 42 AND name = 'it''s'"),
            "SELECT * FROM t WHERE id = ? AND name = ?"
        );
        assert_eq!(
            super::fingerprint("INSERT INTO t2 VALUES (1, 'a'), (2, 'b')"),
            "INSERT INTO t2 VALUES (?), (?)"
        );
        assert_eq!(
            super::fingerprint("SELECT c1 FROM t WHERE x = $1"),
            "SELECT c1 FROM t WHERE x = $1"
        );
    }
}
