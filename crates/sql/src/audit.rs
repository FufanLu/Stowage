//! The audit log: authentication, privilege failures, DDL and grants, one JSON
//! line each, to a file (`stowd --audit-log`) and to the `audit` tracing target.

use std::io::Write as _;
use std::sync::{Mutex, OnceLock};

static FILE: OnceLock<Mutex<std::fs::File>> = OnceLock::new();

/// Append audit records to `path` from now on.
pub fn init(path: &std::path::Path) -> std::io::Result<()> {
    let f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    let _ = FILE.set(Mutex::new(f));
    Ok(())
}

pub fn record(event: &str, user: &str, detail: &str) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros());
    let line = serde_json::json!({"ts_us": ts, "event": event, "user": user, "detail": detail})
        .to_string();
    tracing::info!(target: "audit", "{line}");
    if let Some(f) = FILE.get() {
        let mut f = f.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = writeln!(f, "{line}");
    }
}
