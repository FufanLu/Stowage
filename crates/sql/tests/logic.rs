//! sqllogictest-style files in `tests/slt/`, run against the in-memory store,
//! and against a real three-node cluster with `STOWAGE_SLT_CLUSTER=1`.
//!
//! ```text
//! statement ok | statement error [substring]
//! <sql>
//!
//! query <column types, one letter each> [rowsort]
//! <sql>
//! ----
//! <one row per line, values separated by one space, NULL as NULL>
//! ```

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use stowage_sql::{ClusterKv, KvStore, MemKv, Output, Session};

async fn run_file(store: Arc<dyn KvStore>, path: &std::path::Path) -> Result<usize, String> {
    let text = std::fs::read_to_string(path).unwrap();
    let mut s = Session::new(store);
    let mut lines = text.lines().enumerate().peekable();
    let mut checked = 0;
    while let Some((n, line)) = lines.next() {
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let head: Vec<&str> = line.split_whitespace().collect();
        let mut sql = String::new();
        while let Some((_, l)) = lines.peek() {
            if l.trim().is_empty() || l.trim() == "----" {
                break;
            }
            sql.push_str(l);
            sql.push('\n');
            lines.next();
        }
        let at = format!("{}:{}", path.display(), n + 1);
        match head[0] {
            "statement" => {
                let r = s.execute(&sql).await;
                match (head.get(1).copied(), r) {
                    (Some("ok"), Ok(_)) => {}
                    (Some("ok"), Err(e)) => {
                        return Err(format!("{at}: expected ok, got {e}\n{sql}"));
                    }
                    (Some("error"), Ok(_)) => {
                        return Err(format!("{at}: expected an error\n{sql}"));
                    }
                    (Some("error"), Err(e)) => {
                        let want = head[2..].join(" ");
                        if !e.to_string().contains(&want) && e.sqlstate() != want {
                            return Err(format!(
                                "{at}: expected error containing {want:?}, got {e}"
                            ));
                        }
                    }
                    _ => return Err(format!("{at}: bad directive {line}")),
                }
            }
            "query" => {
                let rowsort = head.get(2) == Some(&"rowsort");
                let mut expected: Vec<String> = Vec::new();
                if matches!(lines.peek(), Some((_, l)) if l.trim() == "----") {
                    lines.next();
                    while let Some((_, l)) = lines.peek() {
                        if l.trim().is_empty() {
                            break;
                        }
                        expected.push(l.trim_end().to_owned());
                        lines.next();
                    }
                }
                let out = s
                    .execute(&sql)
                    .await
                    .map_err(|e| format!("{at}: {e}\n{sql}"))?;
                let Some(Output::Rows { .. }) = out.last() else {
                    return Err(format!("{at}: not a query\n{sql}"));
                };
                let mut got: Vec<String> = out
                    .last()
                    .unwrap()
                    .rows()
                    .into_iter()
                    .map(|r| {
                        r.into_iter()
                            .map(|v| v.unwrap_or_else(|| "NULL".into()))
                            .map(|v| if v.is_empty() { "(empty)".into() } else { v })
                            .collect::<Vec<_>>()
                            .join(" ")
                    })
                    .collect();
                if rowsort {
                    got.sort();
                    expected.sort();
                }
                if got != expected {
                    return Err(format!(
                        "{at}: wrong result for\n{sql}expected:\n  {}\ngot:\n  {}",
                        expected.join("\n  "),
                        got.join("\n  ")
                    ));
                }
                checked += 1;
            }
            other => return Err(format!("{at}: unknown directive {other}")),
        }
    }
    Ok(checked)
}

fn files() -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/slt"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "slt"))
        .collect();
    v.sort();
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn logic_tests() {
    let cluster = if std::env::var("STOWAGE_SLT_CLUSTER").is_ok() {
        Some(stowage_node::testing::TestCluster::start(3).await.unwrap())
    } else {
        None
    };
    let mut failures = Vec::new();
    let mut total = 0;
    for f in files() {
        let store: Arc<dyn KvStore> = match &cluster {
            Some(c) => Arc::new(ClusterKv { client: c.client() }),
            None => Arc::new(MemKv::default()),
        };
        match run_file(store, &f).await {
            Ok(n) => total += n,
            Err(e) => failures.push(e),
        }
        if let Some(c) = &cluster {
            // Each file uses fresh tables; drop them so the next file starts clean.
            let mut s = Session::new(Arc::new(ClusterKv { client: c.client() }));
            if let Ok(out) = s
                .execute("SELECT table_name FROM information_schema.tables")
                .await
            {
                for r in out[0].rows() {
                    let _ = s
                        .execute(&format!("DROP TABLE {}", r[0].clone().unwrap()))
                        .await;
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} file(s) failed:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
    assert!(total > 0);
    eprintln!("{total} queries checked");
}
