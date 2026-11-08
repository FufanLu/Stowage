//! The admin HTTP listener: what Prometheus scrapes and what the console reads.
//!
//! ```text
//! GET  /metrics      Prometheus text format
//! GET  /health       "ok"
//! GET  /status       this node's view of every range (JSON)
//! GET  /api/nodes    every node's /health and /metrics, fetched by this node
//! POST /api/sql      {"sql": "..."} → results, run as the HTTP Basic user
//! GET  /*            the console (static files from --console-dir)
//! ```
//!
//! `/api/sql` authenticates exactly like pgwire: once any user exists, a
//! password is required and checked against the stored SCRAM verifier, and the
//! statement runs with that user's privileges.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};
use stowage_sql::KvStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
pub struct Admin {
    pub node: u64,
    pub client: Arc<stowage_node::KvClient>,
    pub store: Arc<dyn KvStore>,
    /// Every node's admin address, this one included (`--admin-peers`).
    pub peers: BTreeMap<u64, String>,
    pub console_dir: Option<PathBuf>,
}

pub fn router(a: Admin) -> axum::Router {
    axum::Router::new()
        .route(
            "/metrics",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                    stowage_common::metrics::render(),
                )
            }),
        )
        .route("/health", get(|| async { "ok\n" }))
        .route("/status", get(status))
        .route("/api/nodes", get(nodes))
        .route("/api/sql", post(sql))
        .fallback(console)
        .with_state(a)
}

pub async fn serve(addr: &str, a: Admin) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "admin HTTP listening");
    axum::serve(listener, router(a)).await
}

fn json_response(code: StatusCode, v: &Value) -> Response {
    (
        code,
        [(header::CONTENT_TYPE, "application/json")],
        v.to_string(),
    )
        .into_response()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

async fn status(State(a): State<Admin>) -> Response {
    let ranges = a.client.ranges().await.unwrap_or_default();
    let body = json!({
        "node": a.node,
        "ranges": ranges.iter().map(|r| json!({
            "id": r.desc.id,
            "start": String::from_utf8_lossy(&r.desc.start),
            "end": String::from_utf8_lossy(&r.desc.end),
            "start_hex": hex(&r.desc.start),
            "end_hex": hex(&r.desc.end),
            "generation": r.desc.generation,
            "replicas": r.desc.replicas.iter().collect::<Vec<_>>(),
            "leader": r.leader,
            "term": r.term,
            "bytes": r.approx_bytes,
        })).collect::<Vec<_>>(),
    });
    json_response(StatusCode::OK, &body)
}

/// One plain HTTP/1.1 GET with a deadline (peers are other stowd processes).
async fn http_get(addr: &str, path: &str, timeout: Duration) -> Option<String> {
    let work = async {
        let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
        let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.ok()?;
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.ok()?;
        let text = String::from_utf8(out).ok()?;
        let (head, body) = text.split_once("\r\n\r\n")?;
        head.starts_with("HTTP/1.1 200").then(|| body.to_owned())
    };
    tokio::time::timeout(timeout, work).await.ok().flatten()
}

async fn nodes(State(a): State<Admin>) -> Response {
    let mut peers = a.peers.clone();
    if peers.is_empty() {
        peers.insert(a.node, String::new());
    }
    let node = a.node;
    let handles: Vec<_> = peers
        .into_iter()
        .map(|(id, addr)| {
            tokio::spawn(async move {
                let t = Instant::now();
                let metrics = if id == node {
                    Some(stowage_common::metrics::render())
                } else {
                    http_get(&addr, "/metrics", Duration::from_millis(800)).await
                };
                json!({
                    "id": id,
                    "addr": addr,
                    "self": id == node,
                    "up": metrics.is_some(),
                    "ms": t.elapsed().as_secs_f64() * 1e3,
                    "metrics": metrics,
                })
            })
        })
        .collect();
    let mut list = Vec::with_capacity(handles.len());
    for h in handles {
        list.push(h.await.unwrap_or(Value::Null));
    }
    json_response(StatusCode::OK, &json!({ "nodes": list }))
}

/// `Authorization: Basic base64(user:password)`.
fn basic_auth(h: &HeaderMap) -> Option<(String, String)> {
    let v = h.get(header::AUTHORIZATION)?.to_str().ok()?;
    let b = stowage_sql::auth::unb64(v.strip_prefix("Basic ")?.trim())?;
    let s = String::from_utf8(b).ok()?;
    let (u, p) = s.split_once(':')?;
    Some((u.to_owned(), p.to_owned()))
}

fn error_body(code: &str, message: &str) -> Value {
    json!({ "error": { "code": code, "message": message } })
}

async fn sql(State(a): State<Admin>, headers: HeaderMap, body: Bytes) -> Response {
    let Some(text) = serde_json::from_slice::<Value>(&body)
        .ok()
        .and_then(|v| v["sql"].as_str().map(str::to_owned))
    else {
        return json_response(
            StatusCode::BAD_REQUEST,
            &error_body("08P01", "expected a JSON body {\"sql\": \"...\"}"),
        );
    };
    let mut session = stowage_sql::Session::new(a.store.clone());
    match stowage_sql::auth::required(&a.store).await {
        Err(e) => {
            return json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &error_body("58000", &e.to_string()),
            );
        }
        Ok(true) => {
            let denied = || {
                json_response(
                    StatusCode::UNAUTHORIZED,
                    &error_body("28P01", "password authentication failed"),
                )
            };
            let Some((user, password)) = basic_auth(&headers) else {
                return denied();
            };
            let found = stowage_sql::auth::lookup(&a.store, &user)
                .await
                .ok()
                .flatten();
            let ok = found
                .as_ref()
                .filter(|u| u.login)
                .and_then(|u| u.scram.as_ref())
                .and_then(|s| {
                    let salt = stowage_sql::auth::unb64(&s.salt)?;
                    let mine = stowage_sql::auth::scram_secret(&password, &salt, s.iterations);
                    Some(mine.stored_key == s.stored_key)
                })
                .unwrap_or(false);
            if !ok {
                stowage_sql::audit::record("login_failed", &user, "console");
                stowage_common::metrics::inc(
                    "stowage_sql_errors_total",
                    &[("sqlstate", "28P01")],
                    1.0,
                );
                return denied();
            }
            stowage_sql::audit::record("login", &user, "console");
            session.set_user(Some(user));
        }
        Ok(false) => {}
    }
    let t = Instant::now();
    let r = session.execute(&text).await;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    match r {
        Ok(outs) => {
            let results: Vec<Value> = outs
                .iter()
                .map(|o| match o {
                    stowage_sql::Output::Rows { columns, .. } => json!({
                        "columns": columns.iter().map(|(n, t)| json!({"name": n, "type": format!("{t:?}")})).collect::<Vec<_>>(),
                        "rows": o.rows(),
                    }),
                    stowage_sql::Output::Command(tag) => json!({ "command": tag }),
                })
                .collect();
            json_response(StatusCode::OK, &json!({ "results": results, "ms": ms }))
        }
        Err(e) => json_response(
            StatusCode::OK,
            &json!({ "error": { "code": e.sqlstate(), "message": e.to_string() }, "ms": ms }),
        ),
    }
}

/// Static files for the console; unknown paths get index.html (client routing).
async fn console(State(a): State<Admin>, uri: Uri) -> Response {
    let Some(dir) = &a.console_dir else {
        return (
            StatusCode::NOT_FOUND,
            "no console here: start stowd with --console-dir web/console/dist\n",
        )
            .into_response();
    };
    let rel = uri.path().trim_start_matches('/');
    if rel.split('/').any(|p| p == ".." || p.starts_with('.')) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let mut path = dir.join(rel);
    if rel.is_empty() || !path.is_file() {
        path = dir.join("index.html");
    }
    let Ok(bytes) = tokio::fs::read(&path).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let ct = match path.extension().and_then(|e| e.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("svg") => "image/svg+xml",
        Some("woff2") => "font/woff2",
        Some("png") => "image/png",
        Some("json") => "application/json",
        _ => "application/octet-stream",
    };
    let cache = if rel.starts_with("assets/") {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    (
        [(header::CONTENT_TYPE, ct), (header::CACHE_CONTROL, cache)],
        bytes,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    async fn call(
        app: &str,
        method: &str,
        path: &str,
        auth: Option<&str>,
        body: &str,
    ) -> (u16, Value) {
        let mut s = tokio::net::TcpStream::connect(app).await.unwrap();
        let auth = auth.map_or(String::new(), |a| {
            format!(
                "Authorization: Basic {}\r\n",
                stowage_sql::auth::b64_public(a.as_bytes())
            )
        });
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: x\r\n{auth}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        s.write_all(req.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        let code = text[9..12].parse().unwrap();
        let body = text.split_once("\r\n\r\n").unwrap().1;
        (code, serde_json::from_str(body).unwrap_or(Value::Null))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn console_api() {
        let cluster = stowage_node::testing::TestCluster::start(3).await.unwrap();
        let client = cluster.client();
        let store: Arc<dyn KvStore> = Arc::new(stowage_sql::ClusterKv {
            client: client.clone(),
        });
        let addr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().to_string()
        };
        let admin = Admin {
            node: 1,
            client: client.clone(),
            store,
            peers: BTreeMap::from([(1, addr.clone()), (2, "127.0.0.1:1".into())]),
            console_dir: None,
        };
        let a = addr.clone();
        tokio::spawn(async move { serve(&a, admin).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Before any user exists, SQL needs no password.
        let (code, r) = call(&addr, "POST", "/api/sql", None, r#"{"sql":"CREATE TABLE t (id INT PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1,'a'),(2,'b'); SELECT v FROM t ORDER BY id"}"#).await;
        assert_eq!(code, 200, "{r}");
        assert_eq!(r["results"][2]["rows"], json!([["a"], ["b"]]), "{r}");
        assert_eq!(r["results"][1]["command"], "INSERT 0 2");

        let (_, r) = call(
            &addr,
            "POST",
            "/api/sql",
            None,
            r#"{"sql":"SELECT nope FROM t"}"#,
        )
        .await;
        assert_eq!(r["error"]["code"], "42703", "{r}");

        // With a user, a password is required and privileges apply.
        call(&addr, "POST", "/api/sql", None, r#"{"sql":"CREATE USER root WITH SUPERUSER PASSWORD 'pw'; CREATE USER ro WITH PASSWORD 'pw2'"}"#).await;
        let (code, _) = call(&addr, "POST", "/api/sql", None, r#"{"sql":"SELECT 1"}"#).await;
        assert_eq!(code, 401);
        let (code, _) = call(
            &addr,
            "POST",
            "/api/sql",
            Some("root:wrong"),
            r#"{"sql":"SELECT 1"}"#,
        )
        .await;
        assert_eq!(code, 401);
        let (code, r) = call(
            &addr,
            "POST",
            "/api/sql",
            Some("root:pw"),
            r#"{"sql":"EXPLAIN SELECT v FROM t WHERE id = 1"}"#,
        )
        .await;
        assert_eq!(code, 200, "{r}");
        assert!(
            r["results"][0]["rows"].to_string().contains("Scan t"),
            "{r}"
        );
        let (_, r) = call(
            &addr,
            "POST",
            "/api/sql",
            Some("ro:pw2"),
            r#"{"sql":"SELECT v FROM t"}"#,
        )
        .await;
        assert_eq!(r["error"]["code"], "42501", "{r}");

        // /status and /api/nodes: node 2's admin address is dead.
        let (code, s) = call(&addr, "GET", "/status", None, "").await;
        assert_eq!(code, 200);
        assert!(
            s["ranges"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| !r["leader"].is_null()),
            "{s}"
        );
        let (_, n) = call(&addr, "GET", "/api/nodes", None, "").await;
        assert_eq!(n["nodes"][0]["up"], true);
        assert_eq!(n["nodes"][1]["up"], false, "{n}");

        let (code, _) = call(&addr, "GET", "/", None, "").await;
        assert_eq!(code, 404, "no --console-dir");
    }
}
