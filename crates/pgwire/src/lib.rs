//! The Postgres frontend/backend protocol, version 3, for Stowage SQL sessions.
//!
//! ```text
//!  client                                   server
//!  StartupMessage(user, database) ───────►
//!                                  ◄─────── AuthenticationOk, ParameterStatus…, BackendKeyData, ReadyForQuery(I)
//!  Query("SELECT …") ────────────────────►
//!                                  ◄─────── RowDescription, DataRow…, CommandComplete("SELECT n"), ReadyForQuery
//!  Parse · Bind · Describe · Execute · Sync ►
//!                                  ◄─────── ParseComplete, BindComplete, RowDescription, DataRow…, CommandComplete, ReadyForQuery
//! ```
//!
//! Every frame after startup is `type byte ‖ i32 length (including itself) ‖ body`.
//! Values travel as text (format 0); binary parameters of integer types are
//! accepted too. With `STOWAGE_PGWIRE_TRACE=1` every frame is logged.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use sqlparser::ast::Statement;
use stowage_sql::{DataType, KvStore, Output, Session, TxStatus, Value};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};

type Reader = Box<dyn AsyncRead + Unpin + Send>;
type Writer = Box<dyn AsyncWrite + Unpin + Send>;

/// TLS for client connections: the server certificate chain and key (PEM).
pub fn tls_from_pem(
    cert: &std::path::Path,
    key: &std::path::Path,
) -> std::io::Result<Arc<rustls::ServerConfig>> {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let certs = CertificateDer::pem_file_iter(cert)
        .map_err(|e| std::io::Error::other(format!("{}: {e}", cert.display())))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| std::io::Error::other(format!("{}: {e}", cert.display())))?;
    let key = PrivateKeyDer::from_pem_file(key)
        .map_err(|e| std::io::Error::other(format!("{}: {e}", key.display())))?;
    let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(std::io::Error::other)?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(std::io::Error::other)?;
    Ok(Arc::new(cfg))
}

/// How a server treats TLS.
#[derive(Clone, Default)]
pub struct TlsPolicy {
    pub config: Option<Arc<rustls::ServerConfig>>,
    /// Refuse clients that do not upgrade to TLS.
    pub require: bool,
}

impl std::fmt::Debug for TlsPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsPolicy")
            .field("enabled", &self.config.is_some())
            .field("require", &self.require)
            .finish()
    }
}

const SSL_REQUEST: i32 = 80_877_103;
const GSSENC_REQUEST: i32 = 80_877_104;
const CANCEL_REQUEST: i32 = 80_877_102;
const PROTOCOL_3: i32 = 196_608;

/// Accept connections on `addr` and serve each with its own session.
pub async fn serve(addr: &str, store: Arc<dyn KvStore>) -> std::io::Result<()> {
    serve_with(addr, store, TlsPolicy::default()).await
}

/// [`serve`], with TLS: a client's `SSLRequest` is answered `S` and the
/// connection continues inside TLS.
pub async fn serve_with(
    addr: &str,
    store: Arc<dyn KvStore>,
    tls: TlsPolicy,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "pgwire listening");
    let next_pid = Arc::new(AtomicU32::new(1));
    loop {
        let (sock, peer) = listener.accept().await?;
        let _ = sock.set_nodelay(true);
        let (store, pid, tls) = (
            store.clone(),
            next_pid.fetch_add(1, Ordering::Relaxed),
            tls.clone(),
        );
        tokio::spawn(async move {
            stowage_common::metrics::add("stowage_sql_connections", &[], 1.0);
            let r = match negotiate(sock, &tls).await {
                Ok((rd, wr, pending, secure)) => {
                    Conn::new(rd, wr, store, pid, pending, secure, tls.require)
                        .run()
                        .await
                }
                Err(e) => Err(e),
            };
            stowage_common::metrics::add("stowage_sql_connections", &[], -1.0);
            if let Err(e) = r {
                tracing::debug!(%peer, error = %e, "pg connection ended");
            }
        });
    }
}

/// Read the first message: an `SSLRequest` is answered (and TLS started when
/// configured); anything else is handed to the connection unread.
async fn negotiate(
    mut sock: TcpStream,
    tls: &TlsPolicy,
) -> std::io::Result<(Reader, Writer, Option<Vec<u8>>, bool)> {
    loop {
        let len = sock.read_i32().await?;
        let mut body = vec![0; usize::try_from(len - 4).map_err(|_| Body::err())?];
        sock.read_exact(&mut body).await?;
        let code = Body(&body).i32()?;
        match (code, &tls.config) {
            (SSL_REQUEST, Some(cfg)) => {
                sock.write_all(b"S").await?;
                let stream = tokio_rustls::TlsAcceptor::from(cfg.clone())
                    .accept(sock)
                    .await?;
                let (r, w) = tokio::io::split(stream);
                return Ok((Box::new(r), Box::new(w), None, true));
            }
            (SSL_REQUEST | GSSENC_REQUEST, _) => sock.write_all(b"N").await?, // plain text; the client continues
            _ => {
                let (r, w) = sock.into_split();
                return Ok((Box::new(r), Box::new(w), Some(body), false));
            }
        }
    }
}

/// Open sessions per user on this node, for `CONNECTION LIMIT`.
fn sessions() -> &'static std::sync::Mutex<HashMap<String, u32>> {
    static S: std::sync::OnceLock<std::sync::Mutex<HashMap<String, u32>>> =
        std::sync::OnceLock::new();
    S.get_or_init(Default::default)
}

fn acquire_slot(user: &str, limit: Option<u32>) -> bool {
    let mut s = sessions()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let n = s.entry(user.to_owned()).or_default();
    if limit.is_some_and(|l| *n >= l) {
        return false;
    }
    *n += 1;
    true
}

impl Drop for Conn {
    fn drop(&mut self) {
        if let Some(u) = self.slot.take() {
            let mut s = sessions()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(n) = s.get_mut(&u) {
                *n = n.saturating_sub(1);
            }
        }
    }
}

fn trace_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("STOWAGE_PGWIRE_TRACE").is_ok())
}

fn preview(b: &[u8]) -> String {
    let s: String = b
        .iter()
        .take(64)
        .map(|&c| {
            if (0x20..0x7f).contains(&c) {
                c as char
            } else {
                '·'
            }
        })
        .collect();
    if b.len() > 64 { format!("{s}…") } else { s }
}

/// An outgoing frame under construction.
struct Msg {
    kind: u8,
    body: Vec<u8>,
}

impl Msg {
    fn new(kind: u8) -> Self {
        Self {
            kind,
            body: Vec::new(),
        }
    }
    fn i16(mut self, v: i16) -> Self {
        self.body.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn i32(mut self, v: i32) -> Self {
        self.body.extend_from_slice(&v.to_be_bytes());
        self
    }
    fn byte(mut self, v: u8) -> Self {
        self.body.push(v);
        self
    }
    fn cstr(mut self, s: &str) -> Self {
        self.body.extend_from_slice(s.as_bytes());
        self.body.push(0);
        self
    }
    fn bytes(mut self, b: &[u8]) -> Self {
        self.body.extend_from_slice(b);
        self
    }
}

/// Reads fields out of a frame body.
struct Body<'a>(&'a [u8]);

impl Body<'_> {
    fn err() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed frame")
    }
    fn i16(&mut self) -> std::io::Result<i16> {
        let (h, t) = self.0.split_at_checked(2).ok_or_else(Self::err)?;
        self.0 = t;
        Ok(i16::from_be_bytes([h[0], h[1]]))
    }
    fn i32(&mut self) -> std::io::Result<i32> {
        let (h, t) = self.0.split_at_checked(4).ok_or_else(Self::err)?;
        self.0 = t;
        Ok(i32::from_be_bytes([h[0], h[1], h[2], h[3]]))
    }
    fn cstr(&mut self) -> std::io::Result<String> {
        let end = self.0.iter().position(|&b| b == 0).ok_or_else(Self::err)?;
        let s = String::from_utf8_lossy(&self.0[..end]).into_owned();
        self.0 = &self.0[end + 1..];
        Ok(s)
    }
    fn take(&mut self, n: usize) -> std::io::Result<Vec<u8>> {
        let (h, t) = self.0.split_at_checked(n).ok_or_else(Self::err)?;
        self.0 = t;
        Ok(h.to_vec())
    }
}

struct Prepared {
    stmt: Option<Statement>,
    param_types: Vec<i32>,
}

struct Portal {
    stmt: Option<Statement>,
    params: Vec<Option<String>>,
    /// Result format codes from Bind (0 text, 1 binary; one code applies to all).
    formats: Vec<i16>,
    /// Rows left over from an Execute with a row limit.
    pending: Option<(Vec<Vec<Value>>, usize)>,
}

fn format_of(formats: &[i16], col: usize) -> i16 {
    match formats.len() {
        0 => 0,
        1 => formats[0],
        _ => formats.get(col).copied().unwrap_or(0),
    }
}

/// Microseconds between the Unix epoch and the Postgres epoch (2000-01-01).
const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;

/// The binary wire form of a value (what `typsend` produces).
fn binary(v: &Value) -> Option<Vec<u8>> {
    Some(match v {
        Value::Null => return None,
        Value::Bool(b) => vec![u8::from(*b)],
        Value::Int(i) => i.to_be_bytes().to_vec(),
        Value::Float(f) => f.to_be_bytes().to_vec(),
        Value::Text(s) => s.as_bytes().to_vec(),
        Value::Timestamp(t) => (t - PG_EPOCH_MICROS).to_be_bytes().to_vec(),
        Value::Bytes(b) => b.clone(),
    })
}

struct Conn {
    /// The user whose connection-limit slot this connection holds.
    slot: Option<String>,
    store: Arc<dyn KvStore>,
    rd: Reader,
    wr: BufWriter<Writer>,
    /// The startup message `negotiate` already read.
    pending: Option<Vec<u8>>,
    secure: bool,
    require_tls: bool,
    session: Session,
    pid: u32,
    prepared: HashMap<String, Prepared>,
    portals: HashMap<String, Portal>,
    /// After an error in the extended protocol, skip until Sync.
    skipping: bool,
}

impl Conn {
    fn new(
        rd: Reader,
        wr: Writer,
        store: Arc<dyn KvStore>,
        pid: u32,
        pending: Option<Vec<u8>>,
        secure: bool,
        require_tls: bool,
    ) -> Self {
        Self {
            slot: None,
            store: store.clone(),
            rd,
            wr: BufWriter::new(wr),
            pending,
            secure,
            require_tls,
            session: Session::new(store),
            pid,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            skipping: false,
        }
    }

    async fn send(&mut self, m: Msg) -> std::io::Result<()> {
        if trace_on() {
            tracing::info!(
                pid = self.pid,
                "pg ◄ {} len {} {}",
                m.kind as char,
                m.body.len() + 4,
                preview(&m.body)
            );
        }
        self.wr.write_u8(m.kind).await?;
        self.wr.write_i32(m.body.len() as i32 + 4).await?;
        self.wr.write_all(&m.body).await
    }

    async fn read_frame(&mut self) -> std::io::Result<(u8, Vec<u8>)> {
        let kind = self.rd.read_u8().await?;
        let len = self.rd.read_i32().await?;
        let mut body = vec![0; usize::try_from(len - 4).map_err(|_| Body::err())?];
        self.rd.read_exact(&mut body).await?;
        if trace_on() {
            tracing::info!(
                pid = self.pid,
                "pg ► {} len {len} {}",
                kind as char,
                preview(&body)
            );
        }
        Ok((kind, body))
    }

    async fn startup(&mut self) -> std::io::Result<bool> {
        loop {
            let body = match self.pending.take() {
                Some(b) => b,
                None => {
                    let len = self.rd.read_i32().await?;
                    let mut body = vec![0; usize::try_from(len - 4).map_err(|_| Body::err())?];
                    self.rd.read_exact(&mut body).await?;
                    body
                }
            };
            let len = body.len() + 4;
            let mut b = Body(&body);
            let code = b.i32()?;
            if trace_on() {
                tracing::info!(
                    pid = self.pid,
                    "pg ► startup code {code} len {len} {}",
                    preview(b.0)
                );
            }
            match code {
                SSL_REQUEST | GSSENC_REQUEST => {
                    // No TLS here yet (M7): the client continues in plain text.
                    self.wr.write_u8(b'N').await?;
                    self.wr.flush().await?;
                }
                CANCEL_REQUEST => return Ok(false),
                PROTOCOL_3 if self.require_tls && !self.secure => {
                    self.error_raw("28000", "this server requires TLS (sslmode=require)")
                        .await?;
                    self.wr.flush().await?;
                    return Ok(false);
                }
                PROTOCOL_3 => {
                    let mut params = HashMap::new();
                    while let Ok(k) = b.cstr() {
                        if k.is_empty() {
                            break;
                        }
                        params.insert(k, b.cstr()?);
                    }
                    tracing::debug!(?params, "pg startup");
                    let user = params.get("user").cloned().unwrap_or_default();
                    let required = match stowage_sql::auth::required(&self.store).await {
                        Ok(r) => r,
                        Err(e) => {
                            self.error_raw("58000", &e.to_string()).await?;
                            return Ok(false);
                        }
                    };
                    if required {
                        if !self.scram(&user).await? {
                            stowage_sql::audit::record("login_failed", &user, "SCRAM-SHA-256");
                            stowage_common::metrics::inc(
                                "stowage_sql_errors_total",
                                &[("sqlstate", "28P01")],
                                1.0,
                            );
                            self.error_raw(
                                "28P01",
                                &format!("password authentication failed for user \"{user}\""),
                            )
                            .await?;
                            self.wr.flush().await?;
                            return Ok(false);
                        }
                        stowage_sql::audit::record("login", &user, "SCRAM-SHA-256");
                        let limit = stowage_sql::auth::lookup(&self.store, &user)
                            .await
                            .ok()
                            .flatten()
                            .and_then(|u| u.connection_limit);
                        if !acquire_slot(&user, limit) {
                            self.error_raw(
                                "53300",
                                &format!("too many connections for role \"{user}\""),
                            )
                            .await?;
                            self.wr.flush().await?;
                            return Ok(false);
                        }
                        self.slot = Some(user.clone());
                        self.session.set_user(Some(user.clone()));
                    }
                    self.send(Msg::new(b'R').i32(0)).await?; // AuthenticationOk
                    for (k, v) in [
                        ("server_version", "16.0"),
                        ("server_encoding", "UTF8"),
                        ("client_encoding", "UTF8"),
                        ("DateStyle", "ISO, MDY"),
                        ("TimeZone", "UTC"),
                        ("integer_datetimes", "on"),
                        ("standard_conforming_strings", "on"),
                        (
                            "application_name",
                            params.get("application_name").map_or("", String::as_str),
                        ),
                    ] {
                        self.send(Msg::new(b'S').cstr(k).cstr(v)).await?;
                    }
                    self.send(Msg::new(b'K').i32(self.pid as i32).i32(0))
                        .await?;
                    self.ready().await?;
                    return Ok(true);
                }
                other => {
                    self.error_raw("08P01", &format!("unsupported protocol {other}"))
                        .await?;
                    return Ok(false);
                }
            }
        }
    }

    /// SCRAM-SHA-256 (RFC 5802 / 7677), the exchange Postgres uses:
    /// client-first → server-first (nonce, salt, iterations) → client-final with
    /// a proof → server-final with the server's signature.
    async fn scram(&mut self, user: &str) -> std::io::Result<bool> {
        use stowage_sql::auth;
        self.send(Msg::new(b'R').i32(10).cstr("SCRAM-SHA-256").byte(0))
            .await?;
        self.wr.flush().await?;
        let (kind, body) = self.read_frame().await?;
        if kind != b'p' {
            return Ok(false);
        }
        let mut b = Body(&body);
        let mech = b.cstr()?;
        let len = b.i32()?;
        let first =
            String::from_utf8_lossy(&b.take(usize::try_from(len).map_err(|_| Body::err())?)?)
                .into_owned();
        // gs2 header "n,," (no channel binding), then "n=…,r=<client nonce>".
        let Some(bare) = first.splitn(3, ',').nth(2).map(str::to_owned) else {
            return Ok(false);
        };
        let Some(cnonce) = bare
            .split(',')
            .find_map(|f| f.strip_prefix("r="))
            .map(str::to_owned)
        else {
            return Ok(false);
        };
        let rec = match auth::lookup(&self.store, user).await {
            Ok(Some(u)) if u.login => u.scram,
            _ => None,
        };
        // Unknown users get a full exchange with a made-up salt, so a client
        // cannot tell them from a wrong password.
        let secret = rec.clone().unwrap_or_else(|| {
            auth::scram_secret(
                &format!("{:x?}", auth::random_salt()),
                &auth::random_salt(),
                auth::ITERATIONS,
            )
        });
        let snonce = format!("{cnonce}{}", auth::b64_public(&auth::random_salt()));
        let server_first = format!("r={snonce},s={},i={}", secret.salt, secret.iterations);
        if mech != "SCRAM-SHA-256" {
            return Ok(false);
        }
        self.send(Msg::new(b'R').i32(11).bytes(server_first.as_bytes()))
            .await?;
        self.wr.flush().await?;
        let (kind, body) = self.read_frame().await?;
        if kind != b'p' {
            return Ok(false);
        }
        let fin = String::from_utf8_lossy(&body).into_owned();
        let Some((without_proof, proof)) = fin.rsplit_once(",p=") else {
            return Ok(false);
        };
        if !without_proof.split(',').any(|f| f == format!("r={snonce}")) {
            return Ok(false);
        }
        let Some(proof) = auth::unb64(proof) else {
            return Ok(false);
        };
        let auth_message = format!("{bare},{server_first},{without_proof}");
        match (rec, auth::scram_verify(&secret, &auth_message, &proof)) {
            (Some(_), Some(sig)) => {
                self.send(
                    Msg::new(b'R')
                        .i32(12)
                        .bytes(format!("v={}", auth::b64_public(&sig)).as_bytes()),
                )
                .await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn ready(&mut self) -> std::io::Result<()> {
        let s = match self.session.status() {
            TxStatus::Idle => b'I',
            TxStatus::InTransaction => b'T',
            TxStatus::Failed => b'E',
        };
        self.send(Msg::new(b'Z').byte(s)).await?;
        self.wr.flush().await
    }

    async fn error_raw(&mut self, code: &str, msg: &str) -> std::io::Result<()> {
        let m = Msg::new(b'E')
            .byte(b'S')
            .cstr("ERROR")
            .byte(b'V')
            .cstr("ERROR")
            .byte(b'C')
            .cstr(code)
            .byte(b'M')
            .cstr(msg)
            .byte(0);
        self.send(m).await
    }

    async fn error(&mut self, e: &stowage_sql::Error) -> std::io::Result<()> {
        self.error_raw(e.sqlstate(), &e.to_string()).await
    }

    async fn run(mut self) -> std::io::Result<()> {
        if !self.startup().await? {
            return Ok(());
        }
        loop {
            let (kind, body) = self.read_frame().await?;
            let mut b = Body(&body);
            if self.skipping && kind != b'S' {
                continue;
            }
            match kind {
                b'Q' => {
                    let sql = b.cstr()?;
                    self.simple_query(&sql).await?;
                    self.ready().await?;
                }
                b'P' => {
                    let name = b.cstr()?;
                    let sql = b.cstr()?;
                    let n = b.i16()?;
                    let param_types = (0..n)
                        .map(|_| b.i32())
                        .collect::<std::io::Result<Vec<_>>>()?;
                    match stowage_sql::session::parse(&sql) {
                        Ok(mut stmts) if stmts.len() <= 1 => {
                            self.prepared.insert(
                                name,
                                Prepared {
                                    stmt: stmts.pop(),
                                    param_types,
                                },
                            );
                            self.send(Msg::new(b'1')).await?;
                        }
                        Ok(_) => {
                            self.fail(&stowage_sql::Error::Parse(
                                "cannot insert multiple commands into a prepared statement".into(),
                            ))
                            .await?
                        }
                        Err(e) => self.fail(&e).await?,
                    }
                }
                b'B' => {
                    let portal = b.cstr()?;
                    let stmt = b.cstr()?;
                    let nf = b.i16()?;
                    let formats = (0..nf)
                        .map(|_| b.i16())
                        .collect::<std::io::Result<Vec<_>>>()?;
                    let np = b.i16()?;
                    let mut params = Vec::with_capacity(np.max(0) as usize);
                    let Some(p) = self.prepared.get(&stmt) else {
                        self.fail(&stowage_sql::Error::UndefinedObject(format!(
                            "prepared statement \"{stmt}\""
                        )))
                        .await?;
                        continue;
                    };
                    let (s, types) = (p.stmt.clone(), p.param_types.clone());
                    for i in 0..np as usize {
                        let len = b.i32()?;
                        if len < 0 {
                            params.push(None);
                            continue;
                        }
                        let raw = b.take(len as usize)?;
                        let binary = match formats.len() {
                            0 => false,
                            1 => formats[0] == 1,
                            _ => formats.get(i) == Some(&1),
                        };
                        params.push(Some(if binary {
                            decode_binary(types.get(i).copied().unwrap_or(0), &raw)
                        } else {
                            String::from_utf8_lossy(&raw).into_owned()
                        }));
                    }
                    let nr = b.i16()?;
                    let result_formats = (0..nr)
                        .map(|_| b.i16())
                        .collect::<std::io::Result<Vec<_>>>()?;
                    self.portals.insert(
                        portal,
                        Portal {
                            stmt: s,
                            params,
                            formats: result_formats,
                            pending: None,
                        },
                    );
                    self.send(Msg::new(b'2')).await?;
                }
                b'D' => {
                    let what = b.take(1)?[0];
                    let name = b.cstr()?;
                    let (stmt, params, types, formats) = if what == b'S' {
                        match self.prepared.get(&name) {
                            Some(p) => (
                                p.stmt.clone(),
                                Vec::new(),
                                Some(p.param_types.clone()),
                                Vec::new(),
                            ),
                            None => (None, Vec::new(), None, Vec::new()),
                        }
                    } else {
                        match self.portals.get(&name) {
                            Some(p) => (p.stmt.clone(), p.params.clone(), None, p.formats.clone()),
                            None => (None, Vec::new(), None, Vec::new()),
                        }
                    };
                    if let Some(types) = types {
                        let n = stmt
                            .as_ref()
                            .map_or(0, |s| count_params(&s.to_string()))
                            .max(types.len());
                        let mut m = Msg::new(b't').i16(n as i16);
                        for i in 0..n {
                            m = m.i32(match types.get(i).copied().unwrap_or(0) {
                                0 => 25,
                                t => t,
                            });
                        }
                        self.send(m).await?;
                    }
                    match stmt {
                        None => self.send(Msg::new(b'n')).await?,
                        Some(s) => match self.session.describe(&s, &params).await {
                            Ok(Some(cols)) => self.row_description(&cols, &formats).await?,
                            Ok(None) => self.send(Msg::new(b'n')).await?,
                            Err(e) => self.fail(&e).await?,
                        },
                    }
                }
                b'E' => {
                    let portal = b.cstr()?;
                    let max = b.i32()?;
                    self.execute(&portal, max).await?;
                }
                b'C' => {
                    let what = b.take(1)?[0];
                    let name = b.cstr()?;
                    if what == b'S' {
                        self.prepared.remove(&name);
                    } else {
                        self.portals.remove(&name);
                    }
                    self.send(Msg::new(b'3')).await?;
                }
                b'S' => {
                    self.skipping = false;
                    self.ready().await?;
                }
                b'H' => self.wr.flush().await?,
                b'X' => return Ok(()),
                other => {
                    self.error_raw("08P01", &format!("unsupported message {}", other as char))
                        .await?;
                    self.ready().await?;
                }
            }
        }
    }

    /// An error in the extended protocol: report it and skip until Sync.
    async fn fail(&mut self, e: &stowage_sql::Error) -> std::io::Result<()> {
        self.error(e).await?;
        self.skipping = true;
        Ok(())
    }

    async fn row_description(
        &mut self,
        cols: &[(String, DataType)],
        formats: &[i16],
    ) -> std::io::Result<()> {
        let mut m = Msg::new(b'T').i16(cols.len() as i16);
        for (i, (name, ty)) in cols.iter().enumerate() {
            let len: i16 = match ty {
                DataType::Bool => 1,
                DataType::Int | DataType::Float | DataType::Timestamp => 8,
                DataType::Text | DataType::Bytes => -1,
            };
            m = m
                .cstr(name)
                .i32(0)
                .i16(0)
                .i32(ty.pg_oid() as i32)
                .i16(len)
                .i32(-1)
                .i16(format_of(formats, i));
        }
        self.send(m).await
    }

    async fn data_rows(&mut self, rows: &[Vec<Value>], formats: &[i16]) -> std::io::Result<()> {
        for r in rows {
            let mut m = Msg::new(b'D').i16(r.len() as i16);
            for (i, v) in r.iter().enumerate() {
                let enc = if format_of(formats, i) == 1 {
                    binary(v)
                } else {
                    v.to_pg_text().map(String::into_bytes)
                };
                m = match enc {
                    None => m.i32(-1),
                    Some(b) => m.i32(b.len() as i32).bytes(&b),
                };
            }
            self.send(m).await?;
        }
        Ok(())
    }

    async fn simple_query(&mut self, sql: &str) -> std::io::Result<()> {
        let stmts = match stowage_sql::session::parse(sql) {
            Ok(s) => s,
            Err(e) => return self.error(&e).await,
        };
        if stmts.is_empty() {
            return self.send(Msg::new(b'I')).await;
        }
        for s in stmts {
            let started = std::time::Instant::now();
            let r = self.session.run(&s, &[]).await;
            record(&s, &r, started);
            if trace_on() {
                let t: Vec<String> = self
                    .session
                    .timings
                    .iter()
                    .map(|(k, v)| format!("{k} {v:.3} ms"))
                    .collect();
                tracing::info!(pid = self.pid, "pg statement timings: {}", t.join(" · "));
            }
            match r {
                Ok(out) => self.output(out, true).await?,
                Err(e) => return self.error(&e).await,
            }
        }
        Ok(())
    }

    async fn output(&mut self, out: Output, describe: bool) -> std::io::Result<()> {
        match out {
            Output::Rows { ref columns, .. } => {
                if describe {
                    self.row_description(columns, &[]).await?;
                }
                let rows = out.values();
                self.data_rows(&rows, &[]).await?;
                self.send(Msg::new(b'C').cstr(&format!("SELECT {}", rows.len())))
                    .await
            }
            Output::Command(tag) => self.send(Msg::new(b'C').cstr(&tag)).await,
        }
    }

    async fn execute(&mut self, portal: &str, max: i32) -> std::io::Result<()> {
        let Some(p) = self.portals.get_mut(portal) else {
            return self
                .fail(&stowage_sql::Error::UndefinedObject(format!(
                    "portal \"{portal}\""
                )))
                .await;
        };
        // Resume a portal suspended by a row limit.
        if let Some((rows, sent)) = p.pending.take() {
            return self.send_some(portal, rows, sent, max).await;
        }
        let Some(stmt) = p.stmt.clone() else {
            return self.send(Msg::new(b'I')).await;
        };
        let params = p.params.clone();
        let started = std::time::Instant::now();
        let r = self.session.run(&stmt, &params).await;
        record(&stmt, &r, started);
        match r {
            Ok(out @ Output::Rows { .. }) => {
                let rows = out.values();
                self.send_some(portal, rows, 0, max).await
            }
            Ok(out) => self.output(out, false).await,
            Err(e) => self.fail(&e).await,
        }
    }

    async fn send_some(
        &mut self,
        portal: &str,
        rows: Vec<Vec<Value>>,
        sent: usize,
        max: i32,
    ) -> std::io::Result<()> {
        let end = if max > 0 {
            (sent + max as usize).min(rows.len())
        } else {
            rows.len()
        };
        let formats = self
            .portals
            .get(portal)
            .map(|p| p.formats.clone())
            .unwrap_or_default();
        self.data_rows(&rows[sent..end], &formats).await?;
        if end < rows.len() {
            if let Some(p) = self.portals.get_mut(portal) {
                p.pending = Some((rows, end));
            }
            return self.send(Msg::new(b's')).await; // PortalSuspended
        }
        self.send(Msg::new(b'C').cstr(&format!("SELECT {}", rows.len())))
            .await
    }
}

/// Statement metrics: count and latency by kind, errors by SQLSTATE.
fn record(stmt: &Statement, r: &stowage_sql::Result<Output>, started: std::time::Instant) {
    use stowage_common::metrics;
    let text = stmt.to_string();
    let kind = text
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    metrics::inc("stowage_sql_statements_total", &[("kind", &kind)], 1.0);
    metrics::observe(
        "stowage_sql_statement_seconds",
        &[("kind", &kind)],
        started.elapsed().as_secs_f64(),
    );
    if let Err(e) = r {
        metrics::inc(
            "stowage_sql_errors_total",
            &[("sqlstate", e.sqlstate())],
            1.0,
        );
    }
}

/// The highest `$n` in a statement.
fn count_params(sql: &str) -> usize {
    let b = sql.as_bytes();
    let mut max = 0;
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'$' {
            let mut j = i + 1;
            while j < b.len() && b[j].is_ascii_digit() {
                j += 1;
            }
            if let Ok(n) = sql[i + 1..j].parse::<usize>() {
                max = max.max(n);
            }
            i = j;
        } else {
            i += 1;
        }
    }
    max
}

/// Binary parameters of the common fixed-width types, as text.
fn decode_binary(oid: i32, raw: &[u8]) -> String {
    match (oid, raw.len()) {
        (16, 1) => if raw[0] == 1 { "true" } else { "false" }.into(),
        (21, 2) => i16::from_be_bytes([raw[0], raw[1]]).to_string(),
        (23, 4) => i32::from_be_bytes(raw.try_into().unwrap_or([0; 4])).to_string(),
        (20, 8) => i64::from_be_bytes(raw.try_into().unwrap_or([0; 8])).to_string(),
        (701, 8) => f64::from_be_bytes(raw.try_into().unwrap_or([0; 8])).to_string(),
        (700, 4) => f32::from_be_bytes(raw.try_into().unwrap_or([0; 4])).to_string(),
        (1114 | 1184, 8) => stowage_sql::value::format_timestamp(
            i64::from_be_bytes(raw.try_into().unwrap_or([0; 8])) + PG_EPOCH_MICROS,
        ),
        _ => String::from_utf8_lossy(raw).into_owned(),
    }
}
