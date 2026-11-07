//! Running a [`Store`] for real.
//!
//! ```text
//!               tokio runtime                                   store thread (one per node)
//!  ┌───────────────────────────────────────────┐          ┌──────────────────────────────────┐
//!  │ listener ─► peer reader  ──Input::Peer───┐ │  std     │ loop {                           │
//!  │          └► client conn ──Input::Client─┼─┼──mpsc──► │   wait ≤ 1 tick for input        │
//!  │ peer writers ◄──────── PeerMsg ─────────┐│ │          │   drain everything queued        │
//!  │ client writers ◄────── Response ───────┐││ │          │   tick if due                    │
//!  └────────────────────────────────────────┼┼┼─┘          │   store.flush()  ← one fsync per │
//!                                           └┴┴──────────── │   route outputs     range/batch  │
//!                                                           └──────────────────────────────────┘
//! ```
//!
//! The store does synchronous disk I/O (fsync), so it lives on its own OS thread
//! instead of a tokio worker. Draining the whole input queue before calling
//! `flush()` is what turns concurrent client writes into one Raft entry batch and
//! one fsync: group commit falls out of the loop's shape.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use stowage_common::NodeId;
use stowage_common::os::OsDisk;
use stowage_kv::{PeerMsg, Request, Store, StoreConfig};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::wire::{self, HELLO, PEER, REQUEST, RESPONSE};

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub store: StoreConfig,
    pub listen: SocketAddr,
    /// Address of every node, including this one.
    pub peers: BTreeMap<NodeId, String>,
    pub data_dir: PathBuf,
    pub tick: Duration,
}

enum Input {
    Peer(NodeId, PeerMsg),
    Client { conn: u64, token: u64, req: Request },
    ClientGone(u64),
}

type ConnWriters = Arc<std::sync::Mutex<HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>>;

/// Start the node and run until the process exits.
pub async fn run(cfg: NodeConfig) -> std::io::Result<()> {
    let disk = Arc::new(OsDisk::open(&cfg.data_dir)?);
    let store = Store::open(
        cfg.store.clone(),
        disk,
        Arc::new(stowage_common::os::SystemClock),
    )
    .map_err(std::io::Error::other)?;
    let (in_tx, in_rx) = std_mpsc::channel::<Input>();
    let clients: ConnWriters = Arc::default();

    // One writer task per peer, each with a bounded queue: if a peer is down or
    // slow, its messages are dropped, which Raft treats as packet loss.
    let mut peer_tx: BTreeMap<NodeId, mpsc::Sender<Vec<u8>>> = BTreeMap::new();
    for (&id, addr) in &cfg.peers {
        if id == cfg.store.node {
            continue;
        }
        let (tx, rx) = mpsc::channel(8192);
        peer_tx.insert(id, tx);
        tokio::spawn(peer_writer(cfg.store.node, id, addr.clone(), rx));
    }

    let listener = TcpListener::bind(cfg.listen).await?;
    tracing::info!(node = cfg.store.node, addr = %cfg.listen, "listening");
    let next_conn = Arc::new(AtomicU64::new(1));
    let me = cfg.store.node;
    {
        let (in_tx, clients) = (in_tx.clone(), clients.clone());
        tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    continue;
                };
                let _ = sock.set_nodelay(true);
                let conn = next_conn.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(serve_conn(me, sock, conn, in_tx.clone(), clients.clone()));
            }
        });
    }

    let tick = cfg.tick;
    let thread = std::thread::Builder::new()
        .name(format!("store-{}", cfg.store.node))
        .spawn(move || store_loop(store, in_rx, peer_tx, clients, tick))?;
    tokio::task::spawn_blocking(move || thread.join())
        .await
        .map_err(std::io::Error::other)?
        .map_err(|_| std::io::Error::other("store thread panicked"))
}

fn store_loop(
    mut store: Store,
    rx: std_mpsc::Receiver<Input>,
    peers: BTreeMap<NodeId, mpsc::Sender<Vec<u8>>>,
    clients: ConnWriters,
    tick: Duration,
) {
    // Store tokens are global; remember which connection and client token each maps to.
    let mut next_token: u64 = 1;
    let mut routes: HashMap<u64, (u64, u64)> = HashMap::new();
    let mut next_tick = Instant::now() + tick;
    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        let first = match rx.recv_timeout(wait) {
            Ok(i) => Some(i),
            Err(std_mpsc::RecvTimeoutError::Timeout) => None,
            Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
        };
        let mut handle_input = |store: &mut Store, input: Input| {
            let r = match input {
                Input::Peer(from, m) => store.handle_peer(from, m),
                Input::Client { conn, token, req } => {
                    let t = next_token;
                    next_token += 1;
                    routes.insert(t, (conn, token));
                    store.handle_client(t, req)
                }
                Input::ClientGone(conn) => {
                    routes.retain(|_, (c, _)| *c != conn);
                    Ok(())
                }
            };
            if let Err(e) = r {
                tracing::error!(error = %e, "store input failed");
            }
        };
        let mut handled = 0usize;
        if let Some(i) = first {
            handle_input(&mut store, i);
            handled += 1;
        }
        while handled < 4096 {
            match rx.try_recv() {
                Ok(i) => {
                    handle_input(&mut store, i);
                    handled += 1;
                }
                Err(_) => break,
            }
        }
        let now = Instant::now();
        if now >= next_tick {
            if let Err(e) = store.tick() {
                tracing::error!(error = %e, "tick failed");
            }
            next_tick += tick;
            if next_tick < now {
                next_tick = now + tick; // fell behind: do not burst-tick
            }
        }
        if let Err(e) = store.flush() {
            tracing::error!(error = %e, "flush failed");
        }
        // Taking the outputs fsyncs the shared Raft log first (one fsync for all
        // ranges): nothing that depends on a vote or an entry leaves before it is
        // durable.
        let (msgs, responses) = match (store.take_peer_msgs(), store.take_responses()) {
            (Ok(m), Ok(r)) => (m, r),
            (Err(e), _) | (_, Err(e)) => {
                tracing::error!(error = %e, "raft log sync failed");
                std::process::exit(1); // fail-stop: never send what is not durable
            }
        };
        for (to, m) in msgs {
            if let Some(tx) = peers.get(&to) {
                let _ = tx.try_send(wire::frame(PEER, &m.encode()));
            }
        }
        if !responses.is_empty() {
            let writers = clients.lock().unwrap_or_else(|e| e.into_inner());
            for (t, resp) in responses {
                if let Some((conn, token)) = routes.remove(&t)
                    && let Some(w) = writers.get(&conn)
                {
                    let _ = w.send(wire::token_frame(RESPONSE, token, &resp.encode()));
                }
            }
        }
    }
}

/// Read the receiver's HELLO and check it is the node we meant to reach. A stale
/// address, a reused IP or a port taken by another process (we hit this with a
/// Docker cluster publishing the same ports) would otherwise silently deliver
/// Raft traffic to the wrong node.
async fn handshake(sock: &mut TcpStream, me: NodeId, peer: NodeId) -> std::io::Result<()> {
    sock.write_all(&wire::frame(HELLO, &me.to_le_bytes()))
        .await?;
    let (mut rd, _) = sock.split();
    let reply = tokio::time::timeout(Duration::from_secs(2), wire::read_frame(&mut rd))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no HELLO reply"))??;
    match reply {
        Some((HELLO, payload)) if wire::split_token(&payload).map(|(id, _)| id) == Some(peer) => {
            Ok(())
        }
        other => Err(std::io::Error::other(format!(
            "expected node {peer} at this address, got {:?}",
            other.and_then(|(_, p)| wire::split_token(&p).map(|(id, _)| id))
        ))),
    }
}

async fn peer_writer(me: NodeId, peer: NodeId, addr: String, mut rx: mpsc::Receiver<Vec<u8>>) {
    let mut backoff = Duration::from_millis(50);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(mut sock) => {
                let _ = sock.set_nodelay(true);
                if let Err(e) = handshake(&mut sock, me, peer).await {
                    tracing::warn!(peer, %addr, error = %e, "peer handshake failed");
                    while rx.try_recv().is_ok() {}
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(1));
                    continue;
                }
                backoff = Duration::from_millis(50);
                let mut w = BufWriter::new(sock);
                tracing::info!(peer, %addr, "connected to peer");
                loop {
                    let Some(first) = rx.recv().await else { return };
                    let mut ok = w.write_all(&first).await.is_ok();
                    // Coalesce whatever else is queued into the same flush.
                    while ok {
                        match rx.try_recv() {
                            Ok(f) => ok = w.write_all(&f).await.is_ok(),
                            Err(_) => break,
                        }
                    }
                    if !ok || w.flush().await.is_err() {
                        tracing::warn!(peer, "peer connection lost");
                        break;
                    }
                }
            }
            Err(_) => {
                // Drop what queued up while the peer was unreachable.
                while rx.try_recv().is_ok() {}
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
        }
    }
}

async fn serve_conn(
    me: NodeId,
    sock: TcpStream,
    conn: u64,
    in_tx: std_mpsc::Sender<Input>,
    clients: ConnWriters,
) {
    let (mut rd, mut wr) = sock.into_split();
    let Ok(Some((kind, payload))) = wire::read_frame(&mut rd).await else {
        return;
    };
    if kind == HELLO {
        let Some((from, _)) = wire::split_token(&payload) else {
            return;
        };
        if wr
            .write_all(&wire::frame(HELLO, &me.to_le_bytes()))
            .await
            .is_err()
        {
            return;
        }
        while let Ok(Some((kind, payload))) = wire::read_frame(&mut rd).await {
            if kind != PEER {
                continue;
            }
            match PeerMsg::decode(&payload) {
                Ok(m) => {
                    if in_tx.send(Input::Peer(from, m)).is_err() {
                        return;
                    }
                }
                Err(e) => tracing::warn!(from, error = %e, "bad peer message"),
            }
        }
        return;
    }
    // A client connection: responses go out through a dedicated writer task.
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    clients
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(conn, tx);
    tokio::spawn(async move {
        let mut w = BufWriter::new(wr);
        while let Some(f) = rx.recv().await {
            if w.write_all(&f).await.is_err() {
                return;
            }
            while let Ok(f) = rx.try_recv() {
                if w.write_all(&f).await.is_err() {
                    return;
                }
            }
            if w.flush().await.is_err() {
                return;
            }
        }
    });
    let mut next = Some((kind, payload));
    while let Some((kind, payload)) = next {
        if kind == REQUEST
            && let Some((token, body)) = wire::split_token(&payload)
            && let Ok(req) = Request::decode(body)
            && in_tx.send(Input::Client { conn, token, req }).is_err()
        {
            break;
        }
        next = wire::read_frame(&mut rd).await.ok().flatten();
    }
    clients
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&conn);
    let _ = in_tx.send(Input::ClientGone(conn));
}
