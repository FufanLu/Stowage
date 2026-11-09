//! Running a [`Store`] for real.
//!
//! ```text
//!               tokio runtime                          store thread (one per node)        syncer thread
//!  ┌──────────────────────────────────────────┐      ┌───────────────────────────────┐   ┌──────────────────┐
//!  │ listener ─► peer reader  ──Input::Peer──┐ │ std  │ loop {                        │   │ take every job   │
//!  │          └► client conn ──Input::Client─┼─┼─mpsc─► │   wait ≤ 1 tick, drain input  │   │ queued, write    │
//!  │ peer writers ◄───────── PeerMsg ───────┐│ │      │   tick if due, store.flush()  │   │ them all, ONE    │
//!  │ client writers ◄─────── Response ─────┐││ │      │   stage(): Appends + replies  ├──►│ fsync, hand each │
//!  │                                       │││ │      │   leave now; the rest waits ──┤   │ back as Synced   │
//!  └───────────────────────────────────────┴┴┴─┘ ◄────┤   Synced: durable(), send it  │◄──┤                  │
//!                                                     └───────────────────────────────┘   └──────────────────┘
//! ```
//!
//! The store thread never blocks on the disk: while the syncer fsyncs one
//! batch, input keeps arriving and becomes the next batch. Draining the whole
//! input queue before `flush()` turns concurrent client writes into one Raft
//! entry batch per range; the syncer then covers every batch queued during an
//! fsync with one fsync. Group commit falls out of the shape of both loops.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant};

use stowage_common::NodeId;
use stowage_common::os::OsDisk;
use stowage_kv::{PeerMsg, Request, Response, Store, StoreConfig};
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
    /// Benchmarks only: see [`OsDisk::skip_fsync`].
    pub unsafe_skip_fsync: bool,
}

enum Input {
    Peer(NodeId, PeerMsg),
    Client {
        conn: u64,
        token: u64,
        req: Request,
    },
    ClientGone(u64),
    /// The syncer made a staged batch durable; these held-back peer messages
    /// may now leave.
    Synced {
        marks: Vec<(stowage_kv::RangeId, u64)>,
        msgs: Vec<(NodeId, PeerMsg)>,
    },
    /// The node's tasks are gone (runtime shut down): stop the store thread.
    /// Needed because the syncer holds an input sender of its own, so the
    /// channel never disconnects by itself.
    Stop,
}

/// Sends [`Input::Stop`] when dropped, i.e. when the task owning it is.
struct StopOnDrop(std_mpsc::Sender<Input>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(Input::Stop);
    }
}

/// A staged batch on its way to disk. Jobs are completed strictly in order; a
/// job without log bytes only keeps its messages behind the batches before it.
struct SyncJob {
    log: Option<stowage_raft::log_engine::LogBatch>,
    marks: Vec<(stowage_kv::RangeId, u64)>,
    msgs: Vec<(NodeId, PeerMsg)>,
}

/// Writes and fsyncs staged log batches off the store thread, in order, and
/// hands each one back as [`Input::Synced`]. Every job queued while an fsync
/// runs is written by the next pass and covered by one fsync: group commit
/// across store-loop rounds.
fn syncer(jobs: std_mpsc::Receiver<SyncJob>, back: std_mpsc::Sender<Input>) {
    while let Ok(first) = jobs.recv() {
        let mut pass = vec![first];
        pass.extend(jobs.try_iter());
        let started = Instant::now();
        let logs: Vec<_> = pass.iter().filter_map(|j| j.log.as_ref()).collect();
        let mut result = logs.iter().try_for_each(|l| l.write_unsynced());
        for (i, l) in logs.iter().enumerate() {
            // The last batch for each file (the log is rewritten into a new one
            // only when nothing is in flight, so in practice there is one).
            let last_for_file = logs[i + 1..].iter().all(|m| !l.same_file(m));
            if result.is_ok() && last_for_file {
                result = l.sync();
            }
        }
        if let Err(e) = result {
            tracing::error!(error = %e, "raft log sync failed");
            std::process::exit(1); // fail-stop: never send what is not durable
        }
        if !logs.is_empty() {
            stowage_common::metrics::observe(
                "stowage_raft_log_sync_seconds",
                &[],
                started.elapsed().as_secs_f64(),
            );
        }
        for job in pass {
            let synced = Input::Synced {
                marks: job.marks,
                msgs: job.msgs,
            };
            if back.send(synced).is_err() {
                return;
            }
        }
    }
}

/// May this message leave before the log batch staged with it is durable?
/// Only a leader's `Append`: replicating entries while writing them locally is
/// thesis §10.2.1, and the leader counts its own copy towards a quorum only once
/// [`Store::durable`] says so. Votes, acknowledgements and everything else
/// promise something about this node's disk and wait.
fn sendable_early(m: &PeerMsg) -> bool {
    matches!(
        m,
        PeerMsg::Raft {
            msg: stowage_raft::Message {
                body: stowage_raft::Body::Append { .. },
                ..
            },
            ..
        }
    )
}

type ConnWriters = Arc<std::sync::Mutex<HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>>;

/// Detecting a restarted peer. Each process picks an incarnation at start and
/// sends it in its HELLO. When a peer connects to us with an incarnation we
/// have not seen, it restarted, possibly at a new IP (a rescheduled pod):
/// our outbound connection to it may now lead nowhere, and writes into a
/// black-holed TCP connection do not fail for minutes. So we redial. A redial
/// carries our unchanged incarnation, so it never triggers one in return.
#[derive(Default)]
struct Peers {
    /// Last incarnation seen per peer.
    seen: std::sync::Mutex<HashMap<NodeId, u64>>,
    /// Wakes a peer's writer to drop its connection and dial again.
    redial: HashMap<NodeId, Arc<tokio::sync::Notify>>,
}

impl Peers {
    fn observe(&self, peer: NodeId, incarnation: u64) {
        let changed = {
            let mut seen = self
                .seen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            seen.insert(peer, incarnation)
                .is_some_and(|old| old != incarnation)
        };
        if changed && let Some(n) = self.redial.get(&peer) {
            tracing::info!(peer, "peer restarted: redialing it");
            n.notify_one();
        }
    }
}

fn hello(me: NodeId, incarnation: u64) -> Vec<u8> {
    let mut p = me.to_le_bytes().to_vec();
    p.extend_from_slice(&incarnation.to_le_bytes());
    wire::frame(HELLO, &p)
}

/// Keepalive probes, and on Linux a cap on unacknowledged data, so a
/// connection to a vanished address fails in seconds instead of minutes.
fn tune(sock: &TcpStream) {
    let _ = sock.set_nodelay(true);
    let s = socket2::SockRef::from(sock);
    let ka = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(5))
        .with_interval(Duration::from_secs(2));
    let _ = s.set_tcp_keepalive(&ka);
    #[cfg(target_os = "linux")]
    let _ = s.set_tcp_user_timeout(Some(Duration::from_secs(10)));
}

/// Start the node and run until the process exits.
pub async fn run(cfg: NodeConfig) -> std::io::Result<()> {
    let disk = Arc::new(OsDisk::open(&cfg.data_dir)?.skip_fsync(cfg.unsafe_skip_fsync));
    if cfg.unsafe_skip_fsync {
        tracing::warn!("fsync disabled: acknowledged writes do not survive power loss");
    }
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
    let incarnation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
        | 1;
    let mut peer_tx: BTreeMap<NodeId, mpsc::Sender<Vec<u8>>> = BTreeMap::new();
    let mut redial = HashMap::new();
    let mut writers = Vec::new();
    for (&id, addr) in &cfg.peers {
        if id == cfg.store.node {
            continue;
        }
        let (tx, rx) = mpsc::channel(8192);
        peer_tx.insert(id, tx);
        let n = Arc::new(tokio::sync::Notify::new());
        redial.insert(id, n.clone());
        writers.push((id, addr.clone(), rx, n));
    }
    let peers = Arc::new(Peers {
        seen: std::sync::Mutex::default(),
        redial,
    });
    for (id, addr, rx, n) in writers {
        tokio::spawn(peer_writer(
            cfg.store.node,
            incarnation,
            id,
            addr,
            rx,
            n,
            peers.clone(),
        ));
    }

    let listener = TcpListener::bind(cfg.listen).await?;
    tracing::info!(node = cfg.store.node, addr = %cfg.listen, "listening");
    let next_conn = Arc::new(AtomicU64::new(1));
    let me = cfg.store.node;
    {
        let (in_tx, clients, peers) = (in_tx.clone(), clients.clone(), peers.clone());
        let stop = StopOnDrop(in_tx.clone());
        tokio::spawn(async move {
            let _stop = stop;
            loop {
                let Ok((sock, _)) = listener.accept().await else {
                    continue;
                };
                tune(&sock);
                let conn = next_conn.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(serve_conn(
                    me,
                    incarnation,
                    sock,
                    conn,
                    in_tx.clone(),
                    clients.clone(),
                    peers.clone(),
                ));
            }
        });
    }

    let tick = cfg.tick;
    let thread = std::thread::Builder::new()
        .name(format!("store-{}", cfg.store.node))
        .spawn(move || store_loop(store, in_rx, in_tx, peer_tx, clients, tick))?;
    tokio::task::spawn_blocking(move || thread.join())
        .await
        .map_err(std::io::Error::other)?
        .map_err(|_| std::io::Error::other("store thread panicked"))
}

fn store_loop(
    mut store: Store,
    rx: std_mpsc::Receiver<Input>,
    back: std_mpsc::Sender<Input>,
    peers: BTreeMap<NodeId, mpsc::Sender<Vec<u8>>>,
    clients: ConnWriters,
    tick: Duration,
) {
    // Store tokens are global; remember which connection and client token each maps to.
    let mut next_token: u64 = 1;
    let mut routes: HashMap<u64, (u64, u64)> = HashMap::new();
    let mut next_tick = Instant::now() + tick;
    let mut next_metrics = Instant::now();
    let (job_tx, job_rx) = std_mpsc::channel::<SyncJob>();
    if let Err(e) = std::thread::Builder::new()
        .name("raft-log-sync".into())
        .spawn(move || syncer(job_rx, back))
    {
        tracing::error!(error = %e, "cannot start the log syncer");
        std::process::exit(1);
    }
    // Jobs handed to the syncer and not yet back.
    let mut in_flight = 0usize;
    let send_peer = |to: NodeId, m: &PeerMsg| {
        if let Some(tx) = peers.get(&to) {
            let _ = tx.try_send(wire::frame(PEER, &m.encode()));
        }
    };
    loop {
        let wait = next_tick.saturating_duration_since(Instant::now());
        let first = match rx.recv_timeout(wait) {
            Ok(Input::Stop) => return,
            Ok(i) => Some(i),
            Err(std_mpsc::RecvTimeoutError::Timeout) => None,
            Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
        };
        let mut handle_input = |store: &mut Store, input: Input| {
            let r = match input {
                Input::Peer(from, m) => store.handle_peer(from, m),
                Input::Client { conn, token, req } => {
                    QUEUED.fetch_sub(1, Ordering::Relaxed);
                    stowage_common::metrics::inc("stowage_kv_requests_total", &[], 1.0);
                    let t = next_token;
                    next_token += 1;
                    routes.insert(t, (conn, token));
                    store.handle_client(t, req)
                }
                Input::ClientGone(conn) => {
                    routes.retain(|_, (c, _)| *c != conn);
                    Ok(())
                }
                Input::Stop => Ok(()),
                Input::Synced { marks, msgs } => {
                    in_flight -= 1;
                    for (to, m) in &msgs {
                        send_peer(*to, m);
                    }
                    let r = store.durable(&marks);
                    if r.is_ok() && in_flight == 0 {
                        store.maintain_log()
                    } else {
                        r
                    }
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
                Ok(Input::Stop) => return,
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
        // Stage the outputs. The log bytes go to the syncer thread, which fsyncs
        // them (one fsync for every range) while this thread keeps taking input:
        // whatever arrives meanwhile becomes the next, larger group commit.
        // Client responses never depend on this node's unsynced log (a write is
        // acknowledged only once a quorum has persisted it; reads see applied,
        // hence committed, state), so they leave now. So does a leader's
        // `Append`. Every other peer message waits for its batch, and for every
        // batch before it.
        let st = store.stage();
        let responses = st.responses;
        let (early, late): (Vec<_>, Vec<_>) = st
            .peer_msgs
            .into_iter()
            .partition(|(_, m)| sendable_early(m));
        for (to, m) in &early {
            send_peer(*to, m);
        }
        if st.log.is_none() && (in_flight == 0 || late.is_empty()) {
            // Nothing new to persist, and nothing earlier to wait for.
            for (to, m) in &late {
                send_peer(*to, m);
            }
        } else {
            in_flight += 1;
            let job = SyncJob {
                log: st.log,
                marks: st.marks,
                msgs: late,
            };
            if job_tx.send(job).is_err() {
                tracing::error!("log syncer gone");
                std::process::exit(1);
            }
        }
        if Instant::now() >= next_metrics {
            next_metrics = Instant::now() + Duration::from_secs(1);
            export_metrics(&store);
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

fn export_metrics(store: &Store) {
    use stowage_common::metrics::set;
    let s = store.stats();
    let (r, replicas, leaders) = store.raft_totals();
    for (name, v) in [
        ("stowage_kv_proposals_total", s.proposals),
        ("stowage_kv_proposal_batches_total", s.batches),
        ("stowage_kv_applied_total", s.applied),
        ("stowage_kv_splits_total", s.splits),
        (
            "stowage_kv_snapshots_installed_total",
            s.snapshots_installed,
        ),
        ("stowage_kv_snapshots_sent_total", s.snapshots_sent),
        ("stowage_raft_elections_total", r.elections),
        ("stowage_raft_terms_led_total", r.terms_led),
        ("stowage_raft_entries_sent_total", r.entries_sent),
        ("stowage_raft_append_rejections_total", r.rejections),
        ("stowage_raft_reads_index_total", r.reads_index),
    ] {
        set(name, &[], v as f64);
    }
    set("stowage_replicas", &[], replicas as f64);
    set(
        "stowage_kv_queued_requests",
        &[],
        QUEUED.load(Ordering::Relaxed) as f64,
    );
    set("stowage_range_leaders", &[], leaders as f64);
}

/// Read the receiver's HELLO and check it is the node we meant to reach. A stale
/// address, a reused IP or a port taken by another process (we hit this with a
/// Docker cluster publishing the same ports) would otherwise silently deliver
/// Raft traffic to the wrong node.
async fn handshake(
    sock: &mut TcpStream,
    me: NodeId,
    incarnation: u64,
    peer: NodeId,
    peers: &Peers,
) -> std::io::Result<()> {
    sock.write_all(&hello(me, incarnation)).await?;
    let (mut rd, _) = sock.split();
    let reply = tokio::time::timeout(Duration::from_secs(2), wire::read_frame(&mut rd))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no HELLO reply"))??;
    match reply {
        Some((HELLO, payload)) if wire::split_token(&payload).map(|(id, _)| id) == Some(peer) => {
            if let Some((_, rest)) = wire::split_token(&payload)
                && let Some((inc, _)) = wire::split_token(rest)
            {
                peers.observe(peer, inc);
            }
            Ok(())
        }
        other => Err(std::io::Error::other(format!(
            "expected node {peer} at this address, got {:?}",
            other.and_then(|(_, p)| wire::split_token(&p).map(|(id, _)| id))
        ))),
    }
}

async fn peer_writer(
    me: NodeId,
    incarnation: u64,
    peer: NodeId,
    addr: String,
    mut rx: mpsc::Receiver<Vec<u8>>,
    redial: Arc<tokio::sync::Notify>,
    peers: Arc<Peers>,
) {
    let mut backoff = Duration::from_millis(50);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(mut sock) => {
                tune(&sock);
                if let Err(e) = handshake(&mut sock, me, incarnation, peer, &peers).await {
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
                    let first = tokio::select! {
                        f = rx.recv() => match f {
                            Some(f) => f,
                            None => return,
                        },
                        () = redial.notified() => break,
                    };
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

/// Client requests waiting for the store thread. Above [`MAX_QUEUED`] new ones
/// are refused with `Busy` (which clients retry with backoff) instead of
/// growing the queue and every request's latency without bound.
static QUEUED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
const MAX_QUEUED: usize = 20_000;

async fn serve_conn(
    me: NodeId,
    incarnation: u64,
    sock: TcpStream,
    conn: u64,
    in_tx: std_mpsc::Sender<Input>,
    clients: ConnWriters,
    peers: Arc<Peers>,
) {
    let (mut rd, mut wr) = sock.into_split();
    let Ok(Some((kind, payload))) = wire::read_frame(&mut rd).await else {
        return;
    };
    if kind == HELLO {
        let Some((from, rest)) = wire::split_token(&payload) else {
            return;
        };
        if let Some((inc, _)) = wire::split_token(rest) {
            peers.observe(from, inc);
        }
        if wr.write_all(&hello(me, incarnation)).await.is_err() {
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
        .insert(conn, tx.clone());
    let busy_tx = tx;
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
        {
            if QUEUED.load(Ordering::Relaxed) >= MAX_QUEUED {
                stowage_common::metrics::inc("stowage_kv_requests_shed_total", &[], 1.0);
                let busy = Response::Error(stowage_kv::KvError::Busy("node overloaded".into()));
                let _ = busy_tx.send(wire::token_frame(RESPONSE, token, &busy.encode()));
            } else {
                QUEUED.fetch_add(1, Ordering::Relaxed);
                if in_tx.send(Input::Client { conn, token, req }).is_err() {
                    break;
                }
            }
        }
        next = wire::read_frame(&mut rd).await.ok().flatten();
    }
    clients
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(&conn);
    let _ = in_tx.send(Input::ClientGone(conn));
}

#[cfg(test)]
mod restart_detection {
    use super::*;

    #[tokio::test]
    async fn a_new_incarnation_redials_and_the_same_one_does_not() {
        let n = Arc::new(tokio::sync::Notify::new());
        let peers = Peers {
            seen: std::sync::Mutex::default(),
            redial: HashMap::from([(2, n.clone())]),
        };
        let fired = || async {
            tokio::time::timeout(Duration::from_millis(50), n.notified())
                .await
                .is_ok()
        };
        peers.observe(2, 100); // first contact: nothing to redial
        assert!(!fired().await);
        peers.observe(2, 100); // our redial reaching it again: same process
        assert!(!fired().await);
        peers.observe(2, 200); // it restarted
        assert!(fired().await);
        peers.observe(3, 7); // a peer we do not dial: ignored
        assert!(!fired().await);
    }
}
