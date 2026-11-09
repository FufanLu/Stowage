//! Async client: connection multiplexing, range-aware routing, retries.
//!
//! Requests carry a token; one TCP connection per node carries any number of
//! concurrent requests. The client caches `RangeInfo`s (span + leader) learned
//! from `Locate`, and on a routing error asks the node it reached where the key
//! lives now.
//!
//! Writes are never retried after they may have been proposed: a timeout or an
//! `Ambiguous` reply is returned to the caller, who knows whether re-applying the
//! write is safe. Transactions ([`KvClient::transact`]) are different: every
//! request a transaction coordinator sends is idempotent, so all of them are
//! retried.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use stowage_common::os::{SystemClock, Xoshiro256};
use stowage_common::{Hlc, NodeId, Rng, Timestamp};
use stowage_kv::txn::{Isolation, OpResult, Outcome, Program, Step, TxnCoord, TxnOp};
use stowage_kv::{KvError, RangeInfo, Request, Response};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::wire::{self, REQUEST, RESPONSE};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("timed out")]
    Timeout,
    #[error("the write may or may not have been applied")]
    Ambiguous,
    #[error("gave up after {0} attempts: {1}")]
    Exhausted(u32, String),
    #[error("unexpected response: {0:?}")]
    Unexpected(Response),
}

type Pending = Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<Response>>>>;

#[derive(Debug)]
struct Conn {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    pending: Pending,
}

#[derive(Debug)]
pub struct KvClient {
    addrs: BTreeMap<NodeId, String>,
    conns: Mutex<HashMap<NodeId, Arc<Conn>>>,
    ranges: std::sync::Mutex<BTreeMap<Vec<u8>, RangeInfo>>,
    next_token: AtomicU64,
    hlc: Hlc,
    rng: std::sync::Mutex<Xoshiro256>,
    pub timeout: Duration,
}

impl KvClient {
    pub fn new(addrs: BTreeMap<NodeId, String>) -> Self {
        Self {
            addrs,
            conns: Mutex::new(HashMap::new()),
            ranges: std::sync::Mutex::new(BTreeMap::new()),
            next_token: AtomicU64::new(1),
            hlc: Hlc::new(Arc::new(SystemClock), 500_000_000),
            rng: std::sync::Mutex::new(Xoshiro256::from_entropy()),
            timeout: Duration::from_secs(3),
        }
    }

    pub fn nodes(&self) -> Vec<NodeId> {
        self.addrs.keys().copied().collect()
    }

    async fn conn(&self, node: NodeId) -> Result<Arc<Conn>, ClientError> {
        let mut conns = self.conns.lock().await;
        if let Some(c) = conns.get(&node)
            && !c.tx.is_closed()
        {
            return Ok(c.clone());
        }
        let addr = self
            .addrs
            .get(&node)
            .ok_or_else(|| ClientError::Exhausted(0, format!("unknown node {node}")))?;
        let sock = TcpStream::connect(addr).await?;
        let _ = sock.set_nodelay(true);
        let (mut rd, wr) = sock.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let pending: Pending = Arc::default();
        let p = pending.clone();
        tokio::spawn(async move {
            while let Ok(Some((kind, payload))) = wire::read_frame(&mut rd).await {
                if kind != RESPONSE {
                    continue;
                }
                if let Some((token, body)) = wire::split_token(&payload)
                    && let Ok(resp) = Response::decode(body)
                    && let Some(waiter) = p.lock().unwrap_or_else(|e| e.into_inner()).remove(&token)
                {
                    let _ = waiter.send(resp);
                }
            }
            p.lock().unwrap_or_else(|e| e.into_inner()).clear(); // wake everyone with an error
        });
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
        let c = Arc::new(Conn { tx, pending });
        conns.insert(node, c.clone());
        Ok(c)
    }

    /// One request to one node.
    pub async fn call(&self, node: NodeId, req: &Request) -> Result<Response, ClientError> {
        let conn = self.conn(node).await?;
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        conn.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(token, tx);
        if conn
            .tx
            .send(wire::token_frame(REQUEST, token, &req.encode()))
            .is_err()
        {
            self.conns.lock().await.remove(&node);
            return Err(ClientError::Io(std::io::Error::other("connection closed")));
        }
        match tokio::time::timeout(self.timeout, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => {
                self.conns.lock().await.remove(&node);
                Err(ClientError::Io(std::io::Error::other("connection closed")))
            }
            Err(_) => {
                conn.pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&token);
                Err(ClientError::Timeout)
            }
        }
    }

    fn cached(&self, key: &[u8]) -> Option<NodeId> {
        let ranges = self.ranges.lock().unwrap_or_else(|e| e.into_inner());
        let (_, info) = ranges.range(..=key.to_vec()).next_back()?;
        info.desc.contains(key).then_some(info.leader).flatten()
    }

    fn remember(&self, info: RangeInfo) {
        let mut ranges = self.ranges.lock().unwrap_or_else(|e| e.into_inner());
        ranges.retain(|_, r| !r.desc.overlaps(&info.desc));
        ranges.insert(info.desc.start.clone(), info);
    }

    fn set_leader(&self, key: &[u8], leader: NodeId) {
        let mut ranges = self.ranges.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, info)) = ranges.range_mut(..=key.to_vec()).next_back()
            && info.desc.contains(key)
        {
            info.leader = Some(leader);
        }
    }

    /// Route `req` to the leader of the range holding its key, following
    /// redirects. Reads are retried on any error; writes only while they cannot
    /// have been proposed.
    pub async fn request(&self, req: Request) -> Result<Response, ClientError> {
        let is_write = matches!(
            req,
            Request::Put { .. } | Request::Delete { .. } | Request::Split { .. }
        );
        self.route(req, !is_write).await
    }

    /// [`Self::request`], with `idempotent` deciding whether a request that may
    /// have been proposed can be sent again.
    async fn route(&self, req: Request, idempotent: bool) -> Result<Response, ClientError> {
        let key = req.routing_key().unwrap_or_default().to_vec();
        let is_write = !idempotent;
        let nodes = self.nodes();
        let mut target = self.cached(&key).unwrap_or(nodes[0]);
        let mut last_err = String::new();
        // Following a leader hint is not a failure: no backoff for that.
        let mut backoff = false;
        for attempt in 0..30u32 {
            if attempt > 0 && backoff {
                tokio::time::sleep(Duration::from_millis(u64::from(attempt.min(10)) * 20)).await;
            }
            backoff = true;
            let resp = match self.call(target, &req).await {
                Ok(r) => r,
                Err(ClientError::Timeout) if is_write => return Err(ClientError::Ambiguous),
                Err(e) => {
                    last_err = e.to_string();
                    let i = nodes.iter().position(|&n| n == target).unwrap_or(0);
                    target = nodes[(i + 1) % nodes.len()];
                    continue;
                }
            };
            match resp {
                Response::Error(KvError::Ambiguous) if is_write => {
                    return Err(ClientError::Ambiguous);
                }
                Response::Error(KvError::Ambiguous) => last_err = "ambiguous".into(),
                Response::Error(KvError::NotLeader {
                    leader: Some(l), ..
                }) if l != target => {
                    if self.cached(&key).is_none() {
                        // Learn the range's span from its leader, so the next
                        // request for any key in it goes straight there.
                        if let Ok(Response::Location(info)) =
                            self.call(l, &Request::Locate { key: key.clone() }).await
                        {
                            self.remember(info);
                        }
                    }
                    self.set_leader(&key, l);
                    target = l;
                    backoff = false;
                }
                Response::Error(
                    e @ (KvError::NotLeader { .. }
                    | KvError::RangeNotFound { .. }
                    | KvError::KeyMoved),
                ) => {
                    last_err = e.to_string();
                    // Ask the node we reached where the key lives now.
                    if let Ok(Response::Location(info)) = self
                        .call(target, &Request::Locate { key: key.clone() })
                        .await
                    {
                        let leader = info.leader;
                        self.remember(info);
                        if let Some(l) = leader {
                            // A different leader: try it now. The same node again
                            // (it is catching up on a split): wait a little.
                            backoff = l == target;
                            target = l;
                            continue;
                        }
                    }
                    if let KvError::RangeNotFound { hint: Some(h) } = e {
                        target = h;
                    } else {
                        let i = nodes.iter().position(|&n| n == target).unwrap_or(0);
                        target = nodes[(i + 1) % nodes.len()];
                    }
                }
                Response::Error(KvError::Busy(m)) => last_err = m,
                ok => {
                    if self.cached(&key).is_none()
                        && let Ok(Response::Location(info)) = self
                            .call(target, &Request::Locate { key: key.clone() })
                            .await
                    {
                        self.remember(info);
                    }
                    self.set_leader(&key, target);
                    return Ok(ok);
                }
            }
        }
        Err(ClientError::Exhausted(30, last_err))
    }

    /// A linearizable read of the newest committed value. A transaction's
    /// pending intent on the key blocks it: the read waits for that writer,
    /// resolving the intent itself once the writer has committed or aborted (a
    /// committed transaction's intents may outlive its acknowledgement
    /// briefly, and skipping one would miss an acknowledged write).
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        use stowage_kv::mvcc::TxnStatus;
        use stowage_kv::types::PushKind;
        for attempt in 0..2_000u32 {
            let txn = match self.request(Request::Get { key: key.to_vec() }).await? {
                Response::Value(v) => return Ok(v),
                Response::Conflict(txn) => txn,
                other => return Err(ClientError::Unexpected(other)),
            };
            // Priority 0 never moves a live writer; it reports its state (and
            // aborts it only if its heartbeats stopped).
            let push = Request::Push {
                pushee: txn.clone(),
                kind: PushKind::Timestamp {
                    ts: txn.write_ts,
                    priority: 0,
                },
            };
            match self.route(push, true).await? {
                Response::Txn {
                    status: TxnStatus::Pending,
                    ..
                } => {
                    let ms = u64::from(attempt.min(10)) + 1;
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                Response::Txn {
                    status, commit_ts, ..
                } => {
                    let resolve = Request::Resolve {
                        key: key.to_vec(),
                        txn_id: txn.id,
                        commit: status == TxnStatus::Committed,
                        commit_ts,
                    };
                    self.route(resolve, true).await?;
                }
                other => return Err(ClientError::Unexpected(other)),
            }
        }
        Err(ClientError::Exhausted(
            2_000,
            "a writer held the key".into(),
        ))
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        match self
            .request(Request::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })
            .await?
        {
            Response::Done => Ok(()),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), ClientError> {
        match self.request(Request::Delete { key: key.to_vec() }).await? {
            Response::Done => Ok(()),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    pub async fn split(&self, key: &[u8]) -> Result<(), ClientError> {
        match self.request(Request::Split { key: key.to_vec() }).await? {
            Response::Done => Ok(()),
            other => Err(ClientError::Unexpected(other)),
        }
    }

    /// Scan `[start, end)` across ranges, up to `limit` pairs.
    pub async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ClientError> {
        let mut out = Vec::new();
        let mut from = start.to_vec();
        loop {
            let want = u32::try_from(limit - out.len()).unwrap_or(u32::MAX);
            let resp = self
                .request(Request::Scan {
                    start: from.clone(),
                    end: end.to_vec(),
                    limit: want,
                })
                .await?;
            let Response::Rows { pairs, range_end } = resp else {
                return Err(ClientError::Unexpected(resp));
            };
            out.extend(pairs);
            let done = range_end.is_empty()
                || (!end.is_empty() && range_end.as_slice() >= end)
                || out.len() >= limit;
            if done {
                return Ok(out);
            }
            from = range_end;
        }
    }

    /// Every range, as seen by some node.
    pub async fn ranges(&self) -> Result<Vec<RangeInfo>, ClientError> {
        for n in self.nodes() {
            if let Ok(Response::Ranges(r)) = self.call(n, &Request::Ranges).await {
                return Ok(r);
            }
        }
        Err(ClientError::Exhausted(0, "no node answered".into()))
    }

    /// Every version committed in `(from, to]` in `[start, end)`, across ranges.
    /// Intents in the way are pushed above `to` (or resolved, if their
    /// transaction already finished), so the window is complete: nothing will
    /// ever commit at or below `to` in this span again.
    pub async fn changes(
        &self,
        start: &[u8],
        end: &[u8],
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<stowage_kv::Change>, ClientError> {
        use stowage_kv::mvcc::TxnStatus;
        use stowage_kv::types::PushKind;
        let mut out = Vec::new();
        let mut cur = start.to_vec();
        let mut skip: Vec<u128> = Vec::new();
        for _ in 0..10_000 {
            let req = Request::Changes {
                start: cur.clone(),
                end: end.to_vec(),
                from,
                to,
                limit: 10_000,
                skip: skip.clone(),
            };
            match self.route(req, true).await? {
                Response::Changes { events, resume } => {
                    out.extend(events);
                    if resume.is_empty() || (!end.is_empty() && resume.as_slice() >= end) {
                        return Ok(out);
                    }
                    cur = resume;
                }
                Response::ConflictAt { txn, key } => {
                    // Like a reader at `to` with the highest priority: the writer
                    // commits above the window, or we learn its fate.
                    let push = Request::Push {
                        pushee: txn.clone(),
                        kind: PushKind::Timestamp {
                            ts: to,
                            priority: u32::MAX,
                        },
                    };
                    match self.route(push, true).await? {
                        Response::Txn {
                            status: TxnStatus::Pending,
                            ..
                        } => skip.push(txn.id),
                        Response::Txn {
                            status, commit_ts, ..
                        } => {
                            let resolve = Request::Resolve {
                                key,
                                txn_id: txn.id,
                                commit: status == TxnStatus::Committed,
                                commit_ts,
                            };
                            self.route(resolve, true).await?;
                        }
                        other => return Err(ClientError::Unexpected(other)),
                    }
                }
                other => return Err(ClientError::Unexpected(other)),
            }
        }
        Err(ClientError::Exhausted(
            10_000,
            "changefeed window did not converge".into(),
        ))
    }

    /// The value of `key` as of `ts` (a historical, non-transactional read).
    /// `Ok(None)` if an unresolved intent at or below `ts` is in the way.
    pub async fn read_at(
        &self,
        key: &[u8],
        ts: Timestamp,
    ) -> Result<Option<Option<Vec<u8>>>, ClientError> {
        match self
            .route(
                Request::TxnGet {
                    key: key.to_vec(),
                    ts,
                    txn: None,
                    skip: Vec::new(),
                },
                true,
            )
            .await?
        {
            Response::Value(v) => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    /// The current time on this client's clock (a changefeed cursor).
    pub fn now(&self) -> Timestamp {
        self.hlc.now()
    }

    fn txn_ids(&self) -> (u128, u32) {
        let mut rng = self.rng.lock().unwrap_or_else(|e| e.into_inner());
        (
            u128::from(rng.next_u64()) << 64 | u128::from(rng.next_u64()),
            u32::try_from(rng.next_u64() % (1 << 20)).unwrap_or(1),
        )
    }

    /// Run `program` as one transaction and return how it ended. The coordinator
    /// state machine is the one the simulator tests ([`TxnCoord`]); this drives it
    /// over the network: each request goes to its range's leader in its own task,
    /// and a 10 ms tick handles backoff and heartbeats.
    pub async fn transact(
        self: &Arc<Self>,
        isolation: Isolation,
        program: Program,
    ) -> Result<Outcome, ClientError> {
        let (id, priority) = self.txn_ids();
        let mut d = Driver::new(
            self.clone(),
            TxnCoord::new(id, self.hlc.now(), priority, isolation, program),
        );
        d.run(Duration::from_secs(60)).await?;
        d.coord
            .outcome
            .clone()
            .ok_or_else(|| ClientError::Exhausted(0, "no outcome".into()))
    }

    /// Begin an interactive transaction: statements one at a time, as a SQL
    /// session issues them. A conflict that would need a restart ends it with
    /// [`TxnError::Retry`]; the caller runs it again from the start.
    pub fn begin(self: &Arc<Self>, isolation: Isolation) -> Txn {
        let (id, priority) = self.txn_ids();
        let next: Arc<std::sync::Mutex<Option<Step>>> = Arc::default();
        let n = next.clone();
        let program: Program = Box::new(move |_, _| {
            n.lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .unwrap_or(Step::Wait)
        });
        let coord = TxnCoord::interactive(id, self.hlc.now(), priority, isolation, program);
        Txn {
            d: Driver::new(self.clone(), coord),
            next,
            heartbeat: None,
        }
    }

    fn observe(&self, resp: &Response) {
        let seen = |ts: Timestamp| {
            if ts.wall < Timestamp::MAX.wall {
                let _ = self.hlc.update(ts);
            }
        };
        match resp {
            Response::Written(ts) | Response::WriteTooOld(ts) => seen(*ts),
            Response::Txn {
                commit_ts,
                min_commit_ts,
                ..
            } => {
                seen(*commit_ts);
                seen(*min_commit_ts);
            }
            _ => {}
        }
    }
}

/// Runs one coordinator against the cluster.
#[derive(Debug)]
struct Driver {
    client: Arc<KvClient>,
    coord: TxnCoord,
    tx: mpsc::UnboundedSender<(u64, Request, Result<Response, ClientError>)>,
    rx: mpsc::UnboundedReceiver<(u64, Request, Result<Response, ClientError>)>,
    tick: tokio::time::Interval,
}

impl Driver {
    fn new(client: Arc<KvClient>, coord: TxnCoord) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            client,
            coord,
            tx,
            rx,
            tick: tokio::time::interval(Duration::from_millis(10)),
        }
    }

    /// Drive until the coordinator is done or (interactive) idle again.
    async fn run(&mut self, limit: Duration) -> Result<(), ClientError> {
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            for o in self.coord.take_requests() {
                let (c, tx) = (self.client.clone(), self.tx.clone());
                tokio::spawn(async move {
                    let resp = c.route(o.req.clone(), true).await;
                    let _ = tx.send((o.tag, o.req, resp));
                });
            }
            if self.coord.is_done() || self.coord.is_idle() {
                return Ok(());
            }
            if tokio::time::Instant::now() > deadline {
                return Err(ClientError::Timeout);
            }
            tokio::select! {
                Some((tag, req, resp)) = self.rx.recv() => {
                    let resp = resp?;
                    self.client.observe(&resp);
                    let now = self.client.hlc.now();
                    self.coord.on_response(tag, &req, resp, now);
                }
                _ = self.tick.tick() => {
                    let now = self.client.hlc.now();
                    self.coord.tick(now);
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TxnError {
    /// The transaction had to restart; run it again (SQLSTATE 40001).
    #[error("restart transaction: {0}")]
    Retry(&'static str),
    #[error("transaction already finished")]
    Finished,
    #[error(transparent)]
    Client(#[from] ClientError),
}

/// An interactive transaction (see [`KvClient::begin`]).
#[derive(Debug)]
pub struct Txn {
    d: Driver,
    next: Arc<std::sync::Mutex<Option<Step>>>,
    /// Keeps the transaction record alive between statements, while the client
    /// is not driving the coordinator (a user thinking inside `BEGIN … COMMIT`).
    heartbeat: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Txn {
    fn drop(&mut self) {
        if let Some(h) = self.heartbeat.take() {
            h.abort();
        }
    }
}

impl Txn {
    async fn step(&mut self, step: Step) -> Result<Option<OpResult>, TxnError> {
        if self.d.coord.is_done() {
            return Err(TxnError::Finished);
        }
        let is_op = matches!(step, Step::Op(_));
        *self.next.lock().unwrap_or_else(|e| e.into_inner()) = Some(step);
        self.d.coord.resume();
        self.d.run(Duration::from_secs(60)).await?;
        if self.heartbeat.is_none()
            && !self.d.coord.meta().anchor.is_empty()
            && !self.d.coord.is_done()
        {
            // The first write created the record's anchor: heartbeat from now on.
            let (client, meta) = (self.d.client.clone(), self.d.coord.meta().clone());
            self.heartbeat = Some(tokio::spawn(async move {
                let mut every = tokio::time::interval(Duration::from_millis(500));
                loop {
                    every.tick().await;
                    let _ = client
                        .route(Request::Heartbeat { txn: meta.clone() }, true)
                        .await;
                }
            }));
        }
        match &self.d.coord.outcome {
            Some(Outcome::Retry { reason }) => Err(TxnError::Retry(reason)),
            Some(Outcome::GaveUp { .. }) => Err(TxnError::Retry("gave up")),
            Some(_) if is_op => Err(TxnError::Finished),
            _ => Ok(if is_op {
                self.d.coord.results.last().cloned()
            } else {
                None
            }),
        }
    }

    /// The snapshot this transaction reads at.
    pub fn read_ts(&self) -> Timestamp {
        self.d.coord.read_ts()
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, TxnError> {
        match self.step(Step::Op(TxnOp::Get(key.to_vec()))).await? {
            Some(OpResult::Value(v)) => Ok(v),
            other => Err(TxnError::Client(ClientError::Exhausted(
                0,
                format!("unexpected {other:?}"),
            ))),
        }
    }

    /// Up to `limit` live pairs in `[start, end)` (`end` empty: unbounded).
    pub async fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnError> {
        match self
            .step(Step::Op(TxnOp::Scan {
                start: start.to_vec(),
                end: end.to_vec(),
                limit,
            }))
            .await?
        {
            Some(OpResult::Rows(r)) => Ok(r),
            other => Err(TxnError::Client(ClientError::Exhausted(
                0,
                format!("unexpected {other:?}"),
            ))),
        }
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), TxnError> {
        self.step(Step::Op(TxnOp::Put(key.to_vec(), value.to_vec())))
            .await
            .map(drop)
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<(), TxnError> {
        self.step(Step::Op(TxnOp::Delete(key.to_vec())))
            .await
            .map(drop)
    }

    /// Commit; returns the commit timestamp.
    pub async fn commit(mut self) -> Result<Timestamp, TxnError> {
        self.step(Step::Commit).await?;
        match &self.d.coord.outcome {
            Some(Outcome::Committed { commit_ts, .. }) => Ok(*commit_ts),
            Some(Outcome::Retry { reason }) => Err(TxnError::Retry(reason)),
            _ => Err(TxnError::Finished),
        }
    }

    pub async fn rollback(mut self) -> Result<(), TxnError> {
        if self.d.coord.is_done() {
            return Ok(());
        }
        self.step(Step::Abort).await.map(drop)
    }
}

/// Tails `[start, end)`: each [`Changefeed::poll`] returns the versions committed
/// since the previous one, in timestamp order, and the closed timestamp it
/// reached. Every committed version is delivered exactly once.
#[derive(Debug)]
pub struct Changefeed {
    client: Arc<KvClient>,
    start: Vec<u8>,
    end: Vec<u8>,
    /// Everything at or below this has been delivered.
    pub cursor: Timestamp,
}

impl Changefeed {
    /// Start delivering changes committed after `from`.
    pub fn new(client: Arc<KvClient>, start: &[u8], end: &[u8], from: Timestamp) -> Self {
        Self {
            client,
            start: start.to_vec(),
            end: end.to_vec(),
            cursor: from,
        }
    }

    pub async fn poll(&mut self) -> Result<(Vec<stowage_kv::Change>, Timestamp), ClientError> {
        let to = self.client.now();
        let mut events = self
            .client
            .changes(&self.start, &self.end, self.cursor, to)
            .await?;
        events.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.key.cmp(&b.key)));
        self.cursor = to;
        Ok((events, to))
    }
}
