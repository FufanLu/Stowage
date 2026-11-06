//! Async client: connection multiplexing, range-aware routing, retries.
//!
//! Requests carry a token; one TCP connection per node carries any number of
//! concurrent requests. The client caches `RangeInfo`s (span + leader) learned
//! from `Locate`, and on a routing error asks the node it reached where the key
//! lives now.
//!
//! Writes are never retried after they may have been proposed: a timeout or an
//! `Ambiguous` reply is returned to the caller, who knows whether re-applying the
//! write is safe.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use stowage_common::NodeId;
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
    pub timeout: Duration,
}

impl KvClient {
    pub fn new(addrs: BTreeMap<NodeId, String>) -> Self {
        Self {
            addrs,
            conns: Mutex::new(HashMap::new()),
            ranges: std::sync::Mutex::new(BTreeMap::new()),
            next_token: AtomicU64::new(1),
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
        let key = req.routing_key().unwrap_or_default().to_vec();
        let is_write = matches!(
            req,
            Request::Put { .. } | Request::Delete { .. } | Request::Split { .. }
        );
        let nodes = self.nodes();
        let mut target = self.cached(&key).unwrap_or(nodes[0]);
        let mut last_err = String::new();
        for attempt in 0..30u32 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(u64::from(attempt.min(10)) * 20)).await;
            }
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
                Response::Error(KvError::Ambiguous) => return Err(ClientError::Ambiguous),
                Response::Error(KvError::NotLeader {
                    leader: Some(l), ..
                }) if l != target => {
                    self.set_leader(&key, l);
                    target = l;
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
                    self.set_leader(&key, target);
                    return Ok(ok);
                }
            }
        }
        Err(ClientError::Exhausted(30, last_err))
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        match self.request(Request::Get { key: key.to_vec() }).await? {
            Response::Value(v) => Ok(v),
            other => Err(ClientError::Unexpected(other)),
        }
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
}
