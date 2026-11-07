//! The key-value transaction the SQL layer runs on.
//!
//! Two implementations: the cluster ([`ClusterKv`], interactive transactions
//! over TCP) and an in-memory store ([`MemKv`]) for fast logic tests, which
//! simply runs one transaction at a time.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;
use std::sync::Arc;

use stowage_kv::txn::Isolation;
use stowage_node::{KvClient, Txn, TxnError};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::{Error, Result};

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

pub trait KvTxn: Send {
    fn get<'a>(&'a mut self, key: &'a [u8]) -> BoxFut<'a, Result<Option<Vec<u8>>>>;
    /// Up to `limit` live pairs in `[start, end)` (`end` empty: unbounded), in order.
    fn scan<'a>(
        &'a mut self,
        start: &'a [u8],
        end: &'a [u8],
        limit: u32,
    ) -> BoxFut<'a, Result<Pairs>>;
    fn put<'a>(&'a mut self, key: &'a [u8], value: &'a [u8]) -> BoxFut<'a, Result<()>>;
    fn delete<'a>(&'a mut self, key: &'a [u8]) -> BoxFut<'a, Result<()>>;
    fn commit(self: Box<Self>) -> BoxFut<'static, Result<()>>;
    fn rollback(self: Box<Self>) -> BoxFut<'static, Result<()>>;
}

pub trait KvStore: Send + Sync {
    fn begin(&self, isolation: Isolation) -> BoxFut<'_, Result<Box<dyn KvTxn>>>;
}

fn kv_err(e: TxnError) -> Error {
    match e {
        TxnError::Retry(r) => Error::Retry(r.to_owned()),
        e => Error::Kv(e.to_string()),
    }
}

/// The cluster: every statement's reads and writes are an interactive transaction.
#[derive(Debug, Clone)]
pub struct ClusterKv {
    pub client: Arc<KvClient>,
}

impl KvStore for ClusterKv {
    fn begin(&self, isolation: Isolation) -> BoxFut<'_, Result<Box<dyn KvTxn>>> {
        let t = self.client.begin(isolation);
        Box::pin(async move { Ok(Box::new(ClusterTxn(t)) as Box<dyn KvTxn>) })
    }
}

struct ClusterTxn(Txn);

impl KvTxn for ClusterTxn {
    fn get<'a>(&'a mut self, key: &'a [u8]) -> BoxFut<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move { self.0.get(key).await.map_err(kv_err) })
    }
    fn scan<'a>(
        &'a mut self,
        start: &'a [u8],
        end: &'a [u8],
        limit: u32,
    ) -> BoxFut<'a, Result<Pairs>> {
        Box::pin(async move { self.0.scan(start, end, limit).await.map_err(kv_err) })
    }
    fn put<'a>(&'a mut self, key: &'a [u8], value: &'a [u8]) -> BoxFut<'a, Result<()>> {
        Box::pin(async move { self.0.put(key, value).await.map_err(kv_err) })
    }
    fn delete<'a>(&'a mut self, key: &'a [u8]) -> BoxFut<'a, Result<()>> {
        Box::pin(async move { self.0.delete(key).await.map_err(kv_err) })
    }
    fn commit(self: Box<Self>) -> BoxFut<'static, Result<()>> {
        Box::pin(async move { self.0.commit().await.map(drop).map_err(kv_err) })
    }
    fn rollback(self: Box<Self>) -> BoxFut<'static, Result<()>> {
        Box::pin(async move { self.0.rollback().await.map_err(kv_err) })
    }
}

/// An in-memory store that runs transactions one at a time (trivially
/// serializable). For logic tests that should not wait on fsyncs.
#[derive(Debug, Clone, Default)]
pub struct MemKv {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl KvStore for MemKv {
    fn begin(&self, _isolation: Isolation) -> BoxFut<'_, Result<Box<dyn KvTxn>>> {
        let data = self.data.clone();
        Box::pin(async move {
            let guard = data.lock_owned().await;
            Ok(Box::new(MemTxn {
                guard,
                writes: BTreeMap::new(),
            }) as Box<dyn KvTxn>)
        })
    }
}

struct MemTxn {
    guard: OwnedMutexGuard<BTreeMap<Vec<u8>, Vec<u8>>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl KvTxn for MemTxn {
    fn get<'a>(&'a mut self, key: &'a [u8]) -> BoxFut<'a, Result<Option<Vec<u8>>>> {
        let v = match self.writes.get(key) {
            Some(w) => w.clone(),
            None => self.guard.get(key).cloned(),
        };
        Box::pin(async move { Ok(v) })
    }
    fn scan<'a>(
        &'a mut self,
        start: &'a [u8],
        end: &'a [u8],
        limit: u32,
    ) -> BoxFut<'a, Result<Pairs>> {
        let hi = if end.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(end)
        };
        let mut merged: BTreeMap<&[u8], Option<&Vec<u8>>> = BTreeMap::new();
        for (k, v) in self.guard.range::<[u8], _>((Bound::Included(start), hi)) {
            merged.insert(k, Some(v));
        }
        for (k, v) in self.writes.range::<[u8], _>((Bound::Included(start), hi)) {
            merged.insert(k, v.as_ref());
        }
        let out: Pairs = merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k.to_vec(), v.clone())))
            .take(limit as usize)
            .collect();
        Box::pin(async move { Ok(out) })
    }
    fn put<'a>(&'a mut self, key: &'a [u8], value: &'a [u8]) -> BoxFut<'a, Result<()>> {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
        Box::pin(async { Ok(()) })
    }
    fn delete<'a>(&'a mut self, key: &'a [u8]) -> BoxFut<'a, Result<()>> {
        self.writes.insert(key.to_vec(), None);
        Box::pin(async { Ok(()) })
    }
    fn commit(mut self: Box<Self>) -> BoxFut<'static, Result<()>> {
        for (k, v) in std::mem::take(&mut self.writes) {
            match v {
                Some(v) => self.guard.insert(k, v),
                None => self.guard.remove(&k),
            };
        }
        Box::pin(async { Ok(()) })
    }
    fn rollback(self: Box<Self>) -> BoxFut<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
