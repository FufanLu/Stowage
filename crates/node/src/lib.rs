//! The pieces that turn a [`stowage_kv::Store`] into a networked node: TCP
//! framing ([`wire`]), the server runtime ([`server`]) and an async client
//! ([`client`]).

pub mod client;
pub mod server;
#[doc(hidden)]
pub mod testing;
pub mod wire;

pub use client::{ClientError, KvClient, Txn, TxnError};
pub use server::{NodeConfig, run};

/// Parse `1=host:port,2=host:port,...`.
pub fn parse_peers(s: &str) -> Result<std::collections::BTreeMap<u64, String>, String> {
    s.split(',')
        .filter(|p| !p.trim().is_empty())
        .map(|p| {
            let (id, addr) = p
                .split_once('=')
                .ok_or_else(|| format!("expected id=addr, got {p:?}"))?;
            Ok((
                id.trim()
                    .parse()
                    .map_err(|e| format!("bad node id {id:?}: {e}"))?,
                addr.trim().to_owned(),
            ))
        })
        .collect()
}
