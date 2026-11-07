//! `stowctl` — talk to a Stowage cluster.
//!
//! ```text
//! stowctl --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 put hello world
//! stowctl --peers ... get hello
//! stowctl --peers ... ranges
//! ```

use clap::{Parser, Subcommand};
use stowage_node::{KvClient, parse_peers};

#[derive(Debug, Parser)]
#[command(version, about = "Stowage command-line client")]
struct Cli {
    /// Cluster nodes: `1=host:port,2=host:port,...`
    #[arg(
        long,
        env = "STOWAGE_PEERS",
        default_value = "1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
    )]
    peers: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Pairs in [start, end) (empty end = to the end).
    Scan {
        #[arg(default_value = "")]
        start: String,
        #[arg(default_value = "")]
        end: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Split the range containing `key` at `key`.
    Split {
        key: String,
    },
    /// Every range: span, replicas, leader, size.
    Ranges,
    /// One node's replicas and their Raft state.
    Status {
        #[arg(long)]
        node: u64,
    },
    /// Back up every version since the newest backup in `dest` (or all, with --full).
    Backup {
        dest: std::path::PathBuf,
        #[arg(long)]
        full: bool,
    },
    /// List the backups in `dest`.
    Backups {
        dest: std::path::PathBuf,
    },
    /// Restore into an empty cluster the state as of a time
    /// (`YYYY-MM-DD HH:MM:SS` UTC, `wall,logical`, or `latest`).
    Restore {
        src: std::path::PathBuf,
        #[arg(long, default_value = "latest")]
        as_of: String,
        #[arg(long)]
        force: bool,
    },
    /// Tail every commit in [start, end): one line per version, in timestamp
    /// order, each exactly once (empty end = to the end).
    Changefeed {
        #[arg(default_value = "")]
        start: String,
        #[arg(default_value = "")]
        end: String,
        /// Poll interval, milliseconds.
        #[arg(long, default_value_t = 200)]
        every_ms: u64,
        /// Start from the beginning of history instead of now.
        #[arg(long)]
        from_start: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = KvClient::new(parse_peers(&cli.peers).map_err(anyhow::Error::msg)?);
    match cli.cmd {
        Cmd::Get { key } => match client.get(key.as_bytes()).await? {
            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
            None => println!("(not found)"),
        },
        Cmd::Put { key, value } => {
            client.put(key.as_bytes(), value.as_bytes()).await?;
            println!("OK");
        }
        Cmd::Delete { key } => {
            client.delete(key.as_bytes()).await?;
            println!("OK");
        }
        Cmd::Scan { start, end, limit } => {
            for (k, v) in client.scan(start.as_bytes(), end.as_bytes(), limit).await? {
                println!(
                    "{}\t{}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
            }
        }
        Cmd::Split { key } => {
            client.split(key.as_bytes()).await?;
            println!("OK");
        }
        Cmd::Backup { dest, full } => {
            let m = stowage_backup::backup(&std::sync::Arc::new(client), &dest, full).await?;
            println!(
                "{} backup to {} ({}): {} versions of {} keys, {} bytes",
                m.kind, m.to, m.to_utc, m.versions, m.keys, m.bytes
            );
        }
        Cmd::Backups { dest } => {
            for (dir, m) in stowage_backup::list(&dest)? {
                println!(
                    "{:<12} ({:>22}, {:>22}]  {}  {:>7} versions  {}",
                    m.kind,
                    m.from,
                    m.to,
                    m.to_utc,
                    m.versions,
                    dir.display()
                );
            }
        }
        Cmd::Restore { src, as_of, force } => {
            let t = if as_of == "latest" {
                stowage_common::Timestamp::MAX
            } else {
                stowage_backup::parse_as_of(&as_of)?
            };
            let r = stowage_backup::restore(&std::sync::Arc::new(client), &src, t, force).await?;
            println!(
                "restored {} keys as of {} from {} backups ({} versions read)",
                r.keys_written, r.as_of, r.backups, r.versions_read
            );
        }
        Cmd::Changefeed {
            start,
            end,
            every_ms,
            from_start,
        } => {
            let client = std::sync::Arc::new(client);
            let from = if from_start {
                stowage_common::Timestamp::MIN
            } else {
                client.now()
            };
            let mut feed =
                stowage_node::Changefeed::new(client, start.as_bytes(), end.as_bytes(), from);
            loop {
                let (events, resolved) = feed.poll().await?;
                for e in events {
                    let v = e.value.map_or_else(
                        || "<deleted>".to_owned(),
                        |v| String::from_utf8_lossy(&v).into_owned(),
                    );
                    println!("{}  {}  {}", e.ts, String::from_utf8_lossy(&e.key), v);
                }
                eprintln!("resolved {resolved}");
                tokio::time::sleep(std::time::Duration::from_millis(every_ms)).await;
            }
        }
        Cmd::Status { node } => match client.call(node, &stowage_kv::Request::Status).await? {
            stowage_kv::Response::Status(lines) => lines.iter().for_each(|l| println!("{l}")),
            other => println!("{other:?}"),
        },
        Cmd::Ranges => {
            println!(
                "{:<16} {:<24} {:<24} {:>4} {:<12} {:>7} {:>10}",
                "range", "start", "end", "gen", "replicas", "leader", "bytes"
            );
            for r in client.ranges().await? {
                let show = |b: &[u8]| {
                    if b.is_empty() {
                        "-".to_string()
                    } else {
                        String::from_utf8_lossy(b).into_owned()
                    }
                };
                println!(
                    "{:<16} {:<24} {:<24} {:>4} {:<12} {:>7} {:>10}",
                    format!("r{}", r.desc.id),
                    show(&r.desc.start),
                    if r.desc.end.is_empty() {
                        "∞".to_string()
                    } else {
                        show(&r.desc.end)
                    },
                    r.desc.generation,
                    format!("{:?}", r.desc.replicas),
                    r.leader.map_or("?".into(), |l| format!("n{l}")),
                    r.approx_bytes
                );
            }
        }
    }
    Ok(())
}
