//! A real Postgres client (tokio-postgres) against the wire protocol, over a
//! real three-node cluster: simple queries, the extended protocol with binary
//! results, transactions, and SQLSTATEs.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use stowage_node::testing::TestCluster;
use stowage_sql::{ClusterKv, KvStore};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_client_works() {
    let cluster = TestCluster::start(3).await.unwrap();
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv {
        client: cluster.client(),
    });
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let a = addr.clone();
    tokio::spawn(async move { stowage_pgwire::serve(&a, store).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (host, port) = addr.split_once(':').unwrap();
    let (client, conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user=stowage dbname=stowage"),
        tokio_postgres::NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(conn);

    client
        .batch_execute(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, ok BOOLEAN, at TIMESTAMP);
             INSERT INTO t VALUES (1, 'a', 1.5, true, '2026-11-06 10:00:00'), (2, 'b', NULL, false, NULL)",
        )
        .await
        .unwrap();

    // Extended protocol, binary results.
    let rows = client
        .query("SELECT id, name, score, ok FROM t ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i64>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "a");
    assert_eq!(rows[0].get::<_, Option<f64>>(2), Some(1.5));
    assert!(rows[0].get::<_, bool>(3));
    assert_eq!(rows[1].get::<_, Option<f64>>(2), None);

    // Parameters (sent as text, typed by the server).
    let rows = client
        .query("SELECT name FROM t WHERE id = $1", &[&"2"])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, &str>(0), "b");

    // Simple query protocol, text results.
    let msgs = client.simple_query("SELECT count(*) FROM t").await.unwrap();
    let count = msgs.iter().find_map(|m| {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            r.get(0).map(str::to_owned)
        } else {
            None
        }
    });
    assert_eq!(count.as_deref(), Some("2"));

    // Transactions and SQLSTATEs.
    client
        .batch_execute("BEGIN; UPDATE t SET name = 'z' WHERE id = 1; ROLLBACK")
        .await
        .unwrap();
    let rows = client
        .query("SELECT name FROM t WHERE id = 1", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, &str>(0), "a");
    let err = client
        .batch_execute("INSERT INTO t VALUES (1, 'dup', 0, true, NULL)")
        .await
        .unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("23505"));
    let err = client.query("SELECT nope FROM t", &[]).await.unwrap_err();
    assert_eq!(err.code().map(|c| c.code()), Some("42703"));
    // The connection is still usable after errors.
    let rows = client.query("SELECT 40 + 2", &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i64>(0), 42);
}

/// SCRAM-SHA-256 logins and table privileges, through a real client.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scram_and_privileges() {
    let cluster = TestCluster::start(3).await.unwrap();
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv {
        client: cluster.client(),
    });
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let a = addr.clone();
    tokio::spawn(async move { stowage_pgwire::serve(&a, store).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (host, port) = addr.split_once(':').unwrap();
    let connect = |user: &str, password: Option<&str>| {
        let mut cfg = format!("host={host} port={port} user={user} dbname=stowage");
        if let Some(p) = password {
            cfg.push_str(&format!(" password={p}"));
        }
        async move {
            let (c, conn) = tokio_postgres::connect(&cfg, tokio_postgres::NoTls).await?;
            tokio::spawn(conn);
            Ok::<_, tokio_postgres::Error>(c)
        }
    };
    // No users yet: trust. The first user turns authentication on.
    let root = connect("anyone", None).await.unwrap();
    root.batch_execute(
        "CREATE TABLE t (id INT PRIMARY KEY, v TEXT); CREATE TABLE secret (id INT PRIMARY KEY);
         CREATE USER admin WITH SUPERUSER PASSWORD 'adm1n'; CREATE USER app WITH PASSWORD 'app-pw';
         GRANT SELECT, INSERT ON t TO app",
    )
    .await
    .unwrap();

    let code = |e: tokio_postgres::Error| {
        e.code()
            .map(|c| c.code().to_owned())
            .unwrap_or_else(|| e.to_string())
    };
    assert_eq!(
        connect("app", Some("wrong"))
            .await
            .err()
            .map(code)
            .as_deref(),
        Some("28P01")
    );
    assert_eq!(
        connect("nobody", Some("x"))
            .await
            .err()
            .map(code)
            .as_deref(),
        Some("28P01")
    );
    assert!(
        connect("app", None).await.is_err(),
        "a password is now required"
    );

    let app = connect("app", Some("app-pw")).await.unwrap();
    app.batch_execute("INSERT INTO t VALUES (1, 'a')")
        .await
        .unwrap();
    assert_eq!(app.query("SELECT v FROM t", &[]).await.unwrap().len(), 1);
    for (sql, why) in [
        ("UPDATE t SET v = 'b'", "no UPDATE"),
        ("DELETE FROM t", "no DELETE"),
        ("SELECT * FROM secret", "no SELECT on secret"),
        (
            "SELECT * FROM t WHERE id IN (SELECT id FROM secret)",
            "a subquery reads secret",
        ),
        (
            "CREATE TABLE u (id INT PRIMARY KEY)",
            "DDL needs a superuser",
        ),
        ("GRANT ALL ON t TO app", "only superusers grant"),
    ] {
        let e = app.batch_execute(sql).await.unwrap_err();
        assert_eq!(code(e), "42501", "{why}: {sql}");
    }
    // Revoking takes effect on the open connection's next statement.
    let admin = connect("admin", Some("adm1n")).await.unwrap();
    admin
        .batch_execute("REVOKE INSERT ON t FROM app")
        .await
        .unwrap();
    assert_eq!(
        code(
            app.batch_execute("INSERT INTO t VALUES (2, 'b')")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    // Changing a password: the old one stops working.
    admin
        .batch_execute("ALTER USER app WITH PASSWORD 'new-pw'")
        .await
        .unwrap();
    assert!(connect("app", Some("app-pw")).await.is_err());
    assert!(connect("app", Some("new-pw")).await.is_ok());
}

/// TLS: `SSLRequest` → `S` → a handshake, then the protocol inside TLS; with
/// `require`, a plain-text startup is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_upgrade_and_require() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cluster = TestCluster::start(3).await.unwrap();
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv {
        client: cluster.client(),
    });
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let dir = std::env::temp_dir().join(format!("stowage-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("cert.pem"), ck.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), ck.key_pair.serialize_pem()).unwrap();
    let cfg = stowage_pgwire::tls_from_pem(&dir.join("cert.pem"), &dir.join("key.pem")).unwrap();
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let a = addr.clone();
    tokio::spawn(async move {
        stowage_pgwire::serve_with(
            &a,
            store,
            stowage_pgwire::TlsPolicy {
                config: Some(cfg),
                require: true,
            },
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let startup = {
        let mut b = Vec::new();
        b.extend_from_slice(&196_608i32.to_be_bytes());
        b.extend_from_slice(b"user\0stowage\0\0");
        let mut m = ((b.len() + 4) as i32).to_be_bytes().to_vec();
        m.extend(b);
        m
    };
    // Plain text is refused.
    let (host, port) = addr.split_once(':').unwrap();
    let mut plain = tokio::net::TcpStream::connect(&addr).await.unwrap();
    plain.write_all(&startup).await.unwrap();
    let mut reply = Vec::new();
    plain.read_to_end(&mut reply).await.unwrap();
    assert_eq!(reply[0], b'E');
    assert!(
        String::from_utf8_lossy(&reply).contains("28000"),
        "{reply:?}"
    );

    // TLS: upgrade, then start up and query inside it.
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
        .await
        .unwrap(); // SSLRequest 80877103
    let mut s = [0u8; 1];
    sock.read_exact(&mut s).await.unwrap();
    assert_eq!(&s, b"S");
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ck.cert.der().clone()).unwrap();
    let client_cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = tokio_rustls::TlsConnector::from(Arc::new(client_cfg))
        .connect(name, sock)
        .await
        .unwrap();
    let _ = (host, port);
    tls.write_all(&startup).await.unwrap();
    let mut q = b"Q".to_vec();
    let sql = b"SELECT 6 * 7\0";
    q.extend_from_slice(&((sql.len() + 4) as i32).to_be_bytes());
    q.extend_from_slice(sql);
    tls.write_all(&q).await.unwrap();
    let mut got = Vec::new();
    let mut buf = [0u8; 4096];
    while !String::from_utf8_lossy(&got).contains("SELECT 1") {
        let n = tls.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed: {got:?}");
        got.extend_from_slice(&buf[..n]);
    }
    assert!(
        got.windows(2).any(|w| w == b"42"),
        "the answer came back inside TLS"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
