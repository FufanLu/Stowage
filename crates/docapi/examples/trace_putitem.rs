//! Real data for `docs/illustrated/08-a-dynamodb-putitem.md`.
//!
//! ```text
//! cargo run --release -p stowage-docapi --example trace_putitem
//! ```
//!
//! A three-node cluster in this process: one conditional PutItem over raw HTTP,
//! the key-value pairs it leaves, the changefeed event, the GSI entry the worker
//! writes from it, and the stream record.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use stowage_node::testing::TestCluster;
use stowage_sql::{ClusterKv, KvStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn hex(b: &[u8]) -> String {
    b.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn raw(addr: &str, op: &str, body: &Value, show: bool) -> Value {
    let b = body.to_string();
    let req = format!(
        "POST / HTTP/1.1\r\nHost: {addr}\r\nX-Amz-Target: DynamoDB_20120810.{op}\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
        b.len()
    );
    let t = Instant::now();
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8(out).unwrap();
    if show {
        println!("► {}", req.replace("\r\n", "\n► "));
        println!("◄ ({:.1} ms)", t.elapsed().as_secs_f64() * 1e3);
        for l in text.lines() {
            if !l.to_ascii_lowercase().starts_with("date:") {
                println!("◄ {l}");
            }
        }
    }
    serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap_or("{}")).unwrap_or(Value::Null)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let cluster = TestCluster::start(3).await.unwrap();
    let client = cluster.client();
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv {
        client: client.clone(),
    });
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let a = addr.clone();
    let c = client.clone();
    tokio::spawn(async move { stowage_docapi::serve(&a, store, Some(c)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    raw(&addr, "CreateTable", &json!({
        "TableName": "orders",
        "AttributeDefinitions": [{"AttributeName": "customer", "AttributeType": "S"}, {"AttributeName": "order_id", "AttributeType": "N"}, {"AttributeName": "status", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "customer", "KeyType": "HASH"}, {"AttributeName": "order_id", "KeyType": "RANGE"}],
        "GlobalSecondaryIndexes": [{"IndexName": "by_status", "KeySchema": [{"AttributeName": "status", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    }), false).await;
    let before = client.now();

    println!("== the request ==");
    raw(&addr, "PutItem", &json!({
        "TableName": "orders",
        "Item": {"customer": {"S": "acme"}, "order_id": {"N": "42"}, "status": {"S": "paid"}, "total": {"N": "99.5"}},
        "ConditionExpression": "attribute_not_exists(order_id)"
    }), true).await;

    println!("\n== what it left in the key space ==");
    for (k, v) in client.scan(&[0x03], &[0x05], 100).await.unwrap() {
        println!("key   {}\nvalue {}", hex(&k), String::from_utf8_lossy(&v));
    }

    println!("\n== the changefeed window that contains it ==");
    let to = client.now();
    for e in client.changes(&[0x03], &[0x04], before, to).await.unwrap() {
        println!(
            "{}  key {}  {} bytes",
            e.ts,
            hex(&e.key),
            e.value.map_or(0, |v| v.len())
        );
    }

    println!("\n== after the GSI worker caught up ==");
    let t = Instant::now();
    loop {
        let q = raw(&addr, "Query", &json!({"TableName": "orders", "IndexName": "by_status", "KeyConditionExpression": "#s = :p", "ExpressionAttributeNames": {"#s": "status"}, "ExpressionAttributeValues": {":p": {"S": "paid"}}}), false).await;
        if q["Count"] == 1 {
            println!(
                "the GSI query finds it after {:.0} ms",
                t.elapsed().as_secs_f64() * 1e3
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for (k, v) in client.scan(&[0x03], &[0x05], 100).await.unwrap() {
        println!("key   {}\nvalue {} bytes", hex(&k), v.len());
    }

    println!("\n== the stream record ==");
    let d = raw(
        &addr,
        "DescribeTable",
        &json!({"TableName": "orders"}),
        false,
    )
    .await;
    let arn = d["Table"]["LatestStreamArn"].as_str().unwrap().to_owned();
    let it = raw(&addr, "GetShardIterator", &json!({"StreamArn": arn, "ShardId": "shardId-00000000000000000000-00000000", "ShardIteratorType": "TRIM_HORIZON"}), false).await;
    let r = raw(
        &addr,
        "GetRecords",
        &json!({"ShardIterator": it["ShardIterator"]}),
        false,
    )
    .await;
    println!("{}", serde_json::to_string_pretty(&r["Records"]).unwrap());

    println!("\n== the same PutItem again: the condition fails ==");
    raw(
        &addr,
        "PutItem",
        &json!({
            "TableName": "orders",
            "Item": {"customer": {"S": "acme"}, "order_id": {"N": "42"}, "status": {"S": "open"}},
            "ConditionExpression": "attribute_not_exists(order_id)"
        }),
        true,
    )
    .await;
}
