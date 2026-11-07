//! The DynamoDB JSON API over HTTP against a real three-node cluster:
//! conditions, transactions, an eventually consistent GSI, and a stream.
//! (`tests/compat/dynamodb/smoke.py` runs the same ground with boto3.)

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use stowage_node::testing::TestCluster;
use stowage_sql::{ClusterKv, KvStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn call(addr: &str, op: &str, body: Value) -> (u16, Value) {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let b = body.to_string();
    let req = format!(
        "POST / HTTP/1.1\r\nHost: {addr}\r\nX-Amz-Target: DynamoDB_20120810.{op}\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
        b.len()
    );
    s.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8(out).unwrap();
    let status: u16 = text[9..12].parse().unwrap();
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("{}");
    (status, serde_json::from_str(body).unwrap_or(Value::Null))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamodb_api() {
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
    tokio::spawn(async move { stowage_docapi::serve(&a, store, Some(client)).await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (st, _) = call(&addr, "CreateTable", json!({
        "TableName": "t",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "N"}, {"AttributeName": "g", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
        "GlobalSecondaryIndexes": [{"IndexName": "by_g", "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).await;
    assert_eq!(st, 200);
    for (sk, g) in [(1, "x"), (2, "y"), (10, "x")] {
        let (st, _) = call(&addr, "PutItem", json!({"TableName": "t", "Item": {"pk": {"S": "a"}, "sk": {"N": sk.to_string()}, "g": {"S": g}}})).await;
        assert_eq!(st, 200);
    }
    // Numbers sort numerically in the key: 1, 2, 10.
    let (_, q) = call(&addr, "Query", json!({"TableName": "t", "KeyConditionExpression": "pk = :p AND sk > :one", "ExpressionAttributeValues": {":p": {"S": "a"}, ":one": {"N": "1"}}})).await;
    let sks: Vec<&str> = q["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["sk"]["N"].as_str().unwrap())
        .collect();
    assert_eq!(sks, ["2", "10"]);

    let (st, e) = call(&addr, "PutItem", json!({"TableName": "t", "Item": {"pk": {"S": "a"}, "sk": {"N": "1"}}, "ConditionExpression": "attribute_not_exists(pk)"})).await;
    assert_eq!(
        (st, e["__type"].as_str().unwrap()),
        (
            400,
            "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException"
        )
    );

    let (st, e) = call(&addr, "TransactWriteItems", json!({"TransactItems": [
        {"Put": {"TableName": "t", "Item": {"pk": {"S": "b"}, "sk": {"N": "1"}}}},
        {"ConditionCheck": {"TableName": "t", "Key": {"pk": {"S": "a"}, "sk": {"N": "2"}}, "ConditionExpression": "g = :z", "ExpressionAttributeValues": {":z": {"S": "z"}}}}
    ]})).await;
    assert_eq!(st, 400);
    assert_eq!(
        e["CancellationReasons"][1]["Code"],
        "ConditionalCheckFailed"
    );
    let (_, g) = call(
        &addr,
        "GetItem",
        json!({"TableName": "t", "Key": {"pk": {"S": "b"}, "sk": {"N": "1"}}}),
    )
    .await;
    assert!(g.get("Item").is_none(), "a cancelled transaction wrote");

    // The GSI catches up asynchronously.
    let mut n = 0;
    for _ in 0..100 {
        let (_, q) = call(&addr, "Query", json!({"TableName": "t", "IndexName": "by_g", "KeyConditionExpression": "g = :x", "ExpressionAttributeValues": {":x": {"S": "x"}}})).await;
        n = q["Count"].as_u64().unwrap_or(0);
        if n == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(n, 2, "the GSI never caught up");
    // An update moves the item between index keys.
    call(&addr, "UpdateItem", json!({"TableName": "t", "Key": {"pk": {"S": "a"}, "sk": {"N": "10"}}, "UpdateExpression": "SET g = :y", "ExpressionAttributeValues": {":y": {"S": "y"}}})).await;
    let mut counts = (0, 0);
    for _ in 0..100 {
        let q = |v: &str| json!({"TableName": "t", "IndexName": "by_g", "KeyConditionExpression": "g = :v", "ExpressionAttributeValues": {":v": {"S": v}}, "Select": "COUNT"});
        let (_, x) = call(&addr, "Query", q("x")).await;
        let (_, y) = call(&addr, "Query", q("y")).await;
        counts = (
            x["Count"].as_u64().unwrap_or(9),
            y["Count"].as_u64().unwrap_or(9),
        );
        if counts == (1, 2) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(counts, (1, 2), "the stale GSI entry was not removed");

    // The stream has every write, in commit order.
    let (_, d) = call(&addr, "DescribeTable", json!({"TableName": "t"})).await;
    let arn = d["Table"]["LatestStreamArn"].as_str().unwrap().to_owned();
    let (_, it) = call(&addr, "GetShardIterator", json!({"StreamArn": arn, "ShardId": "shardId-00000000000000000000-00000000", "ShardIteratorType": "TRIM_HORIZON"})).await;
    let (_, r) = call(
        &addr,
        "GetRecords",
        json!({"ShardIterator": it["ShardIterator"]}),
    )
    .await;
    let kinds: Vec<&str> = r["Records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["eventName"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["INSERT", "INSERT", "INSERT", "MODIFY"]);
}
