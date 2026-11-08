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

/// Provisioned throughput: past the burst, writes are throttled the way
/// DynamoDB throttles them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provisioned_throughput_is_enforced() {
    let cluster = TestCluster::start(3).await.unwrap();
    let store: Arc<dyn KvStore> = Arc::new(ClusterKv {
        client: cluster.client(),
    });
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let a = addr.clone();
    tokio::spawn(async move { stowage_docapi::serve(&a, store, None).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    call(
        &addr,
        "CreateTable",
        json!({
            "TableName": "p", "BillingMode": "PROVISIONED",
            "ProvisionedThroughput": {"ReadCapacityUnits": 100, "WriteCapacityUnits": 2},
            "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}]
        }),
    )
    .await;
    let (_, d) = call(&addr, "DescribeTable", json!({"TableName": "p"})).await;
    assert_eq!(d["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"], 2);
    let mut throttled = 0;
    for i in 0..30 {
        let (st, e) = call(
            &addr,
            "PutItem",
            json!({"TableName": "p", "Item": {"k": {"S": format!("k{i}")}}}),
        )
        .await;
        if st == 400 {
            assert_eq!(
                e["__type"],
                "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException"
            );
            throttled += 1;
        }
    }
    // 2 units/s with 5 s of burst: about 10 pass, the rest are throttled.
    assert!(
        (15..=25).contains(&throttled),
        "{throttled} of 30 throttled"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_to_live_expires_items() {
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

    // The tracking service's events table: PK shipmentId, SK seq, TTL expiresAt.
    call(&addr, "CreateTable", json!({
        "TableName": "tracking_events",
        "BillingMode": "PAY_PER_REQUEST",
        "AttributeDefinitions": [{"AttributeName": "shipmentId", "AttributeType": "S"}, {"AttributeName": "seq", "AttributeType": "N"}, {"AttributeName": "kind", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "shipmentId", "KeyType": "HASH"}, {"AttributeName": "seq", "KeyType": "RANGE"}],
        "GlobalSecondaryIndexes": [{"IndexName": "by_kind", "KeySchema": [{"AttributeName": "kind", "KeyType": "HASH"}], "Projection": {"ProjectionType": "ALL"}}]
    })).await;
    let (_, d) = call(
        &addr,
        "DescribeTimeToLive",
        json!({"TableName": "tracking_events"}),
    )
    .await;
    assert_eq!(d["TimeToLiveDescription"]["TimeToLiveStatus"], "DISABLED");
    let spec = json!({"TableName": "tracking_events", "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expiresAt"}});
    let (st, r) = call(&addr, "UpdateTimeToLive", spec.clone()).await;
    assert_eq!(
        (st, &r["TimeToLiveSpecification"]["AttributeName"]),
        (200, &json!("expiresAt"))
    );
    // Enabling it again is an error the service already tolerates.
    let (st, e) = call(&addr, "UpdateTimeToLive", spec).await;
    assert_eq!(st, 400);
    assert!(
        e["__type"]
            .as_str()
            .unwrap()
            .ends_with("ValidationException"),
        "{e}"
    );
    let (_, d) = call(
        &addr,
        "DescribeTimeToLive",
        json!({"TableName": "tracking_events"}),
    )
    .await;
    assert_eq!(d["TimeToLiveDescription"]["AttributeName"], "expiresAt");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let items = [
        (1, Some(json!({"N": (now - 60).to_string()}))), // expired
        (2, Some(json!({"N": (now + 3600).to_string()}))), // not yet
        (3, None),                                       // no attribute: never expires
        (4, Some(json!({"S": "yesterday"}))),            // not a number: never expires
        (5, Some(json!({"N": (now - 1).to_string()}))),  // expired
    ];
    for (seq, exp) in &items {
        let mut item = json!({"shipmentId": {"S": "shp-1"}, "seq": {"N": seq.to_string()}, "kind": {"S": "scan"}});
        if let Some(e) = exp {
            item["expiresAt"] = e.clone();
        }
        let (st, _) = call(
            &addr,
            "PutItem",
            json!({"TableName": "tracking_events", "Item": item}),
        )
        .await;
        assert_eq!(st, 200);
    }
    let seqs = |q: &Value| -> Vec<String> {
        q["Items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["seq"]["N"].as_str().unwrap().to_owned())
            .collect()
    };
    let query = json!({"TableName": "tracking_events", "KeyConditionExpression": "shipmentId = :s", "ExpressionAttributeValues": {":s": {"S": "shp-1"}}});
    let gsi = json!({"TableName": "tracking_events", "IndexName": "by_kind", "KeyConditionExpression": "kind = :k", "ExpressionAttributeValues": {":k": {"S": "scan"}}});
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let (_, q) = call(&addr, "Query", query.clone()).await;
        let (_, g) = call(&addr, "Query", gsi.clone()).await;
        let (mut a, mut b) = (seqs(&q), seqs(&g));
        a.sort();
        b.sort();
        if a == ["2", "3", "4"] && b == ["2", "3", "4"] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "expired items still there: base {a:?}, gsi {b:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
