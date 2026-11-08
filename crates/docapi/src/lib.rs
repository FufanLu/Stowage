//! A DynamoDB-compatible JSON API on the Stowage key space.
//!
//! ```text
//!  POST /   X-Amz-Target: DynamoDB_20120810.PutItem   {"TableName": …, "Item": {…}}
//!       │
//!       ▼  one SSI transaction per call (TransactWriteItems: one for all items)
//!  0x03 ‖ table ‖ 0 ‖ key(hash) [‖ key(range)] → item JSON
//!       │
//!       └─ committed versions ──► changefeed ──► GSI worker (one, by lease) ──► 0x03 ‖ table ‖ gsi ‖ …
//!                                            └─► Streams: GetRecords
//! ```
//!
//! Global secondary indexes are maintained asynchronously from the changefeed,
//! so they are eventually consistent, as in DynamoDB. Reads of the base table
//! are always strongly consistent.

pub mod expr;
pub mod model;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, json};
use stowage_common::Timestamp;
use stowage_kv::txn::Isolation;
use stowage_node::KvClient;
use stowage_sql::keycodec::{self, META};
use stowage_sql::kv::{KvStore, KvTxn};

use crate::expr::{Bindings, Cond};
use crate::model::{
    BASE, ITEMS, Item, decode_item, encode_item, encode_key_attr, ledger_key, prefix,
};

#[derive(Debug, Clone)]
pub struct DdbError {
    pub kind: &'static str,
    pub message: String,
    pub status: u16,
    /// For TransactionCanceledException: one reason per item.
    pub reasons: Option<Vec<Json>>,
}

impl DdbError {
    fn new(kind: &'static str, m: impl Into<String>) -> Self {
        Self {
            kind,
            message: m.into(),
            status: 400,
            reasons: None,
        }
    }
    pub fn validation(m: impl Into<String>) -> Self {
        Self::new("ValidationException", m)
    }
    pub fn internal(m: impl Into<String>) -> Self {
        Self {
            status: 500,
            ..Self::new("InternalServerError", m)
        }
    }
    fn not_found(table: &str) -> Self {
        Self::new(
            "ResourceNotFoundException",
            format!("Requested resource not found: Table: {table} not found"),
        )
    }
    fn conditional() -> Self {
        Self::new(
            "ConditionalCheckFailedException",
            "The conditional request failed",
        )
    }
}

impl From<stowage_sql::Error> for DdbError {
    fn from(e: stowage_sql::Error) -> Self {
        match e {
            stowage_sql::Error::Retry(m) => Self::new("TransactionConflictException", m),
            e => Self::internal(e.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, DdbError>;

// ---------------------------------------------------------------- tables

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KeyAttr {
    pub name: String,
    pub ty: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Gsi {
    pub name: String,
    pub id: u32,
    pub hash: KeyAttr,
    pub range: Option<KeyAttr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDesc {
    pub name: String,
    pub id: u32,
    pub hash: KeyAttr,
    pub range: Option<KeyAttr>,
    pub attributes: Vec<KeyAttr>,
    pub gsis: Vec<Gsi>,
    pub created: f64,
    pub stream: bool,
    /// Provisioned read / write capacity units per second (None: on demand).
    #[serde(default)]
    pub rcu: Option<u64>,
    #[serde(default)]
    pub wcu: Option<u64>,
    /// `UpdateTimeToLive`: the attribute holding an expiry time (epoch seconds).
    #[serde(default)]
    pub ttl: Option<String>,
}

fn table_key(name: &str) -> Vec<u8> {
    let mut k = vec![META];
    k.extend_from_slice(b"ddb/");
    k.extend_from_slice(name.as_bytes());
    k
}

impl TableDesc {
    fn key_attrs(&self) -> impl Iterator<Item = &KeyAttr> {
        std::iter::once(&self.hash).chain(self.range.as_ref())
    }

    /// The item's key in the base table; the key attributes must be present
    /// and of the declared types.
    pub fn base_key(&self, item: &Item) -> Result<Vec<u8>> {
        let mut k = prefix(self.id, BASE);
        for a in self.key_attrs() {
            let v = item.get(&a.name).ok_or_else(|| {
                DdbError::validation(format!(
                    "One or more parameter values were invalid: Missing the key {} in the item",
                    a.name
                ))
            })?;
            if model::tag(v)?.0 != a.ty {
                return Err(DdbError::validation(format!(
                    "One or more parameter values were invalid: Type mismatch for key {} expected: {} actual: {}",
                    a.name,
                    a.ty,
                    model::tag(v)?.0
                )));
            }
            encode_key_attr(v, &mut k)?;
        }
        Ok(k)
    }

    /// The entry key of `item` in `gsi`, or `None` if the item lacks the index's
    /// key attributes (GSIs are sparse).
    pub fn gsi_key(&self, gsi: &Gsi, item: &Item) -> Result<Option<Vec<u8>>> {
        let mut k = prefix(self.id, gsi.id);
        for a in std::iter::once(&gsi.hash).chain(gsi.range.as_ref()) {
            match item.get(&a.name) {
                Some(v) if model::tag(v)?.0 == a.ty => encode_key_attr(v, &mut k)?,
                _ => return Ok(None),
            }
        }
        for a in self.key_attrs() {
            encode_key_attr(
                item.get(&a.name)
                    .ok_or_else(|| DdbError::internal("indexed item without its key"))?,
                &mut k,
            )?;
        }
        Ok(Some(k))
    }

    fn arn(&self) -> String {
        format!("arn:aws:dynamodb:stowage:000000000000:table/{}", self.name)
    }

    fn stream_arn(&self) -> String {
        format!("{}/stream/{}", self.arn(), self.created as u64)
    }

    fn describe(&self, items: Option<usize>) -> Json {
        let ks = |h: &KeyAttr, r: &Option<KeyAttr>| {
            let mut v = vec![json!({"AttributeName": h.name, "KeyType": "HASH"})];
            if let Some(r) = r {
                v.push(json!({"AttributeName": r.name, "KeyType": "RANGE"}));
            }
            v
        };
        let mut d = json!({
            "TableName": self.name,
            "TableStatus": "ACTIVE",
            "TableArn": self.arn(),
            "TableId": format!("{:08x}", self.id),
            "CreationDateTime": self.created,
            "KeySchema": ks(&self.hash, &self.range),
            "AttributeDefinitions": self.attributes.iter().map(|a| json!({"AttributeName": a.name, "AttributeType": a.ty})).collect::<Vec<_>>(),
            "ItemCount": items.unwrap_or(0),
            "TableSizeBytes": 0,
            "BillingModeSummary": {"BillingMode": if self.rcu.is_some() { "PROVISIONED" } else { "PAY_PER_REQUEST" }},
            "ProvisionedThroughput": {"ReadCapacityUnits": self.rcu.unwrap_or(0), "WriteCapacityUnits": self.wcu.unwrap_or(0), "NumberOfDecreasesToday": 0},
        });
        if !self.gsis.is_empty() {
            d["GlobalSecondaryIndexes"] = self
                .gsis
                .iter()
                .map(|g| json!({"IndexName": g.name, "KeySchema": ks(&g.hash, &g.range), "Projection": {"ProjectionType": "ALL"}, "IndexStatus": "ACTIVE", "IndexArn": format!("{}/index/{}", self.arn(), g.name)}))
                .collect::<Vec<_>>()
                .into();
        }
        if self.stream {
            d["StreamSpecification"] =
                json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"});
            d["LatestStreamArn"] = self.stream_arn().into();
            d["LatestStreamLabel"] = (self.created as u64).to_string().into();
        }
        d
    }
}

async fn get_table(txn: &mut dyn KvTxn, name: &str) -> Result<TableDesc> {
    match txn.get(&table_key(name)).await? {
        Some(b) => serde_json::from_slice(&b)
            .map_err(|e| DdbError::internal(format!("corrupt table descriptor: {e}"))),
        None => Err(DdbError::not_found(name)),
    }
}

async fn list_tables(txn: &mut dyn KvTxn) -> Result<Vec<TableDesc>> {
    let start = table_key("");
    let end = keycodec::prefix_end(&start);
    txn.scan(&start, &end, u32::MAX)
        .await?
        .into_iter()
        .map(|(_, v)| {
            serde_json::from_slice(&v)
                .map_err(|e| DdbError::internal(format!("corrupt table descriptor: {e}")))
        })
        .collect()
}

// ---------------------------------------------------------------- server

pub struct App {
    pub store: Arc<dyn KvStore>,
    /// The cluster client, for changefeeds (Streams, the GSI worker).
    pub client: Option<Arc<KvClient>>,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App").finish_non_exhaustive()
    }
}

/// Serve the API on `addr`, and run the GSI worker when `client` is given.
pub async fn serve(
    addr: &str,
    store: Arc<dyn KvStore>,
    client: Option<Arc<KvClient>>,
) -> std::io::Result<()> {
    let app = Arc::new(App { store, client });
    if app.client.is_some() {
        let a = app.clone();
        tokio::spawn(async move { gsi_worker(a).await });
    }
    let router = Router::new().route("/", post(handle)).with_state(app);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "DynamoDB API listening");
    axum::serve(listener, router).await
}

async fn handle(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let op = target.rsplit('.').next().unwrap_or("").to_owned();
    let req: Json = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return error_response(&DdbError::new("SerializationException", e.to_string())),
    };
    let started = std::time::Instant::now();
    let r = dispatch(&app, &op, &req).await;
    tracing::debug!(
        op,
        ms = started.elapsed().as_secs_f64() * 1e3,
        ok = r.is_ok(),
        "ddb"
    );
    let result = match &r {
        Ok(_) => "ok",
        Err(e) => e.kind,
    };
    stowage_common::metrics::inc(
        "stowage_ddb_requests_total",
        &[("op", &op), ("result", result)],
        1.0,
    );
    stowage_common::metrics::observe(
        "stowage_ddb_request_seconds",
        &[("op", &op)],
        started.elapsed().as_secs_f64(),
    );
    match r {
        Ok(v) => (
            [(header::CONTENT_TYPE, "application/x-amz-json-1.0")],
            v.to_string(),
        )
            .into_response(),
        Err(e) => error_response(&e),
    }
}

fn error_response(e: &DdbError) -> Response {
    let mut body = json!({"__type": format!("com.amazonaws.dynamodb.v20120810#{}", e.kind), "message": e.message});
    if let Some(r) = &e.reasons {
        body["CancellationReasons"] = Json::Array(r.clone());
    }
    let status = StatusCode::from_u16(e.status).unwrap_or(StatusCode::BAD_REQUEST);
    (
        status,
        [(header::CONTENT_TYPE, "application/x-amz-json-1.0")],
        body.to_string(),
    )
        .into_response()
}

/// Run `$body` with `$t: &mut dyn KvTxn` in one transaction; retry conflicts.
macro_rules! txn {
    ($app:expr, $t:ident, $body:expr) => {{
        let mut attempt = 0u64;
        loop {
            let mut boxed = $app.store.begin(Isolation::Serializable).await?;
            let r: Result<_> = {
                let $t: &mut dyn KvTxn = boxed.as_mut();
                $body.await
            };
            let r = match r {
                Ok(v) => boxed.commit().await.map(|()| v).map_err(DdbError::from),
                Err(e) => {
                    let _ = boxed.rollback().await;
                    Err(e)
                }
            };
            match r {
                Err(e) if e.kind == "TransactionConflictException" && attempt < 20 => {
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(attempt * 5)).await;
                }
                r => break r,
            }
        }
    }};
}

fn s<'a>(req: &'a Json, field: &str) -> Result<&'a str> {
    req[field].as_str().ok_or_else(|| DdbError::validation(format!("1 validation error detected: Value null at '{field}' failed to satisfy constraint: Member must not be null")))
}

fn obj(req: &Json, field: &str) -> Result<Item> {
    req[field].as_object().cloned().ok_or_else(|| DdbError::validation(format!("1 validation error detected: Value null at '{field}' failed to satisfy constraint: Member must not be null")))
}

async fn dispatch(app: &App, op: &str, req: &Json) -> Result<Json> {
    match op {
        "CreateTable" => create_table(app, req).await,
        "DescribeTable" => {
            let name = s(req, "TableName")?.to_owned();
            let d = txn!(app, t, get_table(t, &name))?;
            Ok(json!({"Table": d.describe(None)}))
        }
        "ListTables" => {
            let tables = txn!(app, t, list_tables(t))?;
            Ok(json!({"TableNames": tables.iter().map(|t| t.name.clone()).collect::<Vec<_>>()}))
        }
        "DeleteTable" => delete_table(app, req).await,
        "PutItem" | "DeleteItem" | "UpdateItem" => {
            let req = req.clone();
            let op = op.to_owned();
            txn!(app, t, write_one(t, &op, &req, true))
        }
        "GetItem" => {
            let req = req.clone();
            txn!(app, t, get_item(t, &req))
        }
        "Query" | "Scan" => {
            let req = req.clone();
            let scan = op == "Scan";
            txn!(app, t, query(t, &req, scan))
        }
        "BatchWriteItem" => batch_write(app, req).await,
        "BatchGetItem" => {
            let req = req.clone();
            txn!(app, t, batch_get(t, &req))
        }
        "TransactWriteItems" => transact_write(app, req).await,
        "TransactGetItems" => {
            let req = req.clone();
            txn!(app, t, transact_get(t, &req))
        }
        "DescribeStream" | "GetShardIterator" | "GetRecords" | "ListStreams" => {
            streams(app, op, req).await
        }
        "UpdateTimeToLive" => update_ttl(app, req).await,
        "DescribeTimeToLive" => {
            let name = s(req, "TableName")?.to_owned();
            let d = txn!(app, t, get_table(t, &name))?;
            Ok(json!({"TimeToLiveDescription": match &d.ttl {
                Some(a) => json!({"TimeToLiveStatus": "ENABLED", "AttributeName": a}),
                None => json!({"TimeToLiveStatus": "DISABLED"}),
            }}))
        }
        "DescribeEndpoints" => {
            Ok(json!({"Endpoints": [{"Address": "localhost", "CachePeriodInMinutes": 1440}]}))
        }
        other => Err(DdbError::new(
            "UnknownOperationException",
            format!("unknown operation {other}"),
        )),
    }
}

async fn create_table(app: &App, req: &Json) -> Result<Json> {
    let name = s(req, "TableName")?.to_owned();
    let attrs: Vec<KeyAttr> = req["AttributeDefinitions"]
        .as_array()
        .ok_or_else(|| DdbError::validation("AttributeDefinitions is required"))?
        .iter()
        .map(|a| {
            Ok(KeyAttr {
                name: s(a, "AttributeName")?.to_owned(),
                ty: s(a, "AttributeType")?.to_owned(),
            })
        })
        .collect::<Result<_>>()?;
    let schema = |ks: &Json| -> Result<(KeyAttr, Option<KeyAttr>)> {
        let find = |kind: &str| -> Option<KeyAttr> {
            let n = ks.as_array()?.iter().find(|k| k["KeyType"] == kind)?["AttributeName"]
                .as_str()?
                .to_owned();
            attrs.iter().find(|a| a.name == n).cloned()
        };
        let hash = find("HASH").ok_or_else(|| {
            DdbError::validation("KeySchema needs a HASH key defined in AttributeDefinitions")
        })?;
        Ok((hash, find("RANGE")))
    };
    let (hash, range) = schema(&req["KeySchema"])?;
    let gsi_specs: Vec<(String, KeyAttr, Option<KeyAttr>)> = req["GlobalSecondaryIndexes"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|g| {
                    Ok((
                        s(g, "IndexName")?.to_owned(),
                        schema(&g["KeySchema"])?.0,
                        schema(&g["KeySchema"])?.1,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();
    let stream = req["StreamSpecification"]["StreamEnabled"]
        .as_bool()
        .unwrap_or(false);
    let (rcu, wcu) = if req["BillingMode"] == "PROVISIONED" {
        let pt = &req["ProvisionedThroughput"];
        (
            pt["ReadCapacityUnits"].as_u64(),
            pt["WriteCapacityUnits"].as_u64(),
        )
    } else {
        (None, None)
    };
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    let d = txn!(app, t, async {
        if t.get(&table_key(&name)).await?.is_some() {
            return Err(DdbError::new(
                "ResourceInUseException",
                format!("Table already exists: {name}"),
            ));
        }
        let id = u32::try_from(stowage_sql::catalog::next_seq(t, b"tables").await?)
            .map_err(|_| DdbError::internal("table ids exhausted"))?;
        let gsis = gsi_specs
            .iter()
            .enumerate()
            .map(|(i, (n, h, r))| Gsi {
                name: n.clone(),
                id: i as u32 + 1,
                hash: h.clone(),
                range: r.clone(),
            })
            .collect();
        let d = TableDesc {
            name: name.clone(),
            id,
            hash: hash.clone(),
            range: range.clone(),
            attributes: attrs.clone(),
            gsis,
            created,
            stream,
            rcu,
            wcu,
            ttl: None,
        };
        t.put(
            &table_key(&name),
            &serde_json::to_vec(&d).map_err(|e| DdbError::internal(e.to_string()))?,
        )
        .await?;
        Ok(d)
    })?;
    Ok(json!({"TableDescription": d.describe(Some(0))}))
}

async fn delete_table(app: &App, req: &Json) -> Result<Json> {
    let name = s(req, "TableName")?.to_owned();
    let d = txn!(app, t, async {
        let d = get_table(t, &name).await?;
        let start = model::prefix(d.id, 0)[..5].to_vec();
        let end = keycodec::prefix_end(&start);
        for (k, _) in t.scan(&start, &end, u32::MAX).await? {
            t.delete(&k).await?;
        }
        t.delete(&table_key(&name)).await?;
        Ok(d)
    })?;
    let mut desc = d.describe(None);
    desc["TableStatus"] = "DELETING".into();
    Ok(json!({"TableDescription": desc}))
}

/// Provisioned throughput: a token bucket per (table, read|write) on this
/// gateway, refilled at the provisioned units per second, holding up to five
/// seconds of them (DynamoDB keeps up to 300 s of unused capacity).
fn throttle(d: &TableDesc, write: bool, cost: f64) -> Result<()> {
    let Some(units) = (if write { d.wcu } else { d.rcu }) else {
        return Ok(());
    };
    /// (table id, is_write) → (tokens left, last refill).
    type Buckets = BTreeMap<(u32, bool), (f64, std::time::Instant)>;
    static BUCKETS: std::sync::OnceLock<std::sync::Mutex<Buckets>> = std::sync::OnceLock::new();
    let mut b = BUCKETS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let rate = units as f64;
    let cap = rate * 5.0;
    let now = std::time::Instant::now();
    let (tokens, last) = b.entry((d.id, write)).or_insert((cap, now));
    *tokens = (*tokens + now.duration_since(*last).as_secs_f64() * rate).min(cap);
    *last = now;
    if *tokens < cost.min(cap) {
        stowage_common::metrics::inc("stowage_ddb_throttled_total", &[("table", &d.name)], 1.0);
        return Err(DdbError::new(
            "ProvisionedThroughputExceededException",
            "The level of configured provisioned throughput for the table was exceeded. Consider increasing your provisioning level with the UpdateTable API.",
        ));
    }
    *tokens -= cost;
    Ok(())
}

fn condition(req: &Json, field: &str, b: &Bindings) -> Result<Option<Cond>> {
    req[field]
        .as_str()
        .map(|c| expr::parse_condition(c, b))
        .transpose()
}

fn key_only(d: &TableDesc, key: &Item) -> Result<Item> {
    let mut k = Map::new();
    for a in d.key_attrs() {
        k.insert(
            a.name.clone(),
            key.get(&a.name).cloned().ok_or_else(|| {
                DdbError::validation("The provided key element does not match the schema")
            })?,
        );
    }
    if key.len() != k.len() {
        return Err(DdbError::validation(
            "The provided key element does not match the schema",
        ));
    }
    Ok(k)
}

/// One PutItem / DeleteItem / UpdateItem (also a TransactWriteItems element):
/// read the current item, check the condition, write. `report`: return the
/// DynamoDB response (else the caller only needs success).
async fn write_one(t: &mut dyn KvTxn, op: &str, req: &Json, report: bool) -> Result<Json> {
    let d = get_table(t, s(req, "TableName")?).await?;
    throttle(&d, op != "ConditionCheck", 1.0)?;
    let b = Bindings::from_request(req);
    let cond = condition(req, "ConditionExpression", &b)?;
    let (key, new): (Vec<u8>, Option<Item>);
    let old: Option<Item>;
    match op {
        "PutItem" | "Put" => {
            let item = obj(req, "Item")?;
            key = d.base_key(&item)?;
            old = t.get(&key).await?.map(|v| decode_item(&v)).transpose()?;
            new = Some(item);
        }
        "DeleteItem" | "Delete" => {
            let k = key_only(&d, &obj(req, "Key")?)?;
            key = d.base_key(&k)?;
            old = t.get(&key).await?.map(|v| decode_item(&v)).transpose()?;
            new = None;
        }
        "UpdateItem" | "Update" => {
            let k = key_only(&d, &obj(req, "Key")?)?;
            key = d.base_key(&k)?;
            old = t.get(&key).await?.map(|v| decode_item(&v)).transpose()?;
            let mut item = old.clone().unwrap_or_else(|| k.clone());
            if let Some(u) = req["UpdateExpression"].as_str() {
                let actions = expr::parse_update(u, &b)?;
                for a in &actions {
                    let p = match a {
                        expr::Action::Set(p, _)
                        | expr::Action::Remove(p)
                        | expr::Action::Add(p, _)
                        | expr::Action::Delete(p, _) => p,
                    };
                    if let Some(expr::Step::Attr(top)) = p.first()
                        && d.key_attrs().any(|ka| ka.name == *top)
                    {
                        return Err(DdbError::validation(format!(
                            "One or more parameter values were invalid: Cannot update attribute {top}. This attribute is part of the key"
                        )));
                    }
                }
                expr::apply(&mut item, &actions)?;
            }
            new = Some(item);
        }
        "ConditionCheck" => {
            let k = key_only(&d, &obj(req, "Key")?)?;
            key = d.base_key(&k)?;
            old = t.get(&key).await?.map(|v| decode_item(&v)).transpose()?;
            new = old.clone();
        }
        other => return Err(DdbError::validation(format!("unknown write {other}"))),
    }
    if let Some(c) = &cond
        && !expr::eval(c, old.as_ref().unwrap_or(&Map::new()))
    {
        let mut e = DdbError::conditional();
        if req["ReturnValuesOnConditionCheckFailure"] == "ALL_OLD"
            && let Some(o) = &old
        {
            e.message = format!("{} ({} attributes)", e.message, o.len());
        }
        return Err(e);
    }
    if op != "ConditionCheck" {
        match &new {
            Some(item) => t.put(&key, &encode_item(item)).await?,
            None => t.delete(&key).await?,
        }
    }
    if !report {
        return Ok(Json::Null);
    }
    let rv = req["ReturnValues"].as_str().unwrap_or("NONE");
    let attrs = match rv {
        "ALL_OLD" => old.clone(),
        "ALL_NEW" => new.clone(),
        "UPDATED_NEW" | "UPDATED_OLD" => {
            let (from, other) = if rv == "UPDATED_NEW" {
                (&new, &old)
            } else {
                (&old, &new)
            };
            from.as_ref().map(|f| {
                f.iter()
                    .filter(|(k, v)| other.as_ref().and_then(|o| o.get(*k)) != Some(*v))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
        }
        _ => None,
    };
    Ok(match attrs {
        Some(a) if !a.is_empty() => json!({"Attributes": a}),
        _ => json!({}),
    })
}

async fn get_item(t: &mut dyn KvTxn, req: &Json) -> Result<Json> {
    let d = get_table(t, s(req, "TableName")?).await?;
    throttle(&d, false, 1.0)?;
    let key = d.base_key(&key_only(&d, &obj(req, "Key")?)?)?;
    let b = Bindings::from_request(req);
    let proj = req["ProjectionExpression"]
        .as_str()
        .map(|p| expr::parse_projection(p, &b))
        .transpose()?;
    Ok(match t.get(&key).await? {
        Some(v) => {
            let item = decode_item(&v)?;
            json!({"Item": proj.map_or(item.clone(), |p| expr::project(&item, &p))})
        }
        None => json!({}),
    })
}

/// The key span a KeyConditionExpression selects: `hash = :h` and optionally
/// one condition on the range key.
fn key_span(
    d: &TableDesc,
    index: Option<&Gsi>,
    c: &Cond,
    item_for_values: &Item,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let _ = item_for_values;
    let (hash, range) = match index {
        Some(g) => (&g.hash, &g.range),
        None => (&d.hash, &d.range),
    };
    let mut conds = Vec::new();
    fn flat<'a>(c: &'a Cond, out: &mut Vec<&'a Cond>) {
        match c {
            Cond::And(a, b) => {
                flat(a, out);
                flat(b, out);
            }
            c => out.push(c),
        }
    }
    flat(c, &mut conds);
    let name = |o: &expr::Operand| match o {
        expr::Operand::Path(p) => match p.as_slice() {
            [expr::Step::Attr(a)] => Some(a.clone()),
            _ => None,
        },
        _ => None,
    };
    let value = |o: &expr::Operand| match o {
        expr::Operand::Value(v) => Some(v.clone()),
        _ => None,
    };
    let mut base = prefix(d.id, index.map_or(BASE, |g| g.id));
    let hv = conds
        .iter()
        .find_map(|c| match c {
            Cond::Cmp(l, "=", r) if name(l).as_deref() == Some(&hash.name) => value(r),
            Cond::Cmp(l, "=", r) if name(r).as_deref() == Some(&hash.name) => value(l),
            _ => None,
        })
        .ok_or_else(|| {
            DdbError::validation(format!(
                "Query condition missed key schema element: {}",
                hash.name
            ))
        })?;
    encode_key_attr(&hv, &mut base)?;
    let mut start = base.clone();
    let mut end = keycodec::prefix_end(&base);
    let enc = |v: &Json| -> Result<Vec<u8>> {
        let mut k = base.clone();
        encode_key_attr(v, &mut k)?;
        Ok(k)
    };
    for c in conds {
        let is_range = |o: &expr::Operand| {
            range
                .as_ref()
                .is_some_and(|r| name(o).as_deref() == Some(&r.name))
        };
        match c {
            Cond::Cmp(l, _, _) if name(l).as_deref() == Some(&hash.name) => {}
            Cond::Cmp(l, op, r) if is_range(l) => {
                let v = value(r).ok_or_else(|| DdbError::validation("bad key condition"))?;
                let k = enc(&v)?;
                match *op {
                    "=" => {
                        start = k.clone();
                        end = keycodec::prefix_end(&k);
                    }
                    "<" => end = k,
                    "<=" => end = keycodec::prefix_end(&k),
                    ">" => start = keycodec::prefix_end(&k),
                    ">=" => start = k,
                    _ => {
                        return Err(DdbError::validation(
                            "Unsupported operator on KeyConditionExpression",
                        ));
                    }
                }
            }
            Cond::Between(x, lo, hi) if is_range(x) => {
                start = enc(&value(lo).ok_or_else(|| DdbError::validation("bad key condition"))?)?;
                end = keycodec::prefix_end(&enc(
                    &value(hi).ok_or_else(|| DdbError::validation("bad key condition"))?
                )?);
            }
            Cond::BeginsWith(x, p) if is_range(x) => {
                let mut k = base.clone();
                k.extend(model::key_prefix(
                    &value(p).ok_or_else(|| DdbError::validation("bad key condition"))?,
                )?);
                end = keycodec::prefix_end(&k);
                start = k;
            }
            other => {
                return Err(DdbError::validation(format!(
                    "Query key condition not supported: {other:?}"
                )));
            }
        }
    }
    Ok((start, end))
}

/// The key attributes DynamoDB reports as `LastEvaluatedKey`.
fn last_key(d: &TableDesc, index: Option<&Gsi>, item: &Item) -> Item {
    let mut k = Map::new();
    let attrs: Vec<&KeyAttr> = d
        .key_attrs()
        .chain(
            index
                .into_iter()
                .flat_map(|g| std::iter::once(&g.hash).chain(g.range.as_ref())),
        )
        .collect();
    for a in attrs {
        if let Some(v) = item.get(&a.name) {
            k.insert(a.name.clone(), v.clone());
        }
    }
    k
}

async fn query(t: &mut dyn KvTxn, req: &Json, scan: bool) -> Result<Json> {
    let d = get_table(t, s(req, "TableName")?).await?;
    throttle(&d, false, 1.0)?;
    let b = Bindings::from_request(req);
    let index = match req["IndexName"].as_str() {
        Some(n) => Some(
            d.gsis
                .iter()
                .find(|g| g.name == n)
                .cloned()
                .ok_or_else(|| {
                    DdbError::validation(format!(
                        "The table does not have the specified index: {n}"
                    ))
                })?,
        ),
        None => None,
    };
    let (mut start, end) = if scan {
        let p = prefix(d.id, index.as_ref().map_or(BASE, |g| g.id));
        let e = keycodec::prefix_end(&p);
        (p, e)
    } else {
        let kc = expr::parse_condition(s(req, "KeyConditionExpression")?, &b)?;
        key_span(&d, index.as_ref(), &kc, &Map::new())?
    };
    let forward = req["ScanIndexForward"].as_bool().unwrap_or(true);
    if let Some(esk) = req["ExclusiveStartKey"].as_object() {
        // Resume after the item with this key.
        let mut k = match &index {
            Some(g) => d
                .gsi_key(g, esk)?
                .ok_or_else(|| DdbError::validation("The provided starting key is invalid"))?,
            None => d.base_key(esk)?,
        };
        if forward {
            k.push(0);
            start = start.max(k);
        } else {
            return Err(DdbError::validation(
                "ExclusiveStartKey with ScanIndexForward=false is not supported",
            ));
        }
    }
    let limit = req["Limit"].as_u64().map(|l| l as usize);
    let filter = condition(req, "FilterExpression", &b)?;
    let proj = req["ProjectionExpression"]
        .as_str()
        .map(|p| expr::parse_projection(p, &b))
        .transpose()?;
    // Limit counts items read before the filter, as in DynamoDB.
    let fetch = if forward {
        limit.map_or(u32::MAX, |l| l as u32)
    } else {
        u32::MAX
    };
    let mut pairs = t.scan(&start, &end, fetch).await?;
    if !forward {
        pairs.reverse();
        if let Some(l) = limit {
            pairs.truncate(l);
        }
    }
    let read = pairs.len();
    // The first unit was taken up front; the rest of what was read is owed now.
    if read > 1 {
        let _ = throttle(&d, false, (read - 1) as f64);
    }
    let mut items = Vec::new();
    let mut last = None;
    for (_, v) in pairs {
        let item = decode_item(&v)?;
        last = Some(item.clone());
        if filter.as_ref().is_none_or(|f| expr::eval(f, &item)) {
            items.push(match &proj {
                Some(p) => expr::project(&item, p),
                None => item,
            });
        }
    }
    let mut out = json!({"Count": items.len(), "ScannedCount": read});
    if req["Select"] != "COUNT" {
        out["Items"] = Json::Array(items.into_iter().map(Json::Object).collect());
    }
    if let (Some(l), Some(it)) = (limit, last)
        && read >= l
    {
        out["LastEvaluatedKey"] = Json::Object(last_key(&d, index.as_ref(), &it));
    }
    Ok(out)
}

async fn batch_write(app: &App, req: &Json) -> Result<Json> {
    let items = req["RequestItems"]
        .as_object()
        .ok_or_else(|| DdbError::validation("RequestItems is required"))?
        .clone();
    // Each request is applied on its own, as in DynamoDB (no atomicity across them).
    for (table, reqs) in &items {
        for r in reqs.as_array().into_iter().flatten() {
            let (op, mut one) = if let Some(p) = r.get("PutRequest") {
                ("PutItem", json!({"Item": p["Item"]}))
            } else if let Some(dr) = r.get("DeleteRequest") {
                ("DeleteItem", json!({"Key": dr["Key"]}))
            } else {
                return Err(DdbError::validation(
                    "BatchWriteItem needs PutRequest or DeleteRequest",
                ));
            };
            one["TableName"] = Json::String(table.clone());
            txn!(app, t, write_one(t, op, &one, false))?;
        }
    }
    Ok(json!({"UnprocessedItems": {}}))
}

async fn batch_get(t: &mut dyn KvTxn, req: &Json) -> Result<Json> {
    let mut out = Map::new();
    for (table, spec) in req["RequestItems"].as_object().into_iter().flatten() {
        let d = get_table(t, table).await?;
        let b = Bindings::from_request(spec);
        let proj = spec["ProjectionExpression"]
            .as_str()
            .map(|p| expr::parse_projection(p, &b))
            .transpose()?;
        let mut found = Vec::new();
        for k in spec["Keys"].as_array().into_iter().flatten() {
            let key = d.base_key(&key_only(
                &d,
                k.as_object()
                    .ok_or_else(|| DdbError::validation("bad key"))?,
            )?)?;
            if let Some(v) = t.get(&key).await? {
                let item = decode_item(&v)?;
                found.push(Json::Object(
                    proj.as_ref()
                        .map_or(item.clone(), |p| expr::project(&item, p)),
                ));
            }
        }
        out.insert(table.clone(), Json::Array(found));
    }
    Ok(json!({"Responses": out, "UnprocessedKeys": {}}))
}

/// All writes in one transaction. A failed condition cancels everything, with
/// one reason per item, as DynamoDB reports it.
async fn transact_write(app: &App, req: &Json) -> Result<Json> {
    let items = req["TransactItems"]
        .as_array()
        .cloned()
        .ok_or_else(|| DdbError::validation("TransactItems is required"))?;
    if items.len() > 100 {
        return Err(DdbError::validation(
            "Member must have length less than or equal to 100",
        ));
    }
    let r = txn!(app, t, async {
        for (i, it) in items.iter().enumerate() {
            let (op, body) = ["Put", "Update", "Delete", "ConditionCheck"]
                .iter()
                .find_map(|k| it.get(*k).map(|b| (*k, b)))
                .ok_or_else(|| {
                    DdbError::validation(
                        "each TransactItem needs Put, Update, Delete or ConditionCheck",
                    )
                })?;
            if let Err(e) = write_one(t, op, body, false).await {
                if e.kind == "ConditionalCheckFailedException" {
                    let reasons: Vec<Json> = (0..items.len())
                        .map(|j| if j == i { json!({"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"}) } else { json!({"Code": "None"}) })
                        .collect();
                    return Err(DdbError {
                        kind: "TransactionCanceledException",
                        message: format!(
                            "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
                            reasons
                                .iter()
                                .map(|r| r["Code"].as_str().unwrap_or("None"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                        status: 400,
                        reasons: Some(reasons),
                    });
                }
                return Err(e);
            }
        }
        Ok(())
    });
    r.map(|()| json!({}))
}

async fn transact_get(t: &mut dyn KvTxn, req: &Json) -> Result<Json> {
    let mut out = Vec::new();
    for it in req["TransactItems"].as_array().into_iter().flatten() {
        let r = get_item(t, &it["Get"]).await?;
        out.push(if r.get("Item").is_some() {
            json!({"Item": r["Item"]})
        } else {
            json!({})
        });
    }
    Ok(json!({"Responses": out}))
}

// ---------------------------------------------------------------- streams

/// The greatest timestamp below `t`.
fn just_before(t: Timestamp) -> Timestamp {
    if t.logical > 0 {
        Timestamp::new(t.wall, t.logical - 1)
    } else {
        Timestamp::new(t.wall.saturating_sub(1), u32::MAX)
    }
}

fn ts_str(t: Timestamp) -> String {
    format!("{:020}{:010}", t.wall, t.logical)
}

fn parse_ts(s: &str) -> Option<Timestamp> {
    (s.len() == 30).then(|| Some(Timestamp::new(s[..20].parse().ok()?, s[20..].parse().ok()?)))?
}

fn iterator(table: &str, after: Timestamp) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(json!({"t": table, "ts": ts_str(after)}).to_string())
}

async fn streams(app: &App, op: &str, req: &Json) -> Result<Json> {
    let client = app
        .client
        .clone()
        .ok_or_else(|| DdbError::validation("streams need a cluster"))?;
    let table_of_arn = |arn: &str| {
        arn.split("table/")
            .nth(1)
            .and_then(|r| r.split('/').next())
            .map(str::to_owned)
    };
    match op {
        "ListStreams" => {
            let tables = txn!(app, t, list_tables(t))?;
            Ok(
                json!({"Streams": tables.iter().filter(|t| t.stream).map(|t| json!({"StreamArn": t.stream_arn(), "TableName": t.name, "StreamLabel": (t.created as u64).to_string()})).collect::<Vec<_>>()}),
            )
        }
        "DescribeStream" => {
            let name = table_of_arn(s(req, "StreamArn")?)
                .ok_or_else(|| DdbError::validation("bad StreamArn"))?;
            let d = txn!(app, t, get_table(t, &name))?;
            Ok(json!({"StreamDescription": {
                "StreamArn": d.stream_arn(), "StreamLabel": (d.created as u64).to_string(), "StreamStatus": "ENABLED",
                "StreamViewType": "NEW_AND_OLD_IMAGES", "TableName": d.name, "KeySchema": d.describe(None)["KeySchema"],
                "Shards": [{"ShardId": "shardId-00000000000000000000-00000000", "SequenceNumberRange": {"StartingSequenceNumber": "0"}}],
            }}))
        }
        "GetShardIterator" => {
            let name = table_of_arn(s(req, "StreamArn")?)
                .ok_or_else(|| DdbError::validation("bad StreamArn"))?;
            let after = match s(req, "ShardIteratorType")? {
                "TRIM_HORIZON" => Timestamp::MIN,
                "LATEST" => client.now(),
                "AT_SEQUENCE_NUMBER" => parse_ts(s(req, "SequenceNumber")?)
                    .map(|t| Timestamp::new(t.wall, t.logical.saturating_sub(1)))
                    .unwrap_or(Timestamp::MIN),
                "AFTER_SEQUENCE_NUMBER" => {
                    parse_ts(s(req, "SequenceNumber")?).unwrap_or(Timestamp::MIN)
                }
                other => {
                    return Err(DdbError::validation(format!(
                        "unknown ShardIteratorType {other}"
                    )));
                }
            };
            Ok(json!({"ShardIterator": iterator(&name, after)}))
        }
        _ => {
            let raw = base64::engine::general_purpose::STANDARD
                .decode(s(req, "ShardIterator")?)
                .map_err(|_| DdbError::validation("bad ShardIterator"))?;
            let it: Json = serde_json::from_slice(&raw)
                .map_err(|_| DdbError::validation("bad ShardIterator"))?;
            let name = it["t"].as_str().unwrap_or("").to_owned();
            let from = it["ts"]
                .as_str()
                .and_then(parse_ts)
                .unwrap_or(Timestamp::MIN);
            let d = txn!(app, t, get_table(t, &name))?;
            let start = prefix(d.id, BASE);
            let end = keycodec::prefix_end(&start);
            let to = client.now();
            let mut changes = client
                .changes(&start, &end, from, to)
                .await
                .map_err(|e| DdbError::internal(e.to_string()))?;
            changes.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.key.cmp(&b.key)));
            let limit = req["Limit"].as_u64().unwrap_or(1000) as usize;
            let mut next = to;
            if changes.len() > limit {
                next = changes[limit - 1].ts;
                // Never split one timestamp across pages.
                changes.retain(|c| c.ts <= next);
            }
            let mut records = Vec::new();
            for c in &changes {
                let new = c.value.as_deref().map(decode_item).transpose()?;
                let old = match client.read_at(&c.key, just_before(c.ts)).await {
                    Ok(Some(v)) => v.as_deref().map(decode_item).transpose()?,
                    _ => None,
                };
                let keys = last_key(
                    &d,
                    None,
                    new.as_ref().or(old.as_ref()).unwrap_or(&Map::new()),
                );
                let name = match (&old, &new) {
                    (_, None) => "REMOVE",
                    (None, Some(_)) => "INSERT",
                    (Some(_), Some(_)) => "MODIFY",
                };
                let mut rec = json!({"Keys": keys, "SequenceNumber": ts_str(c.ts), "SizeBytes": c.value.as_ref().map_or(0, Vec::len), "StreamViewType": "NEW_AND_OLD_IMAGES", "ApproximateCreationDateTime": c.ts.wall as f64 / 1e9});
                if let Some(n) = new {
                    rec["NewImage"] = Json::Object(n);
                }
                if let Some(o) = old {
                    rec["OldImage"] = Json::Object(o);
                }
                records.push(json!({"eventID": ts_str(c.ts), "eventName": name, "eventVersion": "1.1", "eventSource": "aws:dynamodb", "awsRegion": "stowage", "dynamodb": rec}));
            }
            Ok(json!({"Records": records, "NextShardIterator": iterator(&name, next)}))
        }
    }
}

// ---------------------------------------------------------------- GSI worker

const LEASE_KEY: &[u8] = b"\x01ddb-gsi/lease";
const CURSOR_KEY: &[u8] = b"\x01ddb-gsi/cursor";

/// Keeps every GSI up to date from the changefeed over all items. One worker
/// in the cluster holds a lease (renewed every second, 3 s long); its cursor
/// and the index entries it writes commit in the same transaction, so a crash
/// or a lease change never applies a change twice or skips one.
async fn gsi_worker(app: Arc<App>) {
    let Some(client) = app.client.clone() else {
        return;
    };
    let me = format!("{:016x}", client.now().wall ^ u64::from(std::process::id()));
    let mut round = 0u64;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        round += 1;
        match gsi_round(&app, &client, &me).await {
            Err(e) => tracing::debug!(error = %e.message, "gsi worker round failed"),
            // The lease holder also expires items, about once a second.
            Ok(true) if round.is_multiple_of(TTL_EVERY_ROUNDS) => {
                if let Err(e) = ttl_round(&app).await {
                    tracing::debug!(error = %e.message, "ttl round failed");
                }
            }
            Ok(_) => {}
        }
    }
}

const TTL_EVERY_ROUNDS: u64 = 10;
/// Items deleted per table per round, so one round stays a small transaction.
const TTL_BATCH: usize = 100;

/// `UpdateTimeToLive`. As in DynamoDB, enabling the attribute that is already
/// enabled is a `ValidationException`.
async fn update_ttl(app: &App, req: &Json) -> Result<Json> {
    let name = s(req, "TableName")?.to_owned();
    let spec = req["TimeToLiveSpecification"].clone();
    let enabled = spec["Enabled"]
        .as_bool()
        .ok_or_else(|| DdbError::validation("TimeToLiveSpecification.Enabled is required"))?;
    let attr = spec["AttributeName"]
        .as_str()
        .ok_or_else(|| DdbError::validation("TimeToLiveSpecification.AttributeName is required"))?
        .to_owned();
    txn!(app, t, async {
        let mut d = get_table(t, &name).await?;
        match (&d.ttl, enabled) {
            (Some(a), true) if *a == attr => {
                return Err(DdbError::validation("TimeToLive is already enabled"));
            }
            (Some(_), true) => {
                return Err(DdbError::validation(
                    "TimeToLive is active on a different AttributeName: disable it first",
                ));
            }
            (None, false) => return Err(DdbError::validation("TimeToLive is already disabled")),
            _ => {}
        }
        d.ttl = enabled.then(|| attr.clone());
        t.put(
            &table_key(&name),
            &serde_json::to_vec(&d).map_err(|e| DdbError::internal(e.to_string()))?,
        )
        .await?;
        Ok(())
    })?;
    Ok(json!({"TimeToLiveSpecification": {"Enabled": enabled, "AttributeName": attr}}))
}

/// Delete items whose TTL attribute (a number, epoch seconds) is in the past.
/// The deletes are ordinary writes, so streams record them as `REMOVE` and the
/// GSI worker drops their index entries. Like DynamoDB, deletion is background
/// work: an expired item stays readable until a round reaches it, and items
/// whose attribute is missing or not a number never expire.
async fn ttl_round(app: &App) -> Result<u64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    txn!(app, t, async {
        let mut deleted = 0u64;
        for d in list_tables(t).await? {
            let Some(attr) = &d.ttl else { continue };
            let start = prefix(d.id, BASE);
            let end = keycodec::prefix_end(&start);
            let mut batch = Vec::new();
            for (k, v) in t.scan(&start, &end, u32::MAX).await? {
                let item = decode_item(&v)?;
                let expired = item
                    .get(attr)
                    .and_then(|a| a["N"].as_str())
                    .and_then(|n| n.parse::<f64>().ok())
                    .is_some_and(|exp| exp < now);
                if expired {
                    batch.push(k);
                    if batch.len() == TTL_BATCH {
                        break;
                    }
                }
            }
            for k in batch {
                t.delete(&k).await?;
                deleted += 1;
            }
            if deleted > 0 {
                stowage_common::metrics::inc(
                    "stowage_ddb_ttl_deleted_total",
                    &[("table", &d.name)],
                    deleted as f64,
                );
            }
        }
        Ok(deleted)
    })
}

/// How far the GSIs have caught up (for tests and DescribeTable).
pub async fn gsi_cursor(t: &mut dyn KvTxn) -> Result<Timestamp> {
    Ok(t.get(CURSOR_KEY)
        .await?
        .and_then(|b| parse_ts(&String::from_utf8_lossy(&b)))
        .unwrap_or(Timestamp::MIN))
}

async fn gsi_round(app: &App, client: &KvClient, me: &str) -> Result<bool> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);
    let me_owned = me.to_owned();
    let leader = txn!(app, t, async {
        let lease: Option<Json> = t
            .get(LEASE_KEY)
            .await?
            .and_then(|b| serde_json::from_slice(&b).ok());
        let until = lease
            .as_ref()
            .and_then(|l| l["until"].as_u64())
            .unwrap_or(0);
        let mine = lease
            .as_ref()
            .is_some_and(|l| l["owner"] == me_owned.as_str());
        let free = mine || until < now_ms;
        // Renew only when less than 2 s remain: about one write a second.
        if free && (!mine || until < now_ms + 2000) {
            t.put(
                LEASE_KEY,
                json!({"owner": me_owned, "until": now_ms + 3000})
                    .to_string()
                    .as_bytes(),
            )
            .await?;
        }
        Ok(free)
    })?;
    if !leader {
        return Ok(false);
    }
    let from = txn!(app, t, gsi_cursor(t))?;
    let to = client.now();
    let mut changes = client
        .changes(&[ITEMS], &[ITEMS + 1], from, to)
        .await
        .map_err(|e| DdbError::internal(e.to_string()))?;
    changes.retain(|c| c.key.len() >= 9 && c.key[5..9] == BASE.to_be_bytes());
    changes.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.key.cmp(&b.key)));
    // Nothing to index: leave the cursor (a wider window next time is as
    // correct), but move it now and then so windows stay short.
    if changes.is_empty() && to.wall.saturating_sub(from.wall) < 5_000_000_000 {
        return Ok(true);
    }
    txn!(app, t, async {
        if gsi_cursor(t).await? != from {
            return Ok(()); // another worker got here first
        }
        let tables: BTreeMap<u32, TableDesc> = list_tables(t)
            .await?
            .into_iter()
            .map(|d| (d.id, d))
            .collect();
        for c in &changes {
            let id = u32::from_be_bytes(
                c.key[1..5]
                    .try_into()
                    .map_err(|_| DdbError::internal("bad key"))?,
            );
            let Some(d) = tables.get(&id) else { continue };
            if d.gsis.is_empty() {
                continue;
            }
            let ledger = ledger_key(id, &c.key);
            // Remove what the previous version indexed, then index this one.
            if let Some(prev) = t.get(&ledger).await? {
                let old: Vec<Vec<u8>> = serde_json::from_slice(&prev).unwrap_or_default();
                for k in old {
                    t.delete(&k).await?;
                }
            }
            match &c.value {
                Some(v) => {
                    let item = decode_item(v)?;
                    let mut entries = Vec::new();
                    for g in &d.gsis {
                        if let Some(k) = d.gsi_key(g, &item)? {
                            t.put(&k, v).await?;
                            entries.push(k);
                        }
                    }
                    t.put(&ledger, &serde_json::to_vec(&entries).unwrap_or_default())
                        .await?;
                }
                None => t.delete(&ledger).await?,
            }
        }
        t.put(CURSOR_KEY, ts_str(to).as_bytes()).await?;
        Ok(())
    })?;
    Ok(true)
}
