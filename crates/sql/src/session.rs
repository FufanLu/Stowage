//! A SQL session: statements, transactions, retries, EXPLAIN.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use sqlparser::ast::{self, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use stowage_common::os::Xoshiro256;
use stowage_kv::txn::Isolation;

use crate::binder::{Binder, object_name, unsupported};
use crate::exec::{self, NodeStat};
use crate::kv::{KvStore, KvTxn};
use crate::plan::Plan;
use crate::value::{DataType, Value, build_array};
use crate::{Error, Result, dml, explain, optimizer};

/// What a statement returns.
#[derive(Debug, Clone)]
pub enum Output {
    Rows {
        columns: Vec<(String, DataType)>,
        batch: RecordBatch,
    },
    /// A command tag, as Postgres reports it (`INSERT 0 3`, `CREATE TABLE`, …).
    Command(String),
}

impl Output {
    /// Rows as values.
    pub fn values(&self) -> Vec<Vec<Value>> {
        match self {
            Self::Rows { batch, .. } => (0..batch.num_rows())
                .map(|r| {
                    (0..batch.num_columns())
                        .map(|c| {
                            Value::from_array(batch.column(c).as_ref(), r).unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .collect(),
            Self::Command(_) => Vec::new(),
        }
    }

    /// Rows as text (tests, the CLI).
    pub fn rows(&self) -> Vec<Vec<Option<String>>> {
        match self {
            Self::Rows { batch, .. } => (0..batch.num_rows())
                .map(|r| {
                    (0..batch.num_columns())
                        .map(|c| {
                            Value::from_array(batch.column(c).as_ref(), r)
                                .ok()
                                .and_then(|v| v.to_pg_text())
                        })
                        .collect()
                })
                .collect(),
            Self::Command(_) => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    Idle,
    InTransaction,
    Failed,
}

pub struct Session {
    store: Arc<dyn KvStore>,
    txn: Option<Box<dyn KvTxn>>,
    failed: bool,
    isolation: Isolation,
    next_isolation: Option<Isolation>,
    vars: BTreeMap<String, String>,
    rng: Xoshiro256,
    /// Autocommit statements retried after a serialization conflict.
    pub retries: u64,
    /// Where the last statement spent its time: (stage, milliseconds).
    pub timings: Vec<(&'static str, f64)>,
    /// Steps of the last online schema change, with milliseconds since it began.
    pub schema_log: Vec<(f64, String)>,
    /// The authenticated user (pgwire sets it); `None` when authentication is off.
    user_name: Option<String>,
    /// Its record, re-read before every statement so grants apply at once.
    user: Option<crate::auth::User>,
}

/// Run `$body` (which may use the binder `$b`) in its own transaction, retrying
/// on serialization conflicts.
macro_rules! own_txn {
    ($self:ident, $b:ident, $body:expr) => {{
        let mut attempt = 0;
        loop {
            let mut txn = $self.store.begin($self.isolation).await?;
            let r = {
                let mut $b = Binder::new(txn.as_mut(), Vec::new(), now_micros());
                $body.await
            };
            let r = match r {
                Ok(v) => txn.commit().await.map(|()| v),
                Err(e) => {
                    let _ = txn.rollback().await;
                    Err(e)
                }
            };
            match r {
                Err(Error::Retry(_)) if attempt < AUTOCOMMIT_RETRIES => {
                    attempt += 1;
                    $self.retries += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(attempt as u64 * 5)).await;
                }
                r => break r,
            }
        }
    }};
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

const AUTOCOMMIT_RETRIES: usize = 20;

pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    Parser::parse_sql(&PostgreSqlDialect {}, &user_as_role(sql))
        .map_err(|e| Error::Parse(e.to_string()))
}

/// Postgres' `CREATE/ALTER/DROP USER` are `… ROLE` (with LOGIN); the parser
/// knows only the ROLE forms. Rewrites the keyword outside quotes.
fn user_as_role(sql: &str) -> std::borrow::Cow<'_, str> {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains(" USER") {
        return std::borrow::Cow::Borrowed(sql);
    }
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut copied = 0; // sql[..copied] is in `out`
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
            i += 1;
            continue;
        }
        let boundary = i == 0 || !b[i - 1].is_ascii_alphanumeric();
        let hit = ["CREATE USER", "ALTER USER", "DROP USER"]
            .into_iter()
            .find(|p| {
                boundary
                    && upper[i..].starts_with(p)
                    && upper
                        .as_bytes()
                        .get(i + p.len())
                        .is_none_or(|x| !x.is_ascii_alphanumeric() && *x != b'_')
            });
        match hit {
            Some(p) => {
                let verb = p.len() - " USER".len();
                out.push_str(&sql[copied..i + verb]);
                out.push_str(" ROLE");
                i += p.len();
                copied = i;
            }
            None => i += 1,
        }
    }
    out.push_str(&sql[copied..]);
    std::borrow::Cow::Owned(out)
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

impl Session {
    pub fn new(store: Arc<dyn KvStore>) -> Self {
        let mut vars = BTreeMap::new();
        for (k, v) in [
            ("server_version", "16.0"),
            ("server_encoding", "UTF8"),
            ("client_encoding", "UTF8"),
            ("datestyle", "ISO, MDY"),
            ("timezone", "UTC"),
            ("standard_conforming_strings", "on"),
            ("integer_datetimes", "on"),
            ("transaction_isolation", "serializable"),
            ("search_path", "public"),
        ] {
            vars.insert(k.to_owned(), v.to_owned());
        }
        Self {
            store,
            txn: None,
            failed: false,
            isolation: Isolation::Serializable,
            next_isolation: None,
            vars,
            rng: Xoshiro256::from_entropy(),
            retries: 0,
            timings: Vec::new(),
            schema_log: Vec::new(),
            user_name: None,
            user: None,
        }
    }

    /// Run as `name` (after it authenticated). `None`: no checks.
    pub fn set_user(&mut self, name: Option<String>) {
        self.user_name = name;
    }

    fn who(&self) -> &str {
        self.user_name.as_deref().unwrap_or("-")
    }

    pub fn status(&self) -> TxStatus {
        match (&self.txn, self.failed) {
            (_, true) => TxStatus::Failed,
            (Some(_), _) => TxStatus::InTransaction,
            (None, _) => TxStatus::Idle,
        }
    }

    pub fn var(&self, name: &str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }

    /// Run every statement in `sql` (the simple-query protocol): stop at the first error.
    pub async fn execute(&mut self, sql: &str) -> Result<Vec<Output>> {
        let mut out = Vec::new();
        for s in parse(sql)? {
            out.push(self.run(&s, &[]).await?);
        }
        Ok(out)
    }

    /// Run one statement with `$n` parameters (the extended protocol).
    pub async fn run(&mut self, stmt: &Statement, params: &[Option<String>]) -> Result<Output> {
        let started = Instant::now();
        let r = self.run_inner(stmt, params).await;
        let ms = started.elapsed().as_secs_f64() * 1e3;
        if !matches!(stmt, Statement::Set(_) | Statement::ShowVariable { .. }) {
            let text = summary_full(stmt);
            let rows = match &r {
                Ok(Output::Rows { batch, .. }) => batch.num_rows() as u64,
                Ok(Output::Command(tag)) => tag
                    .rsplit(' ')
                    .next()
                    .and_then(|n| n.parse().ok())
                    .unwrap_or(0),
                Err(_) => 0,
            };
            crate::stats::record(&text, ms, rows, r.is_ok());
            let threshold: f64 = self
                .var("slow_query_ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(500.0);
            if ms >= threshold {
                crate::stats::slow(self.who(), &text, ms, &self.timings);
            }
        }
        r
    }

    async fn run_inner(&mut self, stmt: &Statement, params: &[Option<String>]) -> Result<Output> {
        self.timings.clear();
        if let Some(name) = self.user_name.clone() {
            let mut t = self.store.begin(Isolation::Serializable).await?;
            let u = crate::auth::get_user(t.as_mut(), &name).await;
            let _ = t.rollback().await;
            self.user = Some(u?.ok_or_else(|| Error::UndefinedObject(format!("role \"{name}\"")))?);
        }
        let admin = matches!(
            stmt,
            Statement::CreateTable(_)
                | Statement::AlterTable(_)
                | Statement::CreateIndex(_)
                | Statement::Drop { .. }
                | Statement::Analyze(_)
                | Statement::CreateRole(_)
                | Statement::AlterRole { .. }
                | Statement::Grant(_)
                | Statement::Revoke(_)
        );
        if admin {
            if let Some(u) = &self.user
                && !u.superuser
            {
                crate::audit::record("permission_denied", &u.name, &summary(stmt));
                return Err(Error::PermissionDenied(format!(
                    "to run {}: superuser required",
                    summary(stmt)
                )));
            }
            crate::audit::record("ddl", self.who(), &summary(stmt));
        }
        match stmt {
            Statement::StartTransaction { modes, .. } => {
                if self.txn.is_some() || self.failed {
                    return Ok(Output::Command("BEGIN".into())); // Postgres warns, and stays in the transaction
                }
                let iso = modes
                    .iter()
                    .find_map(|m| match m {
                        ast::TransactionMode::IsolationLevel(l) => Some(match l {
                            ast::TransactionIsolationLevel::Serializable => Isolation::Serializable,
                            _ => Isolation::Snapshot,
                        }),
                        ast::TransactionMode::AccessMode(_) => None,
                    })
                    .or(self.next_isolation)
                    .unwrap_or(self.isolation);
                self.txn = Some(self.store.begin(iso).await?);
                Ok(Output::Command("BEGIN".into()))
            }
            Statement::Commit { .. } => {
                self.next_isolation = None;
                if self.failed {
                    self.failed = false;
                    if let Some(t) = self.txn.take() {
                        t.rollback().await?;
                    }
                    return Ok(Output::Command("ROLLBACK".into()));
                }
                match self.txn.take() {
                    Some(t) => {
                        t.commit().await?;
                        Ok(Output::Command("COMMIT".into()))
                    }
                    None => Ok(Output::Command("COMMIT".into())),
                }
            }
            Statement::Rollback { .. } => {
                self.next_isolation = None;
                self.failed = false;
                if let Some(t) = self.txn.take() {
                    t.rollback().await?;
                }
                Ok(Output::Command("ROLLBACK".into()))
            }
            Statement::Set(set) => self.set(set),
            Statement::ShowVariable { variable } => {
                let name = variable
                    .iter()
                    .map(|i| i.value.to_lowercase())
                    .collect::<Vec<_>>()
                    .join(" ");
                let name = match name.as_str() {
                    "transaction isolation level" => "transaction_isolation".to_owned(),
                    n => n.to_owned(),
                };
                let v = self.vars.get(&name).cloned().ok_or_else(|| {
                    Error::UndefinedObject(format!(
                        "unrecognized configuration parameter \"{name}\""
                    ))
                })?;
                text_rows(&name, vec![v])
            }
            _ if self.failed => Err(Error::InFailedTransaction),
            Statement::CreateIndex(ci) if self.txn.is_none() => self.create_index_online(ci).await,
            _ => {
                if self.txn.is_some() {
                    let r = self.in_txn(stmt, params).await;
                    if r.is_err() {
                        // The transaction is dead: everything until ROLLBACK fails.
                        self.failed = true;
                        if let Some(t) = self.txn.take() {
                            let _ = t.rollback().await;
                        }
                    }
                    r
                } else {
                    let mut attempt = 0;
                    loop {
                        let iso = self.isolation;
                        let t0 = Instant::now();
                        self.txn = Some(self.store.begin(iso).await?);
                        self.timings.push(("begin", ms(t0)));
                        let r = self.in_txn(stmt, params).await;
                        let t = self.txn.take();
                        let t1 = Instant::now();
                        let r = match (r, t) {
                            (Ok(o), Some(t)) => {
                                let r = t.commit().await.map(|()| o);
                                self.timings.push(("commit", ms(t1)));
                                r
                            }
                            (Err(e), Some(t)) => {
                                let _ = t.rollback().await;
                                Err(e)
                            }
                            (r, None) => r,
                        };
                        match r {
                            Err(Error::Retry(_)) if attempt < AUTOCOMMIT_RETRIES => {
                                attempt += 1;
                                self.retries += 1;
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    attempt as u64 * 5,
                                ))
                                .await;
                            }
                            r => return r,
                        }
                    }
                }
            }
        }
    }

    /// `CREATE INDEX` outside a transaction block, online (F1 states):
    /// 1. one transaction adds the index **write-only**: writes maintain it, reads ignore it;
    /// 2. the existing rows are backfilled in small transactions;
    /// 3. one transaction makes it **public**.
    ///
    /// Writers are never blocked for longer than one batch.
    async fn create_index_online(&mut self, ci: &ast::CreateIndex) -> Result<Output> {
        let t0 = Instant::now();
        self.schema_log.clear();
        let batch: u32 = self
            .var("index_backfill_batch")
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);
        let Some((table, ix)) = own_txn!(self, b, dml::create_index_start(&mut b, ci))? else {
            return Ok(Output::Command("CREATE INDEX".into()));
        };
        self.schema_log
            .push((ms(t0), format!("index {ix} on {table} added write-only")));
        let mut cursor: Option<Vec<u8>> = None;
        let mut total = 0usize;
        let mut batches = 0usize;
        loop {
            let from = cursor.clone();
            let r = own_txn!(
                self,
                b,
                dml::backfill_batch(&mut b, &table, ix, from.clone(), batch)
            );
            let (next, n) = match r {
                Ok(x) => x,
                Err(e) => {
                    let _ = own_txn!(self, b, dml::create_index_abort(&mut b, &table, ix));
                    self.schema_log
                        .push((ms(t0), format!("build failed ({e}); index removed")));
                    return Err(e);
                }
            };
            total += n;
            batches += 1;
            self.schema_log.push((
                ms(t0),
                format!("backfill batch {batches}: {n} rows ({total} so far)"),
            ));
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        own_txn!(self, b, dml::create_index_finish(&mut b, &table, ix))?;
        self.schema_log.push((
            ms(t0),
            format!("index {ix} public after {total} rows in {batches} batches"),
        ));
        Ok(Output::Command("CREATE INDEX".into()))
    }

    fn set(&mut self, set: &ast::Set) -> Result<Output> {
        match set {
            ast::Set::SingleAssignment {
                variable, values, ..
            } => {
                let name = object_name(variable)?;
                let value = values
                    .iter()
                    .map(|v| match v {
                        ast::Expr::Value(v) => match &v.value {
                            ast::Value::SingleQuotedString(s) => s.clone(),
                            other => other.to_string(),
                        },
                        ast::Expr::Identifier(i) => i.value.clone(),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                if name == "transaction_isolation" || name == "default_transaction_isolation" {
                    self.isolation = if value.to_lowercase().contains("serializable") {
                        Isolation::Serializable
                    } else {
                        Isolation::Snapshot
                    };
                }
                self.vars.insert(name, value);
            }
            ast::Set::SetTransaction { modes, .. } => {
                for m in modes {
                    if let ast::TransactionMode::IsolationLevel(l) = m {
                        self.next_isolation = Some(
                            if matches!(l, ast::TransactionIsolationLevel::Serializable) {
                                Isolation::Serializable
                            } else {
                                Isolation::Snapshot
                            },
                        );
                    }
                }
            }
            _ => {} // SET NAMES, SET TIME ZONE, …: accepted and ignored
        }
        Ok(Output::Command("SET".into()))
    }

    async fn in_txn(&mut self, stmt: &Statement, params: &[Option<String>]) -> Result<Output> {
        let now = now_micros();
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| Error::Internal("no transaction".into()))?
            .as_mut();
        let mut b = Binder::new(txn, params.to_vec(), now);
        b.user = self.user.clone();
        match stmt {
            Statement::CreateRole(cr) => {
                crate::auth::create_role(b.txn, cr).await?;
                Ok(Output::Command("CREATE ROLE".into()))
            }
            Statement::AlterRole { name, operation } => {
                crate::auth::alter_role(b.txn, name, operation).await?;
                Ok(Output::Command("ALTER ROLE".into()))
            }
            Statement::Drop {
                object_type: ast::ObjectType::Role | ast::ObjectType::User,
                names,
                if_exists,
                ..
            } => {
                for n in names {
                    let name = object_name(n)?;
                    if crate::auth::get_user(b.txn, &name).await?.is_none() {
                        if *if_exists {
                            continue;
                        }
                        return Err(Error::UndefinedObject(format!("role \"{name}\"")));
                    }
                    crate::auth::drop_user(b.txn, &name).await?;
                }
                Ok(Output::Command("DROP ROLE".into()))
            }
            Statement::Grant(g) => {
                let d =
                    crate::auth::grant(b.txn, &g.privileges, g.objects.as_ref(), &g.grantees, true)
                        .await?;
                crate::audit::record("grant", self.user_name.as_deref().unwrap_or("-"), &d);
                Ok(Output::Command("GRANT".into()))
            }
            Statement::Revoke(r) => {
                let d = crate::auth::grant(
                    b.txn,
                    &r.privileges,
                    r.objects.as_ref(),
                    &r.grantees,
                    false,
                )
                .await?;
                crate::audit::record("revoke", self.user_name.as_deref().unwrap_or("-"), &d);
                Ok(Output::Command("REVOKE".into()))
            }
            Statement::Query(q) => {
                let t = Instant::now();
                let bound = b.bind_query(q).await?;
                self.timings.push(("bind", ms(t)));
                let t = Instant::now();
                let plan = optimizer::optimize(bound);
                self.timings.push(("optimize", ms(t)));
                let t = Instant::now();
                let batch = exec::execute(&plan, b.txn, now, None).await?;
                self.timings.push(("execute", ms(t)));
                Ok(rows_output(&plan, batch))
            }
            Statement::Explain {
                analyze,
                verbose,
                statement,
                ..
            } => {
                let Statement::Query(q) = statement.as_ref() else {
                    return unsupported("EXPLAIN of a non-query");
                };
                let bound = b.bind_query(q).await?;
                let mut lines = Vec::new();
                if *verbose {
                    // The plan as bound, before the optimizer touched it.
                    lines.push("Bound plan:".to_owned());
                    lines.extend(explain::render(&bound, None));
                    lines.push("Optimized plan:".to_owned());
                }
                let plan = optimizer::optimize(bound);
                if *analyze {
                    let mut stats: Vec<NodeStat> = Vec::new();
                    let t = Instant::now();
                    let batch = exec::execute(&plan, b.txn, now, Some(&mut stats)).await?;
                    lines.extend(explain::render(&plan, Some(&stats)));
                    lines.push(format!(
                        "Execution: {} rows in {:.3} ms",
                        batch.num_rows(),
                        t.elapsed().as_secs_f64() * 1e3
                    ));
                } else {
                    lines.extend(explain::render(&plan, None));
                }
                text_rows("QUERY PLAN", lines)
            }
            Statement::Insert(ins) => {
                let n = dml::insert(&mut b, ins, &mut self.rng).await?;
                Ok(Output::Command(format!("INSERT 0 {n}")))
            }
            Statement::Update(u) => Ok(Output::Command(format!(
                "UPDATE {}",
                dml::update(&mut b, u).await?
            ))),
            Statement::Delete(d) => Ok(Output::Command(format!(
                "DELETE {}",
                dml::delete(&mut b, d).await?
            ))),
            Statement::CreateTable(ct) => {
                dml::create_table(&mut b, ct).await?;
                Ok(Output::Command("CREATE TABLE".into()))
            }
            Statement::AlterTable(at) => {
                dml::alter_table(&mut b, at).await?;
                Ok(Output::Command("ALTER TABLE".into()))
            }
            Statement::CreateIndex(ci) => {
                // Inside a transaction block: every phase in this transaction.
                if let Some((table, ix)) = dml::create_index_start(&mut b, ci).await? {
                    let mut cursor = None;
                    loop {
                        let (next, _) =
                            dml::backfill_batch(&mut b, &table, ix, cursor, u32::MAX).await?;
                        if next.is_none() {
                            break;
                        }
                        cursor = next;
                    }
                    dml::create_index_finish(&mut b, &table, ix).await?;
                }
                Ok(Output::Command("CREATE INDEX".into()))
            }
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
                ..
            } => {
                dml::drop(&mut b, object_type, names, *if_exists, *cascade).await?;
                Ok(Output::Command(format!("DROP {object_type}")))
            }
            Statement::Analyze(a) => {
                dml::analyze(&mut b, a.table_name.as_ref()).await?;
                Ok(Output::Command("ANALYZE".into()))
            }
            other => unsupported(format!(
                "statement {}",
                other
                    .to_string()
                    .split_whitespace()
                    .take(3)
                    .collect::<Vec<_>>()
                    .join(" ")
            )),
        }
    }

    /// The columns a query would return, without running it (extended-protocol
    /// Describe). Binding reads the catalog, so this uses a short transaction.
    pub async fn describe(
        &mut self,
        stmt: &Statement,
        params: &[Option<String>],
    ) -> Result<Option<Vec<(String, DataType)>>> {
        let q = match stmt {
            Statement::Query(q) => q,
            Statement::Explain { .. } | Statement::ShowVariable { .. } => {
                return Ok(Some(vec![("QUERY PLAN".into(), DataType::Text)]));
            }
            _ => return Ok(None),
        };
        let now = now_micros();
        let own = self.txn.is_none();
        if own {
            self.txn = Some(self.store.begin(self.isolation).await?);
        }
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| Error::Internal("no transaction".into()))?
            .as_mut();
        // Unknown parameters bind as NULL text for typing purposes.
        let ps: Vec<Option<String>> = if params.is_empty() {
            vec![None; 64]
        } else {
            params.to_vec()
        };
        let r = Binder::new(txn, ps, now).bind_query(q).await;
        if own && let Some(t) = self.txn.take() {
            let _ = t.rollback().await;
        }
        let plan = r?;
        Ok(Some(
            plan.schema()
                .into_iter()
                .filter(|f| !f.hidden)
                .map(|f| (f.name, f.ty))
                .collect(),
        ))
    }
}

/// A statement's text for statistics, passwords masked.
fn summary_full(stmt: &Statement) -> String {
    let s = stmt.to_string();
    match s.to_ascii_uppercase().find("PASSWORD") {
        Some(i) => format!("{} PASSWORD ?", s[..i].trim_end()),
        None => s,
    }
}

/// The first words of a statement, for audit records and errors.
fn summary(stmt: &Statement) -> String {
    let s = stmt.to_string();
    // Never log a password.
    let s = match s.to_ascii_uppercase().find("PASSWORD") {
        Some(i) => format!("{} PASSWORD '***'", s[..i].trim_end()),
        None => s,
    };
    s.chars().take(200).collect()
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1e3
}

fn rows_output(plan: &Plan, batch: RecordBatch) -> Output {
    let schema = plan.schema();
    let keep: Vec<usize> = schema
        .iter()
        .enumerate()
        .filter(|(_, f)| !f.hidden)
        .map(|(i, _)| i)
        .collect();
    let columns = keep
        .iter()
        .map(|&i| (schema[i].name.clone(), schema[i].ty))
        .collect();
    let batch = if keep.len() == schema.len() {
        batch
    } else {
        batch.project(&keep).unwrap_or(batch)
    };
    Output::Rows { columns, batch }
}

fn text_rows(name: &str, lines: Vec<String>) -> Result<Output> {
    let n = lines.len();
    let col = build_array(DataType::Text, lines.into_iter().map(Value::Text))?;
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new(name, arrow::datatypes::DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![col],
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(n)),
    )?;
    Ok(Output::Rows {
        columns: vec![(name.to_owned(), DataType::Text)],
        batch,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn user_statements_become_role_statements() {
        assert_eq!(
            super::user_as_role("create user a with password 'héllo USER'"),
            "create ROLE a with password 'héllo USER'"
        );
        assert_eq!(
            super::user_as_role("ALTER USER b WITH PASSWORD 'x'; DROP USER IF EXISTS c"),
            "ALTER ROLE b WITH PASSWORD 'x'; DROP ROLE IF EXISTS c"
        );
        assert_eq!(
            super::user_as_role("SELECT current_user, 'CREATE USER x' FROM users"),
            "SELECT current_user, 'CREATE USER x' FROM users"
        );
    }
}
