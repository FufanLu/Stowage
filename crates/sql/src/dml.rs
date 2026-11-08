//! Statements that change data or schema: INSERT, UPDATE, DELETE, CREATE / DROP
//! TABLE and INDEX, ANALYZE.

use std::collections::HashSet;

use sqlparser::ast;
use stowage_common::Rng;

use crate::binder::{self, Binder, ident, object_name, unsupported};
use crate::catalog::{self, Check, Column, FkAction, ForeignKey, Index, Stats, Table};
use crate::exec;
use crate::keycodec::{self, PRIMARY};
use crate::kv::KvTxn;
use crate::plan::{Access, Expr, Plan};
use crate::value::{DataType, Value};
use crate::{Error, Result};

fn constraint_name(t: &Table, ix: Option<&Index>) -> String {
    match ix {
        Some(ix) => ix.name.clone(),
        None => format!("{}_pkey", t.name),
    }
}

/// Fill defaults, sequences and the hidden row id; check types and NOT NULL.
async fn complete_row(
    txn: &mut dyn KvTxn,
    t: &Table,
    given: Vec<Option<Value>>,
    rng: &mut dyn Rng,
    now: i64,
) -> Result<Vec<Value>> {
    let mut row = Vec::with_capacity(t.columns.len());
    for (c, v) in t.columns.iter().zip(given) {
        let v = match v {
            Some(v) => v,
            None if c.hidden => Value::Int((rng.next_u64() >> 1) as i64),
            None if c.serial => {
                let mut name = t.id.to_be_bytes().to_vec();
                name.extend_from_slice(c.name.as_bytes());
                Value::Int(catalog::next_seq(txn, &name).await?)
            }
            None if c.default_now => Value::Timestamp(now),
            None => c.default.clone().unwrap_or(Value::Null),
        };
        let v = v.coerce(c.ty)?;
        if v.is_null() && !c.nullable {
            return Err(Error::NotNullViolation(c.name.clone(), t.name.clone()));
        }
        row.push(v);
    }
    Ok(row)
}

/// Write a new row and its index entries. `Ok(false)`: a duplicate key, and
/// `skip_duplicates` (ON CONFLICT DO NOTHING) said to ignore it.
async fn insert_row(
    txn: &mut dyn KvTxn,
    t: &Table,
    row: &[Value],
    skip_duplicates: bool,
) -> Result<bool> {
    let key = t.row_key(row);
    if txn.get(&key).await?.is_some() {
        if skip_duplicates {
            return Ok(false);
        }
        return Err(Error::UniqueViolation(constraint_name(t, None)));
    }
    let mut entries = Vec::new();
    for ix in &t.indexes {
        let (k, unique) = t.index_key(ix, row);
        if unique && txn.get(&k).await?.is_some() {
            if skip_duplicates {
                return Ok(false);
            }
            return Err(Error::UniqueViolation(constraint_name(t, Some(ix))));
        }
        entries.push((k, if unique { key.clone() } else { Vec::new() }));
    }
    txn.put(&key, &keycodec::encode_row(row)).await?;
    for (k, v) in entries {
        txn.put(&k, &v).await?;
    }
    Ok(true)
}

pub(crate) async fn delete_row(txn: &mut dyn KvTxn, t: &Table, row: &[Value]) -> Result<()> {
    txn.delete(&t.row_key(row)).await?;
    for ix in &t.indexes {
        txn.delete(&t.index_key(ix, row).0).await?;
    }
    Ok(())
}

pub async fn insert(b: &mut Binder<'_>, ins: &ast::Insert, rng: &mut dyn Rng) -> Result<u64> {
    if ins.returning.is_some() {
        return unsupported("INSERT … RETURNING");
    }
    let name = match &ins.table {
        ast::TableObject::TableName(n) => object_name(n)?,
        other => return unsupported(format!("INSERT INTO {other}")),
    };
    let skip_duplicates = match &ins.on {
        None => false,
        Some(ast::OnInsert::OnConflict(c))
            if matches!(c.action, ast::OnConflictAction::DoNothing) =>
        {
            true
        }
        Some(other) => return unsupported(format!("{other}")),
    };
    b.require(&name, "INSERT")?;
    let t = catalog::table(b.txn, &name).await?;
    let targets: Vec<usize> = if ins.columns.is_empty() {
        t.visible_columns().map(|(i, _)| i).collect()
    } else {
        ins.columns
            .iter()
            .map(|c| {
                let n = object_name(c)?;
                t.column(&n)
                    .ok_or_else(|| Error::UndefinedColumn(format!("{name}.{n}")))
            })
            .collect::<Result<_>>()?
    };
    let Some(source) = &ins.source else {
        return unsupported("INSERT without VALUES or SELECT");
    };
    let types: Vec<DataType> = targets.iter().map(|&i| t.columns[i].ty).collect();
    let plan = match source.body.as_ref() {
        ast::SetExpr::Values(v) => {
            // DEFAULT in VALUES means "not given".
            b.bind_values(v, Some(&types)).await?
        }
        _ => crate::optimizer::optimize(b.bind_query(source).await?),
    };
    let width = plan.schema().iter().filter(|f| !f.hidden).count();
    if width != targets.len() {
        return Err(Error::InvalidInput(format!(
            "INSERT has {width} expressions but {} target columns",
            targets.len()
        )));
    }
    let batch = exec::execute(&plan, b.txn, b.now, None).await?;
    let mut n = 0;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        let mut given: Vec<Option<Value>> = vec![None; t.columns.len()];
        for (k, &col) in targets.iter().enumerate() {
            let v = Value::from_array(batch.column(k).as_ref(), r)?;
            // An explicit NULL for a SERIAL or defaulted column is still NULL.
            given[col] = Some(v);
        }
        rows.push(complete_row(b.txn, &t, given, rng, b.now).await?);
    }
    crate::constraints::check_rows(b, &t, &rows).await?;
    crate::constraints::check_parents(b.txn, &t, &rows).await?;
    for row in &rows {
        if insert_row(b.txn, &t, row, skip_duplicates).await? {
            n += 1;
        }
    }
    Ok(n)
}

/// The rows of `table` matching `selection`, read through the optimizer (so a
/// key or index predicate becomes a narrow scan). Returns (scan plan, rows).
async fn matching_rows(
    b: &mut Binder<'_>,
    table: &str,
    alias: Option<String>,
    selection: Option<&ast::Expr>,
) -> Result<(Plan, Vec<Vec<Value>>)> {
    let mut scan = b.scan(table, alias).await?;
    let scope = scan.schema();
    if let Some(w) = selection {
        let pred = b.bind_bool(w, &scope).await?;
        scan = Plan::Filter {
            input: Box::new(scan),
            pred,
        };
    }
    let plan = crate::optimizer::optimize(scan);
    let batch = exec::execute(&plan, b.txn, b.now, None).await?;
    let rows = (0..batch.num_rows())
        .map(|r| {
            (0..batch.num_columns())
                .map(|c| Value::from_array(batch.column(c).as_ref(), r))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((plan, rows))
}

fn target_table(twj: &ast::TableWithJoins) -> Result<(String, Option<String>)> {
    if !twj.joins.is_empty() {
        return unsupported("UPDATE/DELETE with joins");
    }
    match &twj.relation {
        ast::TableFactor::Table { name, alias, .. } => {
            Ok((object_name(name)?, alias.as_ref().map(|a| ident(&a.name))))
        }
        other => unsupported(format!("target {other}")),
    }
}

pub async fn update(b: &mut Binder<'_>, u: &ast::Update) -> Result<u64> {
    if u.from.is_some() || u.returning.is_some() {
        return unsupported("UPDATE … FROM / RETURNING");
    }
    let (name, alias) = target_table(&u.table)?;
    b.require(&name, "UPDATE")?;
    let (plan, rows) = matching_rows(b, &name, alias, u.selection.as_ref()).await?;
    let Plan::Scan { table: t, .. } = find_scan(&plan) else {
        return Err(Error::Internal("update without scan".into()));
    };
    let t = t.clone();
    let scope = plan.schema();
    // New values, evaluated over the old rows (vectorized).
    let mut sets: Vec<(usize, Expr)> = Vec::new();
    for a in &u.assignments {
        let col = match &a.target {
            ast::AssignmentTarget::ColumnName(n) => object_name(n)?,
            ast::AssignmentTarget::Tuple(_) => return unsupported("UPDATE (a, b) = …"),
        };
        let i = t
            .column(&col)
            .ok_or_else(|| Error::UndefinedColumn(format!("{name}.{col}")))?;
        let e = b.bind_expr(&a.value, &scope, &mut None).await?;
        sets.push((i, e));
    }
    if rows.is_empty() {
        return Ok(0);
    }
    let fields = scope.clone();
    let cols = t
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| crate::value::build_array(c.ty, rows.iter().map(|r| r[i].clone())))
        .collect::<Result<Vec<_>>>()?;
    let batch = arrow::array::RecordBatch::try_new(
        std::sync::Arc::new(arrow::datatypes::Schema::new(
            fields
                .iter()
                .map(|f| arrow::datatypes::Field::new(format!("c{}", f.id.0), f.ty.arrow(), true))
                .collect::<Vec<_>>(),
        )),
        cols,
    )?;
    let mut new_values = Vec::new();
    for (i, e) in &sets {
        new_values.push((*i, exec::eval(e, &batch, &fields, b.now)?));
    }
    let mut olds = Vec::new();
    let mut news = Vec::new();
    for (r, old) in rows.iter().enumerate() {
        let mut new = old.clone();
        for (i, arr) in &new_values {
            let v = Value::from_array(arr.as_ref(), r)?.coerce(t.columns[*i].ty)?;
            if v.is_null() && !t.columns[*i].nullable {
                return Err(Error::NotNullViolation(
                    t.columns[*i].name.clone(),
                    t.name.clone(),
                ));
            }
            new[*i] = v;
        }
        if new == *old {
            continue;
        }
        olds.push(old.clone());
        news.push(new);
    }
    crate::constraints::check_rows(b, &t, &news).await?;
    crate::constraints::check_parents(b.txn, &t, &news).await?;
    crate::constraints::before_parent_change(b, &t, &olds, Some(&news)).await?;
    for (old, new) in olds.iter().zip(&news) {
        replace_row(b.txn, &t, old, new).await?;
    }
    Ok(rows.len() as u64)
}

/// Overwrite `old` with `new`, moving its key and index entries if they change.
pub(crate) async fn replace_row(
    txn: &mut dyn KvTxn,
    t: &Table,
    old: &[Value],
    new: &[Value],
) -> Result<()> {
    let (old_key, new_key) = (t.row_key(old), t.row_key(new));
    if old_key != new_key {
        if txn.get(&new_key).await?.is_some() {
            return Err(Error::UniqueViolation(constraint_name(t, None)));
        }
        txn.delete(&old_key).await?;
    }
    for ix in &t.indexes {
        let (ok, _) = t.index_key(ix, old);
        let (nk, unique) = t.index_key(ix, new);
        if ok != nk || old_key != new_key {
            txn.delete(&ok).await?;
            if unique && txn.get(&nk).await?.is_some() {
                return Err(Error::UniqueViolation(constraint_name(t, Some(ix))));
            }
            txn.put(&nk, if unique { &new_key } else { &[] }).await?;
        }
    }
    txn.put(&new_key, &keycodec::encode_row(new)).await
}

fn find_scan(p: &Plan) -> &Plan {
    match p {
        Plan::Scan { .. } => p,
        p => p.children().first().map_or(p, |c| find_scan(c)),
    }
}

pub async fn delete(b: &mut Binder<'_>, d: &ast::Delete) -> Result<u64> {
    if d.returning.is_some() || d.using.is_some() {
        return unsupported("DELETE … USING / RETURNING");
    }
    let from = match &d.from {
        ast::FromTable::WithFromKeyword(v) | ast::FromTable::WithoutKeyword(v) => v,
    };
    let [twj] = from.as_slice() else {
        return unsupported("DELETE from several tables");
    };
    let (name, alias) = target_table(twj)?;
    b.require(&name, "DELETE")?;
    let (plan, rows) = matching_rows(b, &name, alias, d.selection.as_ref()).await?;
    let Plan::Scan { table: t, .. } = find_scan(&plan) else {
        return Err(Error::Internal("delete without scan".into()));
    };
    let t = t.clone();
    crate::constraints::before_parent_change(b, &t, &rows, None).await?;
    for row in &rows {
        delete_row(b.txn, &t, row).await?;
    }
    Ok(rows.len() as u64)
}

// ---------------------------------------------------------------- DDL

pub async fn create_table(b: &mut Binder<'_>, ct: &ast::CreateTable) -> Result<()> {
    let name = object_name(&ct.name)?;
    if catalog::get_table(b.txn, &name).await?.is_some() {
        if ct.if_not_exists {
            return Ok(());
        }
        return Err(Error::DuplicateTable(name));
    }
    if ct.query.is_some() {
        return unsupported("CREATE TABLE … AS");
    }
    let mut columns = Vec::new();
    let mut pk: Vec<usize> = Vec::new();
    let mut uniques: Vec<(Option<String>, Vec<usize>)> = Vec::new();
    let mut checks: Vec<Check> = Vec::new();
    let mut pending_fks: Vec<(Vec<usize>, ast::ForeignKeyConstraint)> = Vec::new();
    for (i, c) in ct.columns.iter().enumerate() {
        let serial = matches!(&c.data_type, ast::DataType::Custom(n, _) if object_name(n).is_ok_and(|n| n.contains("serial")));
        let mut col = Column {
            name: ident(&c.name),
            ty: binder::data_type(&c.data_type)?,
            nullable: true,
            default: None,
            serial,
            hidden: false,
            default_now: false,
        };
        for o in &c.options {
            match &o.option {
                ast::ColumnOption::NotNull => col.nullable = false,
                ast::ColumnOption::Null => col.nullable = true,
                ast::ColumnOption::Default(e) if is_now(e) => col.default_now = true,
                ast::ColumnOption::Default(e) => {
                    let v = b.bind_expr(e, &[], &mut None).await?;
                    match v {
                        Expr::Literal(v, _) => col.default = Some(v.coerce(col.ty)?),
                        // nextval(...) and other non-constant defaults: a sequence.
                        Expr::Func(..) => col.serial = true,
                        _ => return unsupported(format!("DEFAULT {e}")),
                    }
                }
                ast::ColumnOption::PrimaryKey(_) => {
                    pk = vec![i];
                    col.nullable = false;
                }
                ast::ColumnOption::Unique(_) => uniques.push((None, vec![i])),
                ast::ColumnOption::Check(ck) => checks.push(Check {
                    name: ck
                        .name
                        .as_ref()
                        .map_or_else(|| format!("{name}_{}_check", ident(&c.name)), ident),
                    sql: ck.expr.to_string(),
                }),
                ast::ColumnOption::ForeignKey(fk) => pending_fks.push((vec![i], fk.clone())),
                ast::ColumnOption::Comment(_) => {}
                other => return unsupported(format!("column option {other}")),
            }
        }
        if col.serial {
            col.nullable = false;
        }
        columns.push(col);
    }
    let col_index = |n: &str, cols: &[Column]| {
        cols.iter()
            .position(|c| c.name == n)
            .ok_or_else(|| Error::UndefinedColumn(n.to_owned()))
    };
    let index_cols = |list: &[ast::IndexColumn], cols: &[Column]| -> Result<Vec<usize>> {
        list.iter()
            .map(|ic| match &ic.column.expr {
                ast::Expr::Identifier(i) => col_index(&ident(i), cols),
                other => unsupported(format!("index on {other}")),
            })
            .collect()
    };
    for tc in &ct.constraints {
        match tc {
            ast::TableConstraint::PrimaryKey(p) => {
                pk = index_cols(&p.columns, &columns)?;
                for &c in &pk {
                    columns[c].nullable = false;
                }
            }
            ast::TableConstraint::Unique(u) => uniques.push((
                u.name.as_ref().map(ident),
                index_cols(&u.columns, &columns)?,
            )),
            ast::TableConstraint::Check(ck) => checks.push(Check {
                name: ck
                    .name
                    .as_ref()
                    .map_or_else(|| format!("{name}_check{}", checks.len() + 1), ident),
                sql: ck.expr.to_string(),
            }),
            ast::TableConstraint::ForeignKey(fk) => {
                let cols = fk
                    .columns
                    .iter()
                    .map(|c| col_index(&ident(c), &columns))
                    .collect::<Result<Vec<_>>>()?;
                pending_fks.push((cols, fk.clone()));
            }
            other => return unsupported(format!("constraint {other}")),
        }
    }
    if pk.is_empty() {
        columns.push(Column {
            name: "rowid".into(),
            ty: DataType::Int,
            nullable: false,
            default: None,
            serial: false,
            hidden: true,
            default_now: false,
        });
        pk = vec![columns.len() - 1];
    }
    let id = u32::try_from(catalog::next_seq(b.txn, b"tables").await?)
        .map_err(|_| Error::Internal("table ids exhausted".into()))?;
    let mut t = Table {
        id,
        name: name.clone(),
        columns,
        pk,
        indexes: Vec::new(),
        next_index_id: PRIMARY + 1,
        stats: None,
        checks: Vec::new(),
        fks: Vec::new(),
    };
    for (n, cols) in uniques {
        let ix_name = n.unwrap_or_else(|| {
            format!(
                "{name}_{}_key",
                cols.iter()
                    .map(|&c| t.columns[c].name.as_str())
                    .collect::<Vec<_>>()
                    .join("_")
            )
        });
        t.indexes.push(Index {
            id: t.next_index_id,
            name: ix_name,
            columns: cols,
            unique: true,
            public: true,
        });
        t.next_index_id += 1;
    }
    for c in checks {
        crate::constraints::validate_check(b, &t, &c.sql).await?;
        t.checks.push(c);
    }
    for (cols, fk) in pending_fks {
        let f = resolve_fk(b.txn, &t, cols, &fk).await?;
        t.fks.push(f);
    }
    catalog::put_table(b.txn, &t).await
}

/// `DEFAULT now()` and its spellings: evaluated per insert, not at CREATE.
fn is_now(e: &ast::Expr) -> bool {
    match e {
        ast::Expr::Function(f) => matches!(
            object_name(&f.name).as_deref(),
            Ok("now"
                | "current_timestamp"
                | "localtimestamp"
                | "transaction_timestamp"
                | "statement_timestamp")
        ),
        ast::Expr::Identifier(i) => {
            matches!(ident(i).as_str(), "current_timestamp" | "localtimestamp")
        }
        _ => false,
    }
}

/// Turn `FOREIGN KEY (cols) REFERENCES parent [(cols)]` into a key on the
/// parent's primary key or a unique index, as Postgres requires.
async fn resolve_fk(
    txn: &mut dyn KvTxn,
    t: &Table,
    cols: Vec<usize>,
    fk: &ast::ForeignKeyConstraint,
) -> Result<ForeignKey> {
    let parent_name = object_name(&fk.foreign_table)?;
    let parent = if parent_name == t.name {
        t.clone()
    } else {
        catalog::table(txn, &parent_name).await?
    };
    let parent_cols: Vec<usize> = if fk.referred_columns.is_empty() {
        parent.pk.clone()
    } else {
        fk.referred_columns
            .iter()
            .map(|c| {
                parent
                    .column(&ident(c))
                    .ok_or_else(|| Error::UndefinedColumn(format!("{parent_name}.{}", ident(c))))
            })
            .collect::<Result<_>>()?
    };
    let same = |a: &[usize], b: &[usize]| a.len() == b.len() && a.iter().all(|x| b.contains(x));
    let keyed = same(&parent_cols, &parent.pk)
        || parent
            .indexes
            .iter()
            .any(|ix| ix.unique && same(&parent_cols, &ix.columns));
    if !keyed || parent.columns[parent.pk[0]].hidden && same(&parent_cols, &parent.pk) {
        return Err(Error::InvalidInput(format!(
            "there is no unique constraint matching given keys for referenced table \"{parent_name}\""
        )));
    }
    if cols.len() != parent_cols.len() {
        return Err(Error::InvalidInput(
            "number of referencing and referenced columns for foreign key disagree".into(),
        ));
    }
    let on_delete = match fk.on_delete {
        None | Some(ast::ReferentialAction::NoAction | ast::ReferentialAction::Restrict) => {
            FkAction::Restrict
        }
        Some(ast::ReferentialAction::Cascade) => FkAction::Cascade,
        Some(ast::ReferentialAction::SetNull) => FkAction::SetNull,
        Some(other) => return unsupported(format!("ON DELETE {other}")),
    };
    if let Some(a) = &fk.on_update
        && !matches!(
            a,
            ast::ReferentialAction::NoAction | ast::ReferentialAction::Restrict
        )
    {
        return unsupported(format!("ON UPDATE {a}"));
    }
    let name = fk.name.as_ref().map_or_else(
        || {
            format!(
                "{}_{}_fkey",
                t.name,
                cols.iter()
                    .map(|&c| t.columns[c].name.as_str())
                    .collect::<Vec<_>>()
                    .join("_")
            )
        },
        ident,
    );
    Ok(ForeignKey {
        name,
        cols,
        parent: parent_name,
        parent_cols,
        on_delete,
    })
}

/// Phase 1 of `CREATE INDEX`: add the index as write-only. From the moment this
/// commits, every write maintains it; no read uses it yet. Returns the table and
/// index id, or `None` if it exists and `IF NOT EXISTS` was given.
pub async fn create_index_start(
    b: &mut Binder<'_>,
    ci: &ast::CreateIndex,
) -> Result<Option<(String, u32)>> {
    let table = object_name(&ci.table_name)?;
    // A partial index (`WHERE …`) is built over every row: a superset that
    // answers the same lookups. A partial *unique* index would enforce
    // uniqueness only within the predicate, which a full index cannot express.
    if ci.predicate.is_some() && ci.unique {
        return unsupported("partial unique index (CREATE UNIQUE INDEX … WHERE)");
    }
    let mut t = catalog::table(b.txn, &table).await?;
    let cols: Vec<usize> = ci
        .columns
        .iter()
        .map(|ic| match &ic.column.expr {
            ast::Expr::Identifier(i) => t
                .column(&ident(i))
                .ok_or_else(|| Error::UndefinedColumn(format!("{table}.{}", ident(i)))),
            other => unsupported(format!("index on {other}")),
        })
        .collect::<Result<_>>()?;
    let name = match &ci.name {
        Some(n) => object_name(n)?,
        None => format!(
            "{table}_{}_idx",
            cols.iter()
                .map(|&c| t.columns[c].name.as_str())
                .collect::<Vec<_>>()
                .join("_")
        ),
    };
    if t.indexes.iter().any(|i| i.name == name) {
        if ci.if_not_exists {
            return Ok(None);
        }
        return Err(Error::DuplicateObject(name));
    }
    let id = t.next_index_id;
    t.indexes.push(Index {
        id,
        name,
        columns: cols,
        unique: ci.unique,
        public: false,
    });
    t.next_index_id += 1;
    catalog::put_table(b.txn, &t).await?;
    Ok(Some((table, id)))
}

/// Phase 2: index up to `batch` rows from `cursor` (a primary key, or the
/// table's start). Returns where to continue, or `None` when done. Each batch is
/// its own transaction: rows it read that a concurrent writer changes make it
/// fail its refresh and run again, so it never writes a stale entry.
pub async fn backfill_batch(
    b: &mut Binder<'_>,
    table: &str,
    index: u32,
    cursor: Option<Vec<u8>>,
    batch: u32,
) -> Result<(Option<Vec<u8>>, usize)> {
    let t = catalog::table(b.txn, table).await?;
    let ix = t
        .indexes
        .iter()
        .find(|i| i.id == index)
        .ok_or_else(|| Error::UndefinedObject(format!("index {index}")))?
        .clone();
    let (start, end) = exec::span(&t, &Access::full());
    let from = cursor.unwrap_or(start);
    let pairs = b.txn.scan(&from, &end, batch).await?;
    let n = pairs.len();
    for (key, v) in &pairs {
        let row = keycodec::decode_row(v, t.columns.len())?;
        let (k, unique) = t.index_key(&ix, &row);
        if unique
            && let Some(existing) = b.txn.get(&k).await?
            && existing != *key
        {
            return Err(Error::UniqueViolation(ix.name.clone()));
        }
        b.txn.put(&k, if unique { key } else { &[] }).await?;
    }
    Ok(match pairs.last() {
        Some((k, _)) if n == batch as usize => {
            let mut next = k.clone();
            next.push(0);
            (Some(next), n)
        }
        _ => (None, n),
    })
}

/// Phase 3: make the index readable.
pub async fn create_index_finish(b: &mut Binder<'_>, table: &str, index: u32) -> Result<()> {
    let mut t = catalog::table(b.txn, table).await?;
    if let Some(ix) = t.indexes.iter_mut().find(|i| i.id == index) {
        ix.public = true;
    }
    catalog::put_table(b.txn, &t).await
}

/// A failed build: remove the index and whatever entries it has.
pub async fn create_index_abort(b: &mut Binder<'_>, table: &str, index: u32) -> Result<()> {
    let mut t = catalog::table(b.txn, table).await?;
    t.indexes.retain(|i| i.id != index);
    let start = keycodec::index_prefix(t.id, index);
    let end = keycodec::prefix_end(&start);
    for (k, _) in b.txn.scan(&start, &end, u32::MAX).await? {
        b.txn.delete(&k).await?;
    }
    catalog::put_table(b.txn, &t).await
}

/// Compare an index with its table: (rows, entries, problems). Every row must
/// have exactly the entry its values imply, and no entry may lack a row.
pub async fn check_index(
    txn: &mut dyn KvTxn,
    table: &str,
    index_name: &str,
) -> Result<(usize, usize, Vec<String>)> {
    let t = catalog::table(txn, table).await?;
    let ix = t
        .indexes
        .iter()
        .find(|i| i.name == index_name)
        .ok_or_else(|| Error::UndefinedObject(index_name.to_owned()))?
        .clone();
    let (start, end) = exec::span(&t, &Access::full());
    let rows = txn.scan(&start, &end, u32::MAX).await?;
    let mut want = std::collections::BTreeMap::new();
    for (key, v) in &rows {
        let row = keycodec::decode_row(v, t.columns.len())?;
        let (k, unique) = t.index_key(&ix, &row);
        want.insert(k, if unique { key.clone() } else { Vec::new() });
    }
    let start = keycodec::index_prefix(t.id, ix.id);
    let end = keycodec::prefix_end(&start);
    let have: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = txn
        .scan(&start, &end, u32::MAX)
        .await?
        .into_iter()
        .collect();
    let mut problems = Vec::new();
    for (k, v) in &want {
        match have.get(k) {
            None => problems.push(format!("missing entry {k:02x?}")),
            Some(h) if h != v => problems.push(format!("entry {k:02x?} points elsewhere")),
            Some(_) => {}
        }
    }
    for k in have.keys() {
        if !want.contains_key(k) {
            problems.push(format!("stray entry {k:02x?}"));
        }
    }
    Ok((rows.len(), have.len(), problems))
}

pub async fn drop(
    b: &mut Binder<'_>,
    object: &ast::ObjectType,
    names: &[ast::ObjectName],
    if_exists: bool,
    cascade: bool,
) -> Result<()> {
    let dropping: Vec<String> = names.iter().filter_map(|n| object_name(n).ok()).collect();
    for n in names {
        let name = object_name(n)?;
        match object {
            ast::ObjectType::Table => {
                let Some(t) = catalog::get_table(b.txn, &name).await? else {
                    if if_exists {
                        continue;
                    }
                    return Err(Error::UndefinedTable(name));
                };
                // Foreign keys in other tables that reference this one: refused,
                // or with CASCADE removed (the referencing rows stay), as in Postgres.
                for mut child in catalog::list_tables(b.txn).await? {
                    if child.name == name || dropping.contains(&child.name) {
                        continue;
                    }
                    let Some(fk) = child.fks.iter().find(|f| f.parent == name) else {
                        continue;
                    };
                    if !cascade {
                        return Err(Error::DependentObjects(format!(
                            "cannot drop table {name} because constraint {} on table {} depends on it",
                            fk.name, child.name
                        )));
                    }
                    child.fks.retain(|f| f.parent != name);
                    catalog::put_table(b.txn, &child).await?;
                }
                let start = keycodec::table_prefix(t.id);
                let end = keycodec::prefix_end(&start);
                for (k, _) in b.txn.scan(&start, &end, u32::MAX).await? {
                    b.txn.delete(&k).await?;
                }
                catalog::drop_table(b.txn, &name).await?;
            }
            ast::ObjectType::Index => {
                let mut found = None;
                for t in catalog::list_tables(b.txn).await? {
                    if let Some(i) = t.indexes.iter().position(|ix| ix.name == name) {
                        found = Some((t, i));
                        break;
                    }
                }
                let Some((mut t, i)) = found else {
                    if if_exists {
                        continue;
                    }
                    return Err(Error::UndefinedObject(name));
                };
                let ix = t.indexes.remove(i);
                let start = keycodec::index_prefix(t.id, ix.id);
                let end = keycodec::prefix_end(&start);
                for (k, _) in b.txn.scan(&start, &end, u32::MAX).await? {
                    b.txn.delete(&k).await?;
                }
                catalog::put_table(b.txn, &t).await?;
            }
            other => return unsupported(format!("DROP {other}")),
        }
    }
    Ok(())
}

/// Collect row counts, distinct counts, NULL counts and equi-depth histograms.
pub async fn analyze(b: &mut Binder<'_>, table: Option<&ast::ObjectName>) -> Result<()> {
    let tables = match table {
        Some(n) => vec![catalog::table(b.txn, &object_name(n)?).await?],
        None => catalog::list_tables(b.txn).await?,
    };
    for mut t in tables {
        let (start, end) = exec::span(&t, &Access::full());
        let pairs = b.txn.scan(&start, &end, u32::MAX).await?;
        let n = t.columns.len();
        let rows: Vec<Vec<Value>> = pairs
            .iter()
            .map(|(_, v)| keycodec::decode_row(v, n))
            .collect::<Result<_>>()?;
        let mut st = Stats {
            rows: rows.len() as u64,
            ..Stats::default()
        };
        for c in 0..n {
            let mut vals: Vec<Value> = rows
                .iter()
                .map(|r| r[c].clone())
                .filter(|v| !v.is_null())
                .collect();
            st.nulls.push((rows.len() - vals.len()) as u64);
            let distinct: HashSet<Vec<u8>> = vals
                .iter()
                .map(|v| {
                    let mut k = Vec::new();
                    keycodec::encode_key(v, &mut k);
                    k
                })
                .collect();
            st.distinct.push(distinct.len() as u64);
            vals.sort_by(Value::sql_cmp);
            let buckets = 16.min(vals.len());
            let bounds = if buckets == 0 {
                Vec::new()
            } else {
                (0..=buckets)
                    .map(|i| vals[(i * (vals.len() - 1)) / buckets].clone())
                    .collect()
            };
            st.bounds.push(bounds);
        }
        t.stats = Some(st);
        catalog::put_table(b.txn, &t).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------- ALTER TABLE

/// Every row of `t`, through its primary index.
async fn all_rows(txn: &mut dyn KvTxn, t: &Table) -> Result<Vec<Vec<Value>>> {
    let start = keycodec::index_prefix(t.id, PRIMARY);
    let end = keycodec::prefix_end(&start);
    txn.scan(&start, &end, u32::MAX)
        .await?
        .into_iter()
        .map(|(_, v)| keycodec::decode_row(&v, t.columns.len()))
        .collect()
}

/// `ALTER TABLE`. Runs in the statement's transaction: changes that touch rows
/// (a new column, a dropped one, a new constraint to validate) read or rewrite
/// every row of the table in that transaction.
pub async fn alter_table(b: &mut Binder<'_>, at: &ast::AlterTable) -> Result<()> {
    let name = object_name(&at.name)?;
    let Some(mut t) = catalog::get_table(b.txn, &name).await? else {
        if at.if_exists {
            return Ok(());
        }
        return Err(Error::UndefinedTable(name));
    };
    for op in &at.operations {
        alter_one(b, &mut t, op).await?;
    }
    catalog::put_table(b.txn, &t).await
}

fn referenced(t: &Table, c: usize) -> Option<&'static str> {
    if t.pk.contains(&c) {
        Some("the primary key")
    } else if t.indexes.iter().any(|ix| ix.columns.contains(&c)) {
        Some("an index")
    } else if t.fks.iter().any(|f| f.cols.contains(&c)) {
        Some("a foreign key")
    } else {
        None
    }
}

async fn alter_one(b: &mut Binder<'_>, t: &mut Table, op: &ast::AlterTableOperation) -> Result<()> {
    use ast::AlterTableOperation as Op;
    match op {
        Op::AddConstraint {
            constraint,
            not_valid,
        } => match constraint {
            ast::TableConstraint::PrimaryKey(p) => {
                let hidden = t.pk.len() == 1 && t.columns[t.pk[0]].hidden;
                if !hidden {
                    return Err(Error::InvalidInput(format!(
                        "multiple primary keys for table \"{}\" are not allowed",
                        t.name
                    )));
                }
                if !all_rows(b.txn, t).await?.is_empty() {
                    return unsupported("ADD PRIMARY KEY on a table that has rows");
                }
                let cols = p
                    .columns
                    .iter()
                    .map(|ic| match &ic.column.expr {
                        ast::Expr::Identifier(i) => t
                            .column(&ident(i))
                            .ok_or_else(|| Error::UndefinedColumn(ident(i))),
                        other => unsupported(format!("primary key on {other}")),
                    })
                    .collect::<Result<Vec<_>>>()?;
                // The hidden rowid is always the last column.
                let rowid = t.pk[0];
                t.columns.remove(rowid);
                for &c in &cols {
                    t.columns[c].nullable = false;
                }
                t.pk = cols;
                t.stats = None;
            }
            ast::TableConstraint::Unique(u) => {
                let cols = u
                    .columns
                    .iter()
                    .map(|ic| match &ic.column.expr {
                        ast::Expr::Identifier(i) => t
                            .column(&ident(i))
                            .ok_or_else(|| Error::UndefinedColumn(ident(i))),
                        other => unsupported(format!("unique on {other}")),
                    })
                    .collect::<Result<Vec<_>>>()?;
                let ix = Index {
                    id: t.next_index_id,
                    name: u.name.as_ref().map_or_else(
                        || {
                            format!(
                                "{}_{}_key",
                                t.name,
                                cols.iter()
                                    .map(|&c| t.columns[c].name.as_str())
                                    .collect::<Vec<_>>()
                                    .join("_")
                            )
                        },
                        ident,
                    ),
                    columns: cols,
                    unique: true,
                    public: true,
                };
                for row in all_rows(b.txn, t).await? {
                    let (k, unique) = t.index_key(&ix, &row);
                    if unique && b.txn.get(&k).await?.is_some() {
                        return Err(Error::UniqueViolation(ix.name.clone()));
                    }
                    let v = if unique { t.row_key(&row) } else { Vec::new() };
                    b.txn.put(&k, &v).await?;
                }
                t.next_index_id += 1;
                t.indexes.push(ix);
            }
            ast::TableConstraint::Check(ck) => {
                let c = Check {
                    name: ck
                        .name
                        .as_ref()
                        .map_or_else(|| format!("{}_check{}", t.name, t.checks.len() + 1), ident),
                    sql: ck.expr.to_string(),
                };
                crate::constraints::validate_check(b, t, &c.sql).await?;
                t.checks.push(c);
                if !not_valid {
                    let rows = all_rows(b.txn, t).await?;
                    crate::constraints::check_rows(b, t, &rows).await?;
                }
            }
            ast::TableConstraint::ForeignKey(fk) => {
                let cols = fk
                    .columns
                    .iter()
                    .map(|c| {
                        t.column(&ident(c))
                            .ok_or_else(|| Error::UndefinedColumn(ident(c)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let f = resolve_fk(b.txn, t, cols, fk).await?;
                t.fks.push(f);
                if !not_valid {
                    let rows = all_rows(b.txn, t).await?;
                    crate::constraints::check_parents(b.txn, t, &rows).await?;
                }
            }
            other => return unsupported(format!("ADD {other}")),
        },
        Op::DropConstraint {
            if_exists, name, ..
        } => {
            let n = ident(name);
            let before = t.checks.len() + t.fks.len();
            t.checks.retain(|c| c.name != n);
            t.fks.retain(|f| f.name != n);
            if t.checks.len() + t.fks.len() == before && !if_exists {
                return Err(Error::UndefinedObject(format!(
                    "constraint \"{n}\" of relation \"{}\"",
                    t.name
                )));
            }
        }
        Op::AddColumn {
            if_not_exists,
            column_def: c,
            ..
        } => {
            let col_name = ident(&c.name);
            if t.column(&col_name).is_some() {
                if *if_not_exists {
                    return Ok(());
                }
                return Err(Error::DuplicateObject(format!("column \"{col_name}\"")));
            }
            let mut col = Column {
                name: col_name.clone(),
                ty: binder::data_type(&c.data_type)?,
                nullable: true,
                default: None,
                serial: false,
                hidden: false,
                default_now: false,
            };
            let mut checks = Vec::new();
            let mut fk = None;
            for o in &c.options {
                match &o.option {
                    ast::ColumnOption::NotNull => col.nullable = false,
                    ast::ColumnOption::Null => col.nullable = true,
                    ast::ColumnOption::Default(e) if is_now(e) => col.default_now = true,
                    ast::ColumnOption::Default(e) => match b.bind_expr(e, &[], &mut None).await? {
                        Expr::Literal(v, _) => col.default = Some(v.coerce(col.ty)?),
                        _ => return unsupported(format!("DEFAULT {e}")),
                    },
                    ast::ColumnOption::Check(ck) => checks.push(Check {
                        name: ck
                            .name
                            .as_ref()
                            .map_or_else(|| format!("{}_{col_name}_check", t.name), ident),
                        sql: ck.expr.to_string(),
                    }),
                    ast::ColumnOption::ForeignKey(f) => fk = Some(f.clone()),
                    ast::ColumnOption::Comment(_) => {}
                    other => return unsupported(format!("ADD COLUMN … {other}")),
                }
            }
            // Insert before the hidden rowid, which stays last.
            let at = if t.columns.last().is_some_and(|c| c.hidden) {
                t.columns.len() - 1
            } else {
                t.columns.len()
            };
            let rows = all_rows(b.txn, t).await?;
            let fill = if col.default_now {
                Value::Timestamp(b.now)
            } else {
                col.default.clone().unwrap_or(Value::Null)
            };
            if fill.is_null() && !col.nullable && !rows.is_empty() {
                return Err(Error::NotNullViolation(col_name, t.name.clone()));
            }
            let shift = |p: &mut usize| {
                if *p >= at {
                    *p += 1;
                }
            };
            t.pk.iter_mut().for_each(shift);
            for ix in &mut t.indexes {
                ix.columns.iter_mut().for_each(shift);
            }
            for f in &mut t.fks {
                f.cols.iter_mut().for_each(shift);
            }
            t.columns.insert(at, col);
            t.stats = None;
            for mut row in rows {
                row.insert(at, fill.clone());
                b.txn
                    .put(&t.row_key(&row), &keycodec::encode_row(&row))
                    .await?;
            }
            for c in checks {
                crate::constraints::validate_check(b, t, &c.sql).await?;
                t.checks.push(c);
            }
            if let Some(f) = fk {
                let f = resolve_fk(b.txn, t, vec![at], &f).await?;
                t.fks.push(f);
            }
            let rows = all_rows(b.txn, t).await.unwrap_or_default();
            crate::constraints::check_rows(b, t, &rows).await?;
        }
        Op::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            for n in column_names {
                let n = ident(n);
                let Some(c) = t.column(&n) else {
                    if *if_exists {
                        continue;
                    }
                    return Err(Error::UndefinedColumn(n));
                };
                if let Some(what) = referenced(t, c) {
                    return unsupported(format!(
                        "dropping column \"{n}\", which is part of {what}"
                    ));
                }
                let rows = all_rows(b.txn, t).await?;
                let unshift = |p: &mut usize| {
                    if *p > c {
                        *p -= 1;
                    }
                };
                t.pk.iter_mut().for_each(unshift);
                for ix in &mut t.indexes {
                    ix.columns.iter_mut().for_each(unshift);
                }
                for f in &mut t.fks {
                    f.cols.iter_mut().for_each(unshift);
                }
                t.columns.remove(c);
                t.checks.retain(|k| !k.sql.contains(&n));
                t.stats = None;
                for mut row in rows {
                    row.remove(c);
                    b.txn
                        .put(&t.row_key(&row), &keycodec::encode_row(&row))
                        .await?;
                }
            }
        }
        Op::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            let (old, new) = (ident(old_column_name), ident(new_column_name));
            let c = t
                .column(&old)
                .ok_or_else(|| Error::UndefinedColumn(old.clone()))?;
            if t.column(&new).is_some() {
                return Err(Error::DuplicateObject(format!("column \"{new}\"")));
            }
            if t.checks.iter().any(|k| k.sql.contains(&old)) {
                return unsupported(format!(
                    "renaming column \"{old}\", which a CHECK constraint uses"
                ));
            }
            t.columns[c].name = new;
        }
        Op::AlterColumn { column_name, op } => {
            let n = ident(column_name);
            let c = t
                .column(&n)
                .ok_or_else(|| Error::UndefinedColumn(n.clone()))?;
            match op {
                ast::AlterColumnOperation::SetNotNull => {
                    if all_rows(b.txn, t).await?.iter().any(|r| r[c].is_null()) {
                        return Err(Error::NotNullViolation(n, t.name.clone()));
                    }
                    t.columns[c].nullable = false;
                }
                ast::AlterColumnOperation::DropNotNull => {
                    if t.pk.contains(&c) {
                        return Err(Error::InvalidInput(format!(
                            "column \"{n}\" is in a primary key"
                        )));
                    }
                    t.columns[c].nullable = true;
                }
                ast::AlterColumnOperation::SetDefault { value } if is_now(value) => {
                    t.columns[c].default = None;
                    t.columns[c].default_now = true;
                }
                ast::AlterColumnOperation::SetDefault { value } => {
                    match b.bind_expr(value, &[], &mut None).await? {
                        Expr::Literal(v, _) => {
                            t.columns[c].default = Some(v.coerce(t.columns[c].ty)?);
                            t.columns[c].default_now = false;
                        }
                        _ => return unsupported(format!("DEFAULT {value}")),
                    }
                }
                ast::AlterColumnOperation::DropDefault => {
                    t.columns[c].default = None;
                    t.columns[c].default_now = false;
                }
                other => return unsupported(format!("ALTER COLUMN … {other}")),
            }
        }
        other => return unsupported(format!("ALTER TABLE … {other}")),
    }
    Ok(())
}
