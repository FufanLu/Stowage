//! Statements that change data or schema: INSERT, UPDATE, DELETE, CREATE / DROP
//! TABLE and INDEX, ANALYZE.

use std::collections::HashSet;

use sqlparser::ast;
use stowage_common::Rng;

use crate::binder::{self, Binder, ident, object_name, unsupported};
use crate::catalog::{self, Column, Index, Stats, Table};
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

async fn delete_row(txn: &mut dyn KvTxn, t: &Table, row: &[Value]) -> Result<()> {
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
    for r in 0..batch.num_rows() {
        let mut given: Vec<Option<Value>> = vec![None; t.columns.len()];
        for (k, &col) in targets.iter().enumerate() {
            let v = Value::from_array(batch.column(k).as_ref(), r)?;
            // An explicit NULL for a SERIAL or defaulted column is still NULL.
            given[col] = Some(v);
        }
        let row = complete_row(b.txn, &t, given, rng).await?;
        if insert_row(b.txn, &t, &row, skip_duplicates).await? {
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
        let (old_key, new_key) = (t.row_key(old), t.row_key(&new));
        if old_key != new_key {
            if b.txn.get(&new_key).await?.is_some() {
                return Err(Error::UniqueViolation(constraint_name(&t, None)));
            }
            b.txn.delete(&old_key).await?;
        }
        for ix in &t.indexes {
            let (ok, _) = t.index_key(ix, old);
            let (nk, unique) = t.index_key(ix, &new);
            if ok != nk || old_key != new_key {
                b.txn.delete(&ok).await?;
                if unique && b.txn.get(&nk).await?.is_some() {
                    return Err(Error::UniqueViolation(constraint_name(&t, Some(ix))));
                }
                b.txn.put(&nk, if unique { &new_key } else { &[] }).await?;
            }
        }
        b.txn.put(&new_key, &keycodec::encode_row(&new)).await?;
    }
    Ok(rows.len() as u64)
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
    let (plan, rows) = matching_rows(b, &name, alias, d.selection.as_ref()).await?;
    let Plan::Scan { table: t, .. } = find_scan(&plan) else {
        return Err(Error::Internal("delete without scan".into()));
    };
    let t = t.clone();
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
    for (i, c) in ct.columns.iter().enumerate() {
        let serial = matches!(&c.data_type, ast::DataType::Custom(n, _) if object_name(n).is_ok_and(|n| n.contains("serial")));
        let mut col = Column {
            name: ident(&c.name),
            ty: binder::data_type(&c.data_type)?,
            nullable: true,
            default: None,
            serial,
            hidden: false,
        };
        for o in &c.options {
            match &o.option {
                ast::ColumnOption::NotNull => col.nullable = false,
                ast::ColumnOption::Null => col.nullable = true,
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
                ast::ColumnOption::ForeignKey(_)
                | ast::ColumnOption::Check(_)
                | ast::ColumnOption::Comment(_) => {
                    // Accepted, not enforced (documented in docs/compat.md).
                }
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
            _ => {} // foreign keys and checks: accepted, not enforced
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
        });
        t.next_index_id += 1;
    }
    catalog::put_table(b.txn, &t).await
}

pub async fn create_index(b: &mut Binder<'_>, ci: &ast::CreateIndex) -> Result<()> {
    let table = object_name(&ci.table_name)?;
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
            return Ok(());
        }
        return Err(Error::DuplicateObject(name));
    }
    let ix = Index {
        id: t.next_index_id,
        name,
        columns: cols,
        unique: ci.unique,
    };
    t.next_index_id += 1;
    // Backfill from the existing rows, in this transaction.
    let (start, end) = exec::span(&t, &Access::full());
    let pairs = b.txn.scan(&start, &end, u32::MAX).await?;
    for (key, v) in pairs {
        let row = keycodec::decode_row(&v, t.columns.len())?;
        let (k, unique) = t.index_key(&ix, &row);
        if unique && b.txn.get(&k).await?.is_some() {
            return Err(Error::UniqueViolation(ix.name.clone()));
        }
        b.txn.put(&k, if unique { &key } else { &[] }).await?;
    }
    t.indexes.push(ix);
    catalog::put_table(b.txn, &t).await
}

pub async fn drop(
    b: &mut Binder<'_>,
    object: &ast::ObjectType,
    names: &[ast::ObjectName],
    if_exists: bool,
) -> Result<()> {
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
