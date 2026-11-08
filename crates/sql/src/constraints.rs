//! CHECK and FOREIGN KEY enforcement.
//!
//! Both run inside the writing statement's transaction, so they see exactly
//! what that transaction sees and conflict like any other read under SSI:
//!
//! - **CHECK** binds the stored expression against the table's columns and
//!   evaluates it over the written rows as one Arrow batch. A row fails only if
//!   the result is false; NULL passes, as in SQL.
//! - **FOREIGN KEY**, child side: every written row whose key columns are all
//!   non-NULL must find its parent through the parent's primary key or unique
//!   index (one `Get`). That read enters the timestamp cache, so a concurrent
//!   delete of the parent cannot commit underneath it.
//! - **FOREIGN KEY**, parent side: before rows are deleted (or their referenced
//!   columns change), every table referencing this one is searched for rows that
//!   point at them: through an index on the child's key columns when one exists,
//!   by a scan of the child table otherwise. `RESTRICT`/`NO ACTION` refuses,
//!   `CASCADE` deletes those rows (recursively), `SET NULL` clears their key.

use std::collections::HashSet;
use std::sync::Arc;

use sqlparser::ast;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::binder::Binder;
use crate::catalog::{self, FkAction, ForeignKey, Table};
use crate::keycodec::{self, PRIMARY};
use crate::kv::{BoxFut, KvTxn};
use crate::plan::Field;
use crate::value::Value;
use crate::{Error, Result, exec};

fn parse_expr(sql: &str) -> Result<ast::Expr> {
    Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| Error::Internal(format!("stored CHECK does not parse: {e}")))
}

/// Fields for a table's columns, as a CHECK expression sees them.
fn row_scope(b: &mut Binder<'_>, t: &Table) -> Vec<Field> {
    t.columns
        .iter()
        .map(|c| Field {
            id: b.col_id(),
            name: c.name.clone(),
            qualifier: Some(t.name.clone()),
            ty: c.ty,
            nullable: c.nullable,
            hidden: c.hidden,
        })
        .collect()
}

fn rows_batch(
    t: &Table,
    fields: &[Field],
    rows: &[Vec<Value>],
) -> Result<arrow::array::RecordBatch> {
    let cols = t
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| crate::value::build_array(c.ty, rows.iter().map(|r| r[i].clone())))
        .collect::<Result<Vec<_>>>()?;
    Ok(arrow::array::RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(
            fields
                .iter()
                .map(|f| arrow::datatypes::Field::new(format!("c{}", f.id.0), f.ty.arrow(), true))
                .collect::<Vec<_>>(),
        )),
        cols,
    )?)
}

/// Check that a CHECK expression binds against `t` (at CREATE / ALTER time).
pub async fn validate_check(b: &mut Binder<'_>, t: &Table, sql: &str) -> Result<()> {
    let scope = row_scope(b, t);
    b.bind_bool(&parse_expr(sql)?, &scope).await.map(|_| ())
}

/// Every CHECK of `t` over `rows`.
pub async fn check_rows(b: &mut Binder<'_>, t: &Table, rows: &[Vec<Value>]) -> Result<()> {
    if t.checks.is_empty() || rows.is_empty() {
        return Ok(());
    }
    let scope = row_scope(b, t);
    let batch = rows_batch(t, &scope, rows)?;
    for c in &t.checks {
        let pred = b.bind_bool(&parse_expr(&c.sql)?, &scope).await?;
        let out = exec::eval(&pred, &batch, &scope, b.now)?;
        let out = out
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| Error::Internal("CHECK is not boolean".into()))?;
        if (0..out.len()).any(|i| !arrow::array::Array::is_null(out, i) && !out.value(i)) {
            return Err(Error::CheckViolation(t.name.clone(), c.name.clone()));
        }
    }
    Ok(())
}

/// A row of `parent` with `values` at `cols` and NULL elsewhere: enough to
/// build a key on those columns.
fn sparse_row(parent: &Table, cols: &[usize], values: &[Value]) -> Vec<Value> {
    let mut row = vec![Value::Null; parent.columns.len()];
    for (&c, v) in cols.iter().zip(values) {
        row[c] = v.clone();
    }
    row
}

/// Does `parent` have a row with `values` in `cols`? `cols` is its primary key
/// or the columns of a unique index (checked when the key was declared).
async fn parent_exists(
    txn: &mut dyn KvTxn,
    parent: &Table,
    cols: &[usize],
    values: &[Value],
) -> Result<bool> {
    let row = sparse_row(parent, cols, values);
    if same_set(cols, &parent.pk) {
        return Ok(txn.get(&parent.row_key(&row)).await?.is_some());
    }
    let ix = parent
        .indexes
        .iter()
        .find(|ix| ix.unique && same_set(cols, &ix.columns))
        .ok_or_else(|| {
            Error::Internal(format!(
                "no unique key on {} for a foreign key",
                parent.name
            ))
        })?;
    Ok(txn.get(&parent.index_key(ix, &row).0).await?.is_some())
}

fn same_set(a: &[usize], b: &[usize]) -> bool {
    a.len() == b.len() && a.iter().all(|x| b.contains(x))
}

/// Child side: each row's parents exist.
pub async fn check_parents(txn: &mut dyn KvTxn, t: &Table, rows: &[Vec<Value>]) -> Result<()> {
    for fk in &t.fks {
        let parent = if fk.parent == t.name {
            t.clone()
        } else {
            catalog::table(txn, &fk.parent).await?
        };
        let mut seen = HashSet::new();
        for row in rows {
            let values: Vec<Value> = fk.cols.iter().map(|&c| row[c].clone()).collect();
            if values.iter().any(Value::is_null) || !seen.insert(keycodec::encode_row(&values)) {
                continue;
            }
            // A self-reference may point at a row written by this same statement.
            let in_batch = fk.parent == t.name
                && rows
                    .iter()
                    .any(|r| fk.parent_cols.iter().zip(&values).all(|(&c, v)| &r[c] == v));
            if !in_batch && !parent_exists(txn, &parent, &fk.parent_cols, &values).await? {
                return Err(Error::ForeignKeyViolation(format!(
                    "insert or update on table \"{}\" violates foreign key constraint \"{}\": key is not present in table \"{}\"",
                    t.name, fk.name, fk.parent
                )));
            }
        }
    }
    Ok(())
}

/// Rows of `child` whose `fk.cols` equal one of `keys`.
async fn referencing_rows(
    txn: &mut dyn KvTxn,
    child: &Table,
    fk: &ForeignKey,
    keys: &HashSet<Vec<u8>>,
) -> Result<Vec<Vec<Value>>> {
    let n = child.columns.len();
    let key_of = |row: &[Value]| {
        let v: Vec<Value> = fk.cols.iter().map(|&c| row[c].clone()).collect();
        keycodec::encode_row(&v)
    };
    // An index whose leading columns are the foreign key: one range per key.
    let index = child.indexes.iter().find(|ix| {
        ix.public && ix.columns.len() >= fk.cols.len() && ix.columns[..fk.cols.len()] == fk.cols[..]
    });
    let Some(ix) = index else {
        let start = keycodec::index_prefix(child.id, PRIMARY);
        let end = keycodec::prefix_end(&start);
        let mut out = Vec::new();
        for (_, v) in txn.scan(&start, &end, u32::MAX).await? {
            let row = keycodec::decode_row(&v, n)?;
            if keys.contains(&key_of(&row)) {
                out.push(row);
            }
        }
        return Ok(out);
    };
    let pk_prefix = keycodec::index_prefix(child.id, PRIMARY);
    let mut out = Vec::new();
    for k in keys {
        let values = keycodec::decode_row(k, fk.cols.len())?;
        let mut start = keycodec::index_prefix(child.id, ix.id);
        for v in &values {
            keycodec::encode_key(v, &mut start);
        }
        let end = keycodec::prefix_end(&start);
        for (ik, iv) in txn.scan(&start, &end, u32::MAX).await? {
            let pk_key = if iv.is_empty() {
                // Non-unique layout: the index key ends with the primary key.
                let mut rest = &ik[9..];
                for &c in &ix.columns {
                    rest = keycodec::decode_key(child.columns[c].ty, rest)?.1;
                }
                [pk_prefix.as_slice(), rest].concat()
            } else {
                iv
            };
            if let Some(row) = txn.get(&pk_key).await? {
                let row = keycodec::decode_row(&row, n)?;
                if keys.contains(&key_of(&row)) {
                    out.push(row);
                }
            }
        }
    }
    Ok(out)
}

/// Parent side, before `rows` of `t` are deleted (`new` = None) or updated to
/// `new`. Applies each referencing foreign key's action.
pub fn before_parent_change<'a>(
    b: &'a mut Binder<'_>,
    t: &'a Table,
    rows: &'a [Vec<Value>],
    new: Option<&'a [Vec<Value>]>,
) -> BoxFut<'a, Result<()>> {
    Box::pin(async move {
        if rows.is_empty() {
            return Ok(());
        }
        let tables = catalog::list_tables(b.txn).await?;
        for child in tables
            .iter()
            .filter(|c| c.fks.iter().any(|f| f.parent == t.name))
        {
            for fk in child.fks.iter().filter(|f| f.parent == t.name) {
                // Keys that disappear: deleted rows, or updated rows whose
                // referenced columns changed.
                let mut gone = HashSet::new();
                for (i, row) in rows.iter().enumerate() {
                    let old: Vec<Value> = fk.parent_cols.iter().map(|&c| row[c].clone()).collect();
                    if old.iter().any(Value::is_null) {
                        continue;
                    }
                    let changed =
                        new.is_none_or(|n| fk.parent_cols.iter().any(|&c| n[i][c] != row[c]));
                    if changed {
                        gone.insert(keycodec::encode_row(&old));
                    }
                }
                if gone.is_empty() {
                    continue;
                }
                let child = if child.name == t.name {
                    t.clone()
                } else {
                    child.clone()
                };
                let mut refs = referencing_rows(b.txn, &child, fk, &gone).await?;
                if child.name == t.name {
                    // Rows deleted by this same statement do not hold anything back.
                    refs.retain(|r| !rows.contains(r));
                }
                if refs.is_empty() {
                    continue;
                }
                match (fk.on_delete, new) {
                    (FkAction::Cascade, None) => {
                        before_parent_change(b, &child, &refs, None).await?;
                        for r in &refs {
                            crate::dml::delete_row(b.txn, &child, r).await?;
                        }
                    }
                    (FkAction::SetNull, None) => {
                        let updated: Vec<Vec<Value>> = refs
                            .iter()
                            .map(|r| {
                                let mut n = r.clone();
                                for &c in &fk.cols {
                                    n[c] = Value::Null;
                                }
                                n
                            })
                            .collect();
                        for (old, n) in refs.iter().zip(&updated) {
                            crate::dml::replace_row(b.txn, &child, old, n).await?;
                        }
                    }
                    _ => {
                        return Err(Error::ForeignKeyViolation(format!(
                            "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
                            t.name, fk.name, child.name
                        )));
                    }
                }
            }
        }
        Ok(())
    })
}
