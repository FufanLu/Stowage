//! A vectorized executor over Arrow record batches.
//!
//! Every operator consumes whole batches and produces one (operator-at-a-time,
//! as in MonetDB): expressions are evaluated column-at-a-time with Arrow compute
//! kernels, hash joins and hash aggregation key on Arrow's row format, and sorts
//! use `lexsort`. Only scans touch the key-value layer.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, StringArray, UInt32Array,
    UInt32Builder,
};
use arrow::compute::kernels::{cmp, concat_elements, numeric, zip};
use arrow::compute::{self, CastOptions, SortColumn, SortOptions};
use arrow::datatypes::{Field as ArrowField, Schema};
use arrow::error::ArrowError;
use arrow::row::{RowConverter, SortField};

use crate::catalog::Table;
use crate::keycodec::{self, PRIMARY};
use crate::kv::KvTxn;
use crate::plan::{Access, AggCall, AggFunc, BinOp, Expr, Field, Func, JoinKind, Plan};
use crate::value::{DataType, Value, build_array};
use crate::{Error, Result};

/// Per-node measurements for `EXPLAIN ANALYZE`, in pre-order.
#[derive(Debug, Clone, Default)]
pub struct NodeStat {
    pub rows: usize,
    pub nanos: u128,
    /// Key-value requests this node made itself.
    pub kv_requests: u64,
}

pub fn size(p: &Plan) -> usize {
    1 + p.children().iter().map(|c| size(c)).sum::<usize>()
}

struct Ctx<'t> {
    txn: &'t mut dyn KvTxn,
    now: i64,
    stats: Option<Vec<NodeStat>>,
}

/// Run `plan`; with `analyze`, fill in per-node statistics.
pub async fn execute(
    plan: &Plan,
    txn: &mut dyn KvTxn,
    now: i64,
    analyze: Option<&mut Vec<NodeStat>>,
) -> Result<RecordBatch> {
    let want = analyze.is_some();
    let mut ctx = Ctx {
        txn,
        now,
        stats: want.then(|| vec![NodeStat::default(); size(plan)]),
    };
    let b = exec(plan, &mut ctx, 0).await?;
    if let (Some(out), Some(s)) = (analyze, ctx.stats) {
        *out = s;
    }
    Ok(b)
}

fn arrow_schema(fields: &[Field]) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| ArrowField::new(format!("c{}", f.id.0), f.ty.arrow(), true))
            .collect::<Vec<_>>(),
    ))
}

fn batch(fields: &[Field], cols: Vec<ArrayRef>, rows: usize) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new_with_options(
        arrow_schema(fields),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}

fn arrow_err(e: ArrowError) -> Error {
    match e {
        ArrowError::DivideByZero => Error::DivisionByZero,
        ArrowError::ArithmeticOverflow(m) => Error::OutOfRange(m),
        ArrowError::CastError(m) | ArrowError::ParseError(m) => Error::InvalidInput(m),
        e => Error::Internal(e.to_string()),
    }
}

impl From<ArrowError> for Error {
    fn from(e: ArrowError) -> Self {
        arrow_err(e)
    }
}

fn exec<'a, 't: 'a>(
    p: &'a Plan,
    ctx: &'a mut Ctx<'t>,
    node: usize,
) -> crate::kv::BoxFut<'a, Result<RecordBatch>> {
    Box::pin(async move {
        let start = Instant::now();
        let child = |i: usize| -> usize {
            node + 1 + p.children()[..i].iter().map(|c| size(c)).sum::<usize>()
        };
        let mut kv = 0u64;
        let out = match p {
            Plan::Scan {
                table,
                cols: _,
                access,
                filter,
                limit,
                ..
            } => {
                let fields = p.schema();
                let rows = scan_rows(
                    ctx.txn,
                    table,
                    access,
                    if filter.is_none() { *limit } else { None },
                    &mut kv,
                )
                .await?;
                let n = rows.len();
                let mut columns = Vec::with_capacity(table.columns.len());
                for (i, c) in table.columns.iter().enumerate() {
                    columns.push(build_array(
                        c.ty,
                        rows.iter()
                            .map(|r| r.get(i).cloned().unwrap_or(Value::Null)),
                    )?);
                }
                let b = batch(&fields, columns, n)?;
                let b = match filter {
                    Some(f) => filter_batch(&b, &fields, f, ctx.now)?,
                    None => b,
                };
                match limit {
                    Some(l) if b.num_rows() > *l as usize => b.slice(0, *l as usize),
                    _ => b,
                }
            }
            Plan::Values { rows, schema } => {
                let one = batch(&[], vec![], 1)?;
                let mut columns: Vec<Vec<ArrayRef>> = vec![Vec::new(); schema.len()];
                for r in rows {
                    for (i, e) in r.iter().enumerate() {
                        let a = eval(e, &one, &[], ctx.now)?;
                        let a = if a.data_type() == &schema[i].ty.arrow() {
                            a
                        } else {
                            compute::cast(&a, &schema[i].ty.arrow())?
                        };
                        columns[i].push(a);
                    }
                }
                let cols = columns
                    .iter()
                    .zip(schema)
                    .map(|(c, f)| {
                        if c.is_empty() {
                            Ok(build_array(f.ty, std::iter::empty())?)
                        } else {
                            let refs: Vec<&dyn Array> = c.iter().map(AsRef::as_ref).collect();
                            Ok(compute::concat(&refs)?)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                batch(schema, cols, rows.len())?
            }
            Plan::Filter { input, pred } => {
                let b = exec(input, ctx, child(0)).await?;
                filter_batch(&b, &input.schema(), pred, ctx.now)?
            }
            Plan::Project {
                input,
                exprs,
                schema,
            } => {
                let b = exec(input, ctx, child(0)).await?;
                let fields = input.schema();
                let cols = exprs
                    .iter()
                    .map(|e| eval(e, &b, &fields, ctx.now))
                    .collect::<Result<Vec<_>>>()?;
                let cols = cols
                    .into_iter()
                    .zip(schema)
                    .map(|(c, f)| {
                        if c.data_type() == &f.ty.arrow() {
                            Ok(c)
                        } else {
                            Ok(compute::cast(&c, &f.ty.arrow())?)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                batch(schema, cols, b.num_rows())?
            }
            Plan::Join {
                kind,
                left,
                right,
                equi,
                residual,
            } => {
                let l = exec(left, ctx, child(0)).await?;
                let r = exec(right, ctx, child(1)).await?;
                join(
                    *kind,
                    &l,
                    &left.schema(),
                    &r,
                    &right.schema(),
                    equi,
                    residual.as_ref(),
                    &p.schema(),
                    ctx.now,
                )?
            }
            Plan::Aggregate {
                input,
                group,
                aggs,
                schema,
            } => {
                let b = exec(input, ctx, child(0)).await?;
                aggregate(&b, &input.schema(), group, aggs, schema, ctx.now)?
            }
            Plan::Sort { input, keys } => {
                let b = exec(input, ctx, child(0)).await?;
                let fields = input.schema();
                if b.num_rows() <= 1 {
                    b
                } else {
                    let cols = keys
                        .iter()
                        .map(|k| {
                            Ok(SortColumn {
                                values: eval(&k.expr, &b, &fields, ctx.now)?,
                                options: Some(SortOptions {
                                    descending: k.desc,
                                    nulls_first: k.nulls_first,
                                }),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let idx = compute::lexsort_to_indices(&cols, None)?;
                    take_batch(&b, &idx)?
                }
            }
            Plan::Limit {
                input,
                limit,
                offset,
            } => {
                let b = exec(input, ctx, child(0)).await?;
                let n = b.num_rows();
                let from = (*offset as usize).min(n);
                let len = limit.map_or(n - from, |l| (l as usize).min(n - from));
                b.slice(from, len)
            }
            Plan::Distinct { input } => {
                let b = exec(input, ctx, child(0)).await?;
                if b.num_columns() == 0 || b.num_rows() == 0 {
                    b
                } else {
                    let rows = row_keys(b.columns())?;
                    let mut seen = HashSet::new();
                    let keep: Vec<u32> = (0..b.num_rows())
                        .filter(|&i| seen.insert(rows.row(i).as_ref().to_vec()))
                        .map(|i| i as u32)
                        .collect();
                    take_batch(&b, &UInt32Array::from(keep))?
                }
            }
        };
        if let Some(s) = ctx.stats.as_mut().and_then(|s| s.get_mut(node)) {
            s.rows = out.num_rows();
            s.nanos = start.elapsed().as_nanos();
            s.kv_requests = kv;
        }
        Ok(out)
    })
}

fn take_batch(b: &RecordBatch, idx: &UInt32Array) -> Result<RecordBatch> {
    let cols = b
        .columns()
        .iter()
        .map(|c| compute::take(c.as_ref(), idx, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new_with_options(
        b.schema(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(idx.len())),
    )?)
}

fn row_keys(cols: &[ArrayRef]) -> Result<arrow::row::Rows> {
    let conv = RowConverter::new(
        cols.iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect(),
    )?;
    Ok(conv.convert_columns(cols)?)
}

fn filter_batch(b: &RecordBatch, fields: &[Field], pred: &Expr, now: i64) -> Result<RecordBatch> {
    let mask = eval(pred, b, fields, now)?;
    let mask = as_bool(&mask)?;
    let mask = null_as_false(mask); // NULL counts as false
    Ok(compute::filter_record_batch(b, &mask)?)
}

/// SQL conditions treat NULL as false.
fn null_as_false(b: &BooleanArray) -> BooleanArray {
    if b.null_count() == 0 {
        b.clone()
    } else {
        compute::prep_null_mask_filter(b)
    }
}

fn as_bool(a: &ArrayRef) -> Result<&BooleanArray> {
    a.as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| Error::InvalidInput(format!("expected a boolean, got {}", a.data_type())))
}

// ---------------------------------------------------------------- scans

/// Key span `[start, end)` an access path reads within an index.
pub fn span(table: &Table, access: &Access) -> (Vec<u8>, Vec<u8>) {
    let mut base = keycodec::index_prefix(table.id, access.index);
    for v in &access.eq {
        keycodec::encode_key(v, &mut base);
    }
    let start = match &access.lower {
        Some((v, incl)) => {
            let mut k = base.clone();
            keycodec::encode_key(v, &mut k);
            if *incl { k } else { keycodec::prefix_end(&k) }
        }
        None => base.clone(),
    };
    let end = match &access.upper {
        Some((v, incl)) => {
            let mut k = base.clone();
            keycodec::encode_key(v, &mut k);
            if *incl { keycodec::prefix_end(&k) } else { k }
        }
        None => keycodec::prefix_end(&base),
    };
    (start, end)
}

async fn scan_rows(
    txn: &mut dyn KvTxn,
    table: &Table,
    access: &Access,
    limit: Option<u64>,
    kv: &mut u64,
) -> Result<Vec<Vec<Value>>> {
    let (start, end) = span(table, access);
    if start >= end && !end.is_empty() {
        return Ok(Vec::new());
    }
    let limit = limit.map_or(u32::MAX, |l| u32::try_from(l).unwrap_or(u32::MAX));
    *kv += 1;
    let pairs = txn.scan(&start, &end, limit).await?;
    let n = table.columns.len();
    if access.index == PRIMARY {
        return pairs
            .into_iter()
            .map(|(_, v)| keycodec::decode_row(&v, n))
            .collect();
    }
    // A secondary index: find each row by its primary key.
    let ix = table
        .indexes
        .iter()
        .find(|i| i.id == access.index)
        .ok_or_else(|| Error::Internal("missing index".into()))?;
    let prefix = keycodec::index_prefix(table.id, PRIMARY);
    let mut rows = Vec::with_capacity(pairs.len());
    for (k, v) in pairs {
        let pk_key = if v.is_empty() {
            // Non-unique layout: the key ends with the encoded primary key.
            let mut rest = &k[9..];
            for &c in &ix.columns {
                rest = keycodec::decode_key(table.columns[c].ty, rest)?.1;
            }
            let mut key = prefix.clone();
            key.extend_from_slice(rest);
            key
        } else {
            v
        };
        *kv += 1;
        if let Some(row) = txn.get(&pk_key).await? {
            rows.push(keycodec::decode_row(&row, n)?);
        }
    }
    Ok(rows)
}

// ---------------------------------------------------------------- expressions

/// Evaluate `e` over `b` (whose columns are `fields`): one value per row.
pub fn eval(e: &Expr, b: &RecordBatch, fields: &[Field], now: i64) -> Result<ArrayRef> {
    let n = b.num_rows();
    let ty_of = |c| {
        fields
            .iter()
            .find(|f| f.id == c)
            .map_or(DataType::Text, |f| f.ty)
    };
    Ok(match e {
        Expr::Column(c) => {
            let i = fields
                .iter()
                .position(|f| f.id == *c)
                .ok_or_else(|| Error::Internal(format!("column #{} not in input", c.0)))?;
            b.column(i).clone()
        }
        Expr::Literal(v, ty) => build_array(*ty, std::iter::repeat_n(v.clone(), n))?,
        Expr::Binary(op, l, r) => {
            let (a, c) = (eval(l, b, fields, now)?, eval(r, b, fields, now)?);
            let (a, c) = if a.data_type() == c.data_type() {
                (a, c)
            } else if op.is_comparison()
                || matches!(
                    op,
                    BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod
                )
            {
                // A NULL literal is typed Text; take the other side's type.
                let target = if a.null_count() == a.len() {
                    c.data_type().clone()
                } else {
                    a.data_type().clone()
                };
                (compute::cast(&a, &target)?, compute::cast(&c, &target)?)
            } else {
                (a, c)
            };
            match op {
                BinOp::Add => numeric::add(&a, &c)?,
                BinOp::Sub => numeric::sub(&a, &c)?,
                BinOp::Mul => numeric::mul(&a, &c)?,
                BinOp::Div => numeric::div(&a, &c)?,
                BinOp::Mod => numeric::rem(&a, &c)?,
                BinOp::Eq => Arc::new(cmp::eq(&a, &c)?),
                BinOp::NotEq => Arc::new(cmp::neq(&a, &c)?),
                BinOp::Lt => Arc::new(cmp::lt(&a, &c)?),
                BinOp::LtEq => Arc::new(cmp::lt_eq(&a, &c)?),
                BinOp::Gt => Arc::new(cmp::gt(&a, &c)?),
                BinOp::GtEq => Arc::new(cmp::gt_eq(&a, &c)?),
                BinOp::And => Arc::new(compute::and_kleene(as_bool(&a)?, as_bool(&c)?)?),
                BinOp::Or => Arc::new(compute::or_kleene(as_bool(&a)?, as_bool(&c)?)?),
                BinOp::Concat => {
                    let (x, y) = (
                        compute::cast(&a, &arrow::datatypes::DataType::Utf8)?,
                        compute::cast(&c, &arrow::datatypes::DataType::Utf8)?,
                    );
                    let (x, y) = (downcast_str(&x)?, downcast_str(&y)?);
                    Arc::new(concat_elements::concat_elements_utf8(x, y)?)
                }
            }
        }
        Expr::Not(x) => Arc::new(compute::not(as_bool(&eval(x, b, fields, now)?)?)?),
        Expr::Neg(x) => numeric::neg(&eval(x, b, fields, now)?)?,
        Expr::IsNull(x, negated) => {
            let a = eval(x, b, fields, now)?;
            Arc::new(if *negated {
                compute::is_not_null(&a)?
            } else {
                compute::is_null(&a)?
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let a = eval(expr, b, fields, now)?;
            let mut acc: Option<BooleanArray> = None;
            for item in list {
                let v = eval(item, b, fields, now)?;
                let v = if v.data_type() == a.data_type() {
                    v
                } else {
                    compute::cast(&v, a.data_type())?
                };
                let eq = cmp::eq(&a, &v)?;
                acc = Some(match acc {
                    None => eq,
                    Some(x) => compute::or_kleene(&x, &eq)?,
                });
            }
            let r = acc.unwrap_or_else(|| BooleanArray::from(vec![false; n]));
            Arc::new(if *negated { compute::not(&r)? } else { r })
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            insensitive,
        } => {
            let (a, p) = (eval(expr, b, fields, now)?, eval(pattern, b, fields, now)?);
            use arrow::compute::kernels::comparison as like;
            Arc::new(match (negated, insensitive) {
                (false, false) => like::like(&a, &p)?,
                (true, false) => like::nlike(&a, &p)?,
                (false, true) => like::ilike(&a, &p)?,
                (true, true) => like::nilike(&a, &p)?,
            })
        }
        Expr::Cast(x, to) => {
            let a = eval(x, b, fields, now)?;
            let from = DataType::from_arrow(a.data_type())?;
            if from == *to {
                a
            } else if matches!(
                (from, to),
                (DataType::Timestamp, _)
                    | (_, DataType::Timestamp)
                    | (DataType::Text, DataType::Bool)
                    | (DataType::Float, DataType::Int)
            ) {
                // Postgres text formats and rounding: convert value by value.
                let vals = (0..n)
                    .map(|i| Value::from_array(a.as_ref(), i)?.coerce(*to))
                    .collect::<Result<Vec<_>>>()?;
                build_array(*to, vals)?
            } else {
                compute::cast_with_options(
                    &a,
                    &to.arrow(),
                    &CastOptions {
                        safe: false,
                        ..CastOptions::default()
                    },
                )?
            }
        }
        Expr::Case {
            whens, otherwise, ..
        } => {
            let ty = e.ty(&ty_of);
            let mut out = match otherwise {
                Some(o) => eval(o, b, fields, now)?,
                None => build_array(ty, std::iter::repeat_n(Value::Null, n))?,
            };
            for (w, t) in whens.iter().rev() {
                let cond = eval(w, b, fields, now)?;
                let cond = null_as_false(as_bool(&cond)?);
                let then = eval(t, b, fields, now)?;
                let then = if then.data_type() == out.data_type() {
                    then
                } else {
                    compute::cast(&then, out.data_type())?
                };
                out = zip::zip(&cond, &then, &out)?;
            }
            out
        }
        Expr::Func(f, args) => {
            let arrays = args
                .iter()
                .map(|a| eval(a, b, fields, now))
                .collect::<Result<Vec<_>>>()?;
            let ty = e.ty(&ty_of);
            let mut out = Vec::with_capacity(n);
            for row in 0..n {
                let vals = arrays
                    .iter()
                    .map(|a| Value::from_array(a.as_ref(), row))
                    .collect::<Result<Vec<_>>>()?;
                out.push(call(*f, vals, now)?);
            }
            build_array(ty, out)?
        }
        Expr::AggRef(_) => return Err(Error::Internal("unresolved aggregate".into())),
    })
}

fn downcast_str(a: &ArrayRef) -> Result<&StringArray> {
    a.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Internal("expected text".into()))
}

fn call(f: Func, args: Vec<Value>, now: i64) -> Result<Value> {
    let arg = |i: usize| args.get(i).cloned().unwrap_or(Value::Null);
    Ok(match f {
        Func::Coalesce => args
            .into_iter()
            .find(|v| !v.is_null())
            .unwrap_or(Value::Null),
        Func::Nullif => {
            if arg(0).sql_cmp(&arg(1)) == std::cmp::Ordering::Equal && !arg(1).is_null() {
                Value::Null
            } else {
                arg(0)
            }
        }
        Func::Now => Value::Timestamp(now),
        _ if arg(0).is_null() => Value::Null,
        Func::Lower => Value::Text(text(&arg(0)).to_lowercase()),
        Func::Upper => Value::Text(text(&arg(0)).to_uppercase()),
        Func::Length => Value::Int(text(&arg(0)).chars().count() as i64),
        Func::Abs => match arg(0) {
            Value::Int(i) => Value::Int(
                i.checked_abs()
                    .ok_or_else(|| Error::OutOfRange("bigint out of range".into()))?,
            ),
            Value::Float(x) => Value::Float(x.abs()),
            v => return Err(Error::InvalidInput(format!("abs({v})"))),
        },
        Func::Round => match (arg(0), arg(1)) {
            (Value::Float(x), Value::Int(d)) => {
                let m = 10f64.powi(d as i32);
                Value::Float((x * m).round() / m)
            }
            (Value::Float(x), _) => Value::Float(x.round()),
            (v, _) => v,
        },
        Func::Substr => {
            let s: Vec<char> = text(&arg(0)).chars().collect();
            let from = match arg(1) {
                Value::Int(i) => i,
                _ => 1,
            };
            let len = match arg(2) {
                Value::Int(l) => Some(l),
                _ => None,
            };
            let start = (from - 1).max(0) as usize;
            let end = match len {
                Some(l) => ((from - 1 + l).max(0) as usize).min(s.len()),
                None => s.len(),
            };
            Value::Text(
                s.get(start.min(end)..end)
                    .map(|x| x.iter().collect())
                    .unwrap_or_default(),
            )
        }
    })
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        v => v.to_string(),
    }
}

// ---------------------------------------------------------------- joins

#[allow(clippy::too_many_arguments)]
fn join(
    kind: JoinKind,
    l: &RecordBatch,
    lf: &[Field],
    r: &RecordBatch,
    rf: &[Field],
    equi: &[(Expr, Expr)],
    residual: Option<&Expr>,
    out_fields: &[Field],
    now: i64,
) -> Result<RecordBatch> {
    // Candidate pairs (left row, right row).
    let mut li: Vec<u32> = Vec::new();
    let mut ri: Vec<u32> = Vec::new();
    if equi.is_empty() {
        let pairs = l.num_rows() as u64 * r.num_rows() as u64;
        if pairs > 50_000_000 {
            return Err(Error::Unsupported(format!(
                "a cross product of {pairs} rows"
            )));
        }
        for i in 0..l.num_rows() as u32 {
            for j in 0..r.num_rows() as u32 {
                li.push(i);
                ri.push(j);
            }
        }
    } else if l.num_rows() > 0 && r.num_rows() > 0 {
        let lk = equi
            .iter()
            .map(|(a, _)| eval(a, l, lf, now))
            .collect::<Result<Vec<_>>>()?;
        let mut rk = equi
            .iter()
            .map(|(_, b)| eval(b, r, rf, now))
            .collect::<Result<Vec<_>>>()?;
        for (a, b) in lk.iter().zip(rk.iter_mut()) {
            if a.data_type() != b.data_type() {
                *b = compute::cast(b, a.data_type())?;
            }
        }
        let conv = RowConverter::new(
            lk.iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect(),
        )?;
        let (lrows, rrows) = (conv.convert_columns(&lk)?, conv.convert_columns(&rk)?);
        let has_null = |cols: &[ArrayRef], i: usize| cols.iter().any(|c| c.is_null(i));
        // Build on the right (the optimizer puts the smaller input there).
        let mut table: HashMap<&[u8], Vec<u32>> = HashMap::new();
        for j in 0..r.num_rows() {
            if !has_null(&rk, j) {
                table.entry(rrows.row(j).data()).or_default().push(j as u32);
            }
        }
        for i in 0..l.num_rows() {
            if has_null(&lk, i) {
                continue;
            }
            if let Some(js) = table.get(lrows.row(i).data()) {
                for &j in js {
                    li.push(i as u32);
                    ri.push(j);
                }
            }
        }
    }
    // Apply the rest of the condition to the candidate pairs.
    let joined = |li: &[u32], ri: &UInt32Array| -> Result<RecordBatch> {
        let lidx = UInt32Array::from(li.to_vec());
        let mut cols = Vec::new();
        for c in l.columns() {
            cols.push(compute::take(c.as_ref(), &lidx, None)?);
        }
        for c in r.columns() {
            cols.push(compute::take(c.as_ref(), ri, None)?);
        }
        batch(out_fields, cols, li.len())
    };
    let (mut li, mut ri) = (li, ri);
    if let Some(res) = residual {
        let cand = joined(&li, &UInt32Array::from(ri.clone()))?;
        let mask = eval(res, &cand, out_fields, now)?;
        let mask = null_as_false(as_bool(&mask)?);
        let keep: Vec<usize> = (0..mask.len()).filter(|&k| mask.value(k)).collect();
        li = keep.iter().map(|&k| li[k]).collect();
        ri = keep.iter().map(|&k| ri[k]).collect();
    }
    let mut rb = UInt32Builder::with_capacity(ri.len());
    for &j in &ri {
        rb.append_value(j);
    }
    if kind == JoinKind::Left {
        // Left rows without a match keep their values; the right side is NULL.
        let mut matched = vec![false; l.num_rows()];
        for &i in &li {
            matched[i as usize] = true;
        }
        for (i, m) in matched.iter().enumerate() {
            if !m {
                li.push(i as u32);
                rb.append_null();
            }
        }
    }
    joined(&li, &rb.finish())
}

// ---------------------------------------------------------------- aggregation

enum Acc {
    Count(i64),
    Sum(Value),
    Avg(f64, i64),
    Min(Value),
    Max(Value),
}

impl Acc {
    fn new(f: AggFunc) -> Self {
        match f {
            AggFunc::Count | AggFunc::CountStar => Self::Count(0),
            AggFunc::Sum => Self::Sum(Value::Null),
            AggFunc::Avg => Self::Avg(0.0, 0),
            AggFunc::Min => Self::Min(Value::Null),
            AggFunc::Max => Self::Max(Value::Null),
        }
    }

    fn add(&mut self, v: Value) -> Result<()> {
        match self {
            Self::Count(c) => {
                if !v.is_null() {
                    *c += 1;
                }
            }
            Self::Sum(s) => {
                *s = match (std::mem::replace(s, Value::Null), v) {
                    (s, Value::Null) => s,
                    (Value::Null, v) => v,
                    (Value::Int(a), Value::Int(b)) => Value::Int(
                        a.checked_add(b)
                            .ok_or_else(|| Error::OutOfRange("bigint out of range".into()))?,
                    ),
                    (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                    (Value::Int(a), Value::Float(b)) | (Value::Float(b), Value::Int(a)) => {
                        Value::Float(a as f64 + b)
                    }
                    (_, v) => return Err(Error::InvalidInput(format!("sum({v})"))),
                }
            }
            Self::Avg(s, n) => match v {
                Value::Int(i) => {
                    *s += i as f64;
                    *n += 1;
                }
                Value::Float(f) => {
                    *s += f;
                    *n += 1;
                }
                Value::Null => {}
                v => return Err(Error::InvalidInput(format!("avg({v})"))),
            },
            Self::Min(m) => {
                if !v.is_null() && (m.is_null() || v.sql_cmp(m).is_lt()) {
                    *m = v;
                }
            }
            Self::Max(m) => {
                if !v.is_null() && (m.is_null() || v.sql_cmp(m).is_gt()) {
                    *m = v;
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> Value {
        match self {
            Self::Count(c) => Value::Int(c),
            Self::Sum(v) | Self::Min(v) | Self::Max(v) => v,
            Self::Avg(_, 0) => Value::Null,
            Self::Avg(s, n) => Value::Float(s / n as f64),
        }
    }
}

fn aggregate(
    b: &RecordBatch,
    fields: &[Field],
    group: &[Expr],
    aggs: &[AggCall],
    schema: &[Field],
    now: i64,
) -> Result<RecordBatch> {
    let n = b.num_rows();
    let keys = group
        .iter()
        .map(|g| eval(g, b, fields, now))
        .collect::<Result<Vec<_>>>()?;
    let args = aggs
        .iter()
        .map(|a| a.arg.as_ref().map(|e| eval(e, b, fields, now)).transpose())
        .collect::<Result<Vec<_>>>()?;
    // Group id per row, in order of first appearance.
    let mut first_row: Vec<u32> = Vec::new();
    let mut gid = vec![0usize; n];
    if !keys.is_empty() && n > 0 {
        let rows = row_keys(&keys)?;
        let mut ids: HashMap<&[u8], usize> = HashMap::new();
        for (i, g) in gid.iter_mut().enumerate() {
            let next = ids.len();
            let id = *ids.entry(rows.row(i).data()).or_insert(next);
            if id == next {
                first_row.push(i as u32);
            }
            *g = id;
        }
    } else if keys.is_empty() {
        first_row.push(0); // one group, even over no rows
    }
    let groups = first_row.len();
    let mut accs: Vec<Vec<Acc>> = (0..groups)
        .map(|_| aggs.iter().map(|a| Acc::new(a.func)).collect())
        .collect();
    let mut seen: Vec<Vec<HashSet<Vec<u8>>>> = (0..groups)
        .map(|_| aggs.iter().map(|_| HashSet::new()).collect())
        .collect();
    for (k, a) in aggs.iter().enumerate() {
        match (&args[k], a.func) {
            (None, AggFunc::CountStar) => {
                if keys.is_empty() {
                    accs[0][k] = Acc::Count(n as i64);
                } else {
                    for &g in &gid {
                        if let Acc::Count(c) = &mut accs[g][k] {
                            *c += 1;
                        }
                    }
                }
            }
            (Some(arr), _) => {
                let distinct_rows = if a.distinct {
                    Some(row_keys(std::slice::from_ref(arr))?)
                } else {
                    None
                };
                for (row, &g) in gid.iter().enumerate().take(n) {
                    if let Some(dr) = &distinct_rows
                        && !seen[g][k].insert(dr.row(row).data().to_vec())
                    {
                        continue;
                    }
                    accs[g][k].add(Value::from_array(arr.as_ref(), row)?)?;
                }
            }
            (None, _) => return Err(Error::Internal("aggregate without argument".into())),
        }
    }
    let idx = UInt32Array::from(first_row);
    let mut cols: Vec<ArrayRef> = Vec::new();
    for k in &keys {
        cols.push(if n == 0 {
            build_array(DataType::from_arrow(k.data_type())?, std::iter::empty())?
        } else {
            compute::take(k.as_ref(), &idx, None)?
        });
    }
    let mut per_agg: Vec<Vec<Value>> = vec![Vec::with_capacity(groups); aggs.len()];
    for g in accs {
        for (k, acc) in g.into_iter().enumerate() {
            per_agg[k].push(acc.finish());
        }
    }
    for (k, vals) in per_agg.into_iter().enumerate() {
        cols.push(build_array(schema[group.len() + k].ty, vals)?);
    }
    batch(schema, cols, groups)
}
