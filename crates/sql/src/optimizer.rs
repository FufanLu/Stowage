//! Rewrites a bound plan into the one that runs:
//!
//! 1. **Predicate pushdown**: filter conjuncts move into scans and joins; join
//!    conditions split into hash-join keys and a residual.
//! 2. **Join ordering**: a chain of inner joins is reordered by dynamic
//!    programming over subsets (Selinger style, bushy trees allowed), using
//!    cardinalities estimated from `ANALYZE` statistics, avoiding cross products.
//! 3. **Access paths**: each scan picks the primary key or a secondary index when
//!    equalities on a key prefix (and a range on the next column) narrow it.
//! 4. **Limit pushdown** into unfiltered scans.

use std::collections::BTreeMap;

use crate::catalog::Table;
use crate::keycodec::PRIMARY;
use crate::plan::{Access, BinOp, ColId, Expr, JoinKind, Plan};
use crate::value::Value;

pub fn optimize(plan: Plan) -> Plan {
    let plan = pushdown(plan, Vec::new());
    let plan = reorder(plan);
    access_paths(plan)
}

// ---------------------------------------------------------------- estimates

/// Rows a plan is expected to produce.
pub fn rows(p: &Plan) -> f64 {
    match p {
        Plan::Scan {
            table,
            cols,
            filter,
            limit,
            ..
        } => {
            let base = table
                .stats
                .as_ref()
                .map_or(1000.0, |s| s.rows.max(1) as f64);
            let r = base
                * filter
                    .as_ref()
                    .map_or(1.0, |f| selectivity(f, &|c| ndv_in(table, cols, c)));
            limit.map_or(r, |l| r.min(l as f64)).max(1.0)
        }
        Plan::Values { rows, .. } => rows.len() as f64,
        Plan::Filter { input, pred } => {
            (rows(input) * selectivity(pred, &|c| ndv(input, c))).max(1.0)
        }
        Plan::Project { input, .. } | Plan::Sort { input, .. } => rows(input),
        Plan::Distinct { input } => (rows(input) * 0.5).max(1.0),
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            let r = (rows(input) - *offset as f64).max(0.0);
            limit.map_or(r, |l| r.min(l as f64))
        }
        Plan::Aggregate { input, group, .. } => {
            if group.is_empty() {
                1.0
            } else {
                let d: f64 = group
                    .iter()
                    .map(|g| {
                        if let Expr::Column(c) = g {
                            ndv(input, *c)
                        } else {
                            10.0
                        }
                    })
                    .product();
                d.min(rows(input)).max(1.0)
            }
        }
        Plan::Join {
            kind,
            left,
            right,
            equi,
            residual,
        } => {
            let (l, r) = (rows(left), rows(right));
            let mut out = l * r;
            for (a, b) in equi {
                let da = if let Expr::Column(c) = a {
                    ndv(left, *c)
                } else {
                    10.0
                };
                let db = if let Expr::Column(c) = b {
                    ndv(right, *c)
                } else {
                    10.0
                };
                out /= da.max(db).max(1.0);
            }
            if let Some(f) = residual {
                out *= selectivity(f, &|c| ndv(p, c));
            }
            if *kind == JoinKind::Left {
                out.max(l)
            } else {
                out.max(1.0)
            }
        }
    }
}

/// Distinct values of column `c` somewhere below `p` (a guess if unknown).
pub fn ndv(p: &Plan, c: ColId) -> f64 {
    match p {
        Plan::Scan { table, cols, .. } => ndv_in(table, cols, c),
        _ => p
            .children()
            .iter()
            .map(|ch| ndv(ch, c))
            .find(|d| *d > 0.0)
            .unwrap_or(0.0),
    }
}

fn ndv_in(table: &Table, cols: &[ColId], c: ColId) -> f64 {
    let Some(i) = cols.iter().position(|x| *x == c) else {
        return 0.0;
    };
    match &table.stats {
        Some(s) => s.distinct.get(i).copied().unwrap_or(0).max(1) as f64,
        None if table.pk == [i] => 1000.0,
        None => 10.0,
    }
}

/// Fraction of rows a predicate keeps (Postgres-like defaults when unknown).
pub fn selectivity(e: &Expr, ndv: &dyn Fn(ColId) -> f64) -> f64 {
    match e {
        Expr::Binary(BinOp::And, a, b) => selectivity(a, ndv) * selectivity(b, ndv),
        Expr::Binary(BinOp::Or, a, b) => {
            let (x, y) = (selectivity(a, ndv), selectivity(b, ndv));
            x + y - x * y
        }
        Expr::Not(x) => 1.0 - selectivity(x, ndv),
        Expr::Binary(BinOp::Eq, a, b) => match (a.as_ref(), b.as_ref()) {
            (Expr::Column(c), Expr::Literal(..)) | (Expr::Literal(..), Expr::Column(c)) => {
                1.0 / ndv(*c).max(1.0)
            }
            (Expr::Column(x), Expr::Column(y)) => 1.0 / ndv(*x).max(ndv(*y)).max(1.0),
            _ => 0.005,
        },
        Expr::Binary(op, _, _) if op.is_comparison() => {
            if *op == BinOp::NotEq {
                0.995
            } else {
                1.0 / 3.0
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let s = match expr.as_ref() {
                Expr::Column(c) => (list.len() as f64 / ndv(*c).max(1.0)).min(1.0),
                _ => 0.05,
            };
            if *negated { 1.0 - s } else { s }
        }
        Expr::IsNull(_, negated) => {
            if *negated {
                0.99
            } else {
                0.01
            }
        }
        Expr::Like { .. } => 0.1,
        Expr::Literal(Value::Bool(true), _) => 1.0,
        Expr::Literal(..) => 0.0,
        _ => 1.0 / 3.0,
    }
}

// ---------------------------------------------------------------- pushdown

fn cols_of(p: &Plan) -> Vec<ColId> {
    p.schema().iter().map(|f| f.id).collect()
}

fn covered(e: &Expr, cols: &[ColId]) -> bool {
    e.column_set().iter().all(|c| cols.contains(c))
}

/// Push `preds` (conjuncts that apply above `p`) as far down as they can go.
fn pushdown(p: Plan, mut preds: Vec<Expr>) -> Plan {
    match p {
        Plan::Filter { input, pred } => {
            preds.extend(pred.conjuncts());
            pushdown(*input, preds)
        }
        Plan::Scan {
            table,
            alias,
            cols,
            access,
            filter,
            limit,
        } => {
            if let Some(f) = filter {
                preds.extend(f.conjuncts());
            }
            Plan::Scan {
                table,
                alias,
                cols,
                access,
                filter: Expr::conjoin(preds),
                limit,
            }
        }
        Plan::Join {
            kind,
            left,
            right,
            mut equi,
            residual,
        } => {
            let (lc, rc) = (cols_of(&left), cols_of(&right));
            let (mut to_left, mut to_right, mut above, mut on) =
                (Vec::new(), Vec::new(), Vec::new(), Vec::new());
            for e in preds {
                if covered(&e, &lc) {
                    to_left.push(e);
                } else if kind == JoinKind::Inner && covered(&e, &rc) {
                    to_right.push(e);
                } else if kind == JoinKind::Inner {
                    on.push(e);
                } else {
                    above.push(e); // a WHERE over a LEFT JOIN may reject the null-extended rows
                }
            }
            for e in residual.map(Expr::conjuncts).unwrap_or_default() {
                if covered(&e, &rc) {
                    to_right.push(e); // ON conditions on the inner side filter it first
                } else if kind == JoinKind::Inner && covered(&e, &lc) {
                    to_left.push(e);
                } else {
                    on.push(e);
                }
            }
            let mut residual = Vec::new();
            for e in on {
                match equi_pair(&e, &lc, &rc) {
                    Some(pair) => equi.push(pair),
                    None => residual.push(e),
                }
            }
            let join = Plan::Join {
                kind,
                left: Box::new(pushdown(*left, to_left)),
                right: Box::new(pushdown(*right, to_right)),
                equi,
                residual: Expr::conjoin(residual),
            };
            wrap(join, above)
        }
        Plan::Project {
            input,
            exprs,
            schema,
        } => {
            // Predicates over plain column renames go below the projection.
            let mut below = Vec::new();
            let mut above = Vec::new();
            for e in preds {
                let mut ok = true;
                let mapped = e.clone().transform(&mut |x| match x {
                    Expr::Column(c) => match schema.iter().position(|f| f.id == c) {
                        Some(i) => match &exprs[i] {
                            Expr::Column(src) => Expr::Column(*src),
                            _ => {
                                ok = false;
                                x
                            }
                        },
                        None => x,
                    },
                    x => x,
                });
                if ok {
                    below.push(mapped)
                } else {
                    above.push(e)
                }
            }
            wrap(
                Plan::Project {
                    input: Box::new(pushdown(*input, below)),
                    exprs,
                    schema,
                },
                above,
            )
        }
        Plan::Limit {
            input,
            limit,
            offset,
        } => {
            let inner = pushdown(*input, Vec::new());
            let inner = match (inner, limit) {
                // LIMIT straight over an unfiltered scan reads only what it needs.
                (
                    Plan::Scan {
                        table,
                        alias,
                        cols,
                        access,
                        filter: None,
                        limit: None,
                    },
                    Some(l),
                ) => Plan::Scan {
                    table,
                    alias,
                    cols,
                    access,
                    filter: None,
                    limit: Some(l + offset),
                },
                (
                    Plan::Project {
                        input,
                        exprs,
                        schema,
                    },
                    Some(l),
                ) => match *input {
                    Plan::Scan {
                        table,
                        alias,
                        cols,
                        access,
                        filter: None,
                        limit: None,
                    } => Plan::Project {
                        input: Box::new(Plan::Scan {
                            table,
                            alias,
                            cols,
                            access,
                            filter: None,
                            limit: Some(l + offset),
                        }),
                        exprs,
                        schema,
                    },
                    other => Plan::Project {
                        input: Box::new(other),
                        exprs,
                        schema,
                    },
                },
                (p, _) => p,
            };
            wrap(
                Plan::Limit {
                    input: Box::new(inner),
                    limit,
                    offset,
                },
                preds,
            )
        }
        Plan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => wrap(
            Plan::Aggregate {
                input: Box::new(pushdown(*input, Vec::new())),
                group,
                aggs,
                schema,
            },
            preds,
        ),
        Plan::Sort { input, keys } => Plan::Sort {
            input: Box::new(pushdown(*input, preds)),
            keys,
        },
        Plan::Distinct { input } => Plan::Distinct {
            input: Box::new(pushdown(*input, preds)),
        },
        p @ Plan::Values { .. } => wrap(p, preds),
    }
}

fn wrap(p: Plan, preds: Vec<Expr>) -> Plan {
    match Expr::conjoin(preds) {
        Some(pred) => Plan::Filter {
            input: Box::new(p),
            pred,
        },
        None => p,
    }
}

/// `l = r` with one side over the left input and the other over the right.
fn equi_pair(e: &Expr, lc: &[ColId], rc: &[ColId]) -> Option<(Expr, Expr)> {
    if let Expr::Binary(BinOp::Eq, a, b) = e {
        if covered(a, lc)
            && covered(b, rc)
            && !a.column_set().is_empty()
            && !b.column_set().is_empty()
        {
            return Some(((**a).clone(), (**b).clone()));
        }
        if covered(b, lc)
            && covered(a, rc)
            && !a.column_set().is_empty()
            && !b.column_set().is_empty()
        {
            return Some(((**b).clone(), (**a).clone()));
        }
    }
    None
}

// ---------------------------------------------------------------- join order

/// Flatten a tree of inner joins into relations and the predicates between them.
fn flatten(p: Plan, rels: &mut Vec<Plan>, preds: &mut Vec<Expr>) {
    match p {
        Plan::Join {
            kind: JoinKind::Inner,
            left,
            right,
            equi,
            residual,
        } => {
            flatten(*left, rels, preds);
            flatten(*right, rels, preds);
            for (a, b) in equi {
                preds.push(Expr::Binary(BinOp::Eq, Box::new(a), Box::new(b)));
            }
            if let Some(r) = residual {
                preds.extend(r.conjuncts());
            }
        }
        other => rels.push(reorder(other)),
    }
}

fn reorder(p: Plan) -> Plan {
    match p {
        j @ Plan::Join {
            kind: JoinKind::Inner,
            ..
        } => {
            let (mut rels, mut preds) = (Vec::new(), Vec::new());
            flatten(j, &mut rels, &mut preds);
            if rels.len() > 10 {
                // Too many for the exhaustive search: keep the written order.
                return build_left_deep(rels, preds);
            }
            best_order(rels, preds)
        }
        Plan::Join {
            kind,
            left,
            right,
            equi,
            residual,
        } => Plan::Join {
            kind,
            left: Box::new(reorder(*left)),
            right: Box::new(reorder(*right)),
            equi,
            residual,
        },
        Plan::Filter { input, pred } => Plan::Filter {
            input: Box::new(reorder(*input)),
            pred,
        },
        Plan::Project {
            input,
            exprs,
            schema,
        } => Plan::Project {
            input: Box::new(reorder(*input)),
            exprs,
            schema,
        },
        Plan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => Plan::Aggregate {
            input: Box::new(reorder(*input)),
            group,
            aggs,
            schema,
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: Box::new(reorder(*input)),
            keys,
        },
        Plan::Limit {
            input,
            limit,
            offset,
        } => Plan::Limit {
            input: Box::new(reorder(*input)),
            limit,
            offset,
        },
        Plan::Distinct { input } => Plan::Distinct {
            input: Box::new(reorder(*input)),
        },
        p => p,
    }
}

fn build_left_deep(rels: Vec<Plan>, preds: Vec<Expr>) -> Plan {
    let mut it = rels.into_iter();
    let first = it.next().unwrap_or(Plan::Values {
        rows: vec![vec![]],
        schema: vec![],
    });
    let j = it.fold(first, |l, r| Plan::Join {
        kind: JoinKind::Inner,
        left: Box::new(l),
        right: Box::new(r),
        equi: vec![],
        residual: None,
    });
    pushdown(j, preds)
}

/// Exhaustive search over subsets: for each set of relations, the cheapest way to
/// join it from two smaller sets connected by a predicate. Cost = sum of
/// intermediate result sizes (a proxy for the work of building and probing).
fn best_order(rels: Vec<Plan>, preds: Vec<Expr>) -> Plan {
    let n = rels.len();
    let cols: Vec<Vec<ColId>> = rels.iter().map(cols_of).collect();
    let mask_of = |e: &Expr| -> u32 {
        let mut m = 0;
        for c in e.column_set() {
            if let Some(i) = cols.iter().position(|cs| cs.contains(&c)) {
                m |= 1 << i;
            }
        }
        m
    };
    let pmask: Vec<u32> = preds.iter().map(mask_of).collect();
    // best[set] = (cost, rows, plan)
    let mut best: BTreeMap<u32, (f64, f64, Plan)> = BTreeMap::new();
    for (i, r) in rels.iter().enumerate() {
        best.insert(1 << i, (0.0, rows(r), r.clone()));
    }
    let full = (1u32 << n) - 1;
    for set in 1..=full {
        if set.count_ones() < 2 {
            continue;
        }
        let mut choice: Option<(f64, f64, Plan)> = None;
        // Enumerate proper subsets `a` of `set` (and b = set - a), each split once.
        let mut a = (set - 1) & set;
        while a > 0 {
            let b = set & !a;
            if a < b {
                a = (a - 1) & set;
                continue;
            }
            let connected = pmask
                .iter()
                .any(|m| m & a != 0 && m & b != 0 && m & !set == 0);
            if let (Some(pa), Some(pb)) = (best.get(&a), best.get(&b))
                && (connected || !has_connected_split(set, &pmask))
            {
                // Build on the smaller side (the right input of the hash join).
                let (big, small) = if pa.1 >= pb.1 { (pa, pb) } else { (pb, pa) };
                let ps: Vec<Expr> = preds
                    .iter()
                    .zip(&pmask)
                    .filter(|(_, m)| **m & set == **m && **m & a != 0 && **m & b != 0)
                    .map(|(e, _)| e.clone())
                    .collect();
                let j = pushdown(
                    Plan::Join {
                        kind: JoinKind::Inner,
                        left: Box::new(big.2.clone()),
                        right: Box::new(small.2.clone()),
                        equi: vec![],
                        residual: None,
                    },
                    ps,
                );
                let r = rows(&j);
                let cost = big.0 + small.0 + r + small.1;
                if choice.as_ref().is_none_or(|c| cost < c.0) {
                    choice = Some((cost, r, j));
                }
            }
            a = (a - 1) & set;
        }
        if let Some(c) = choice {
            best.insert(set, c);
        }
    }
    let mut plan = best
        .remove(&full)
        .map(|b| b.2)
        .unwrap_or_else(|| build_left_deep(rels, Vec::new()));
    // Predicates over a single relation were pushed already; any left over
    // (none expected) are applied on top.
    let used: Vec<Expr> = preds
        .iter()
        .zip(&pmask)
        .filter(|(_, m)| m.count_ones() == 0)
        .map(|(e, _)| e.clone())
        .collect();
    if let Some(f) = Expr::conjoin(used) {
        plan = Plan::Filter {
            input: Box::new(plan),
            pred: f,
        };
    }
    plan
}

fn has_connected_split(set: u32, pmask: &[u32]) -> bool {
    let mut a = (set - 1) & set;
    while a > 0 {
        let b = set & !a;
        if pmask
            .iter()
            .any(|m| m & a != 0 && m & b != 0 && m & !set == 0)
        {
            return true;
        }
        a = (a - 1) & set;
    }
    false
}

// ---------------------------------------------------------------- access paths

fn access_paths(p: Plan) -> Plan {
    match p {
        Plan::Scan {
            table,
            alias,
            cols,
            filter,
            limit,
            ..
        } => {
            let access = match &filter {
                Some(f) => choose_access(&table, &cols, f),
                None => Access::full(),
            };
            Plan::Scan {
                table,
                alias,
                cols,
                access,
                filter,
                limit,
            }
        }
        Plan::Filter { input, pred } => Plan::Filter {
            input: Box::new(access_paths(*input)),
            pred,
        },
        Plan::Project {
            input,
            exprs,
            schema,
        } => Plan::Project {
            input: Box::new(access_paths(*input)),
            exprs,
            schema,
        },
        Plan::Join {
            kind,
            left,
            right,
            equi,
            residual,
        } => Plan::Join {
            kind,
            left: Box::new(access_paths(*left)),
            right: Box::new(access_paths(*right)),
            equi,
            residual,
        },
        Plan::Aggregate {
            input,
            group,
            aggs,
            schema,
        } => Plan::Aggregate {
            input: Box::new(access_paths(*input)),
            group,
            aggs,
            schema,
        },
        Plan::Sort { input, keys } => Plan::Sort {
            input: Box::new(access_paths(*input)),
            keys,
        },
        Plan::Limit {
            input,
            limit,
            offset,
        } => Plan::Limit {
            input: Box::new(access_paths(*input)),
            limit,
            offset,
        },
        Plan::Distinct { input } => Plan::Distinct {
            input: Box::new(access_paths(*input)),
        },
        p => p,
    }
}

/// What the conjuncts say about one column: equal to a constant, or bounded.
#[derive(Default, Clone)]
struct Bounds {
    eq: Option<Value>,
    lower: Option<(Value, bool)>,
    upper: Option<(Value, bool)>,
}

fn column_bounds(f: &Expr, cols: &[ColId]) -> Vec<Bounds> {
    let mut b = vec![Bounds::default(); cols.len()];
    for c in f.clone().conjuncts() {
        let Expr::Binary(op, l, r) = &c else { continue };
        let (col, v, op) = match (l.as_ref(), r.as_ref()) {
            (Expr::Column(c), Expr::Literal(v, _)) => (*c, v, *op),
            (Expr::Literal(v, _), Expr::Column(c)) => (*c, v, op.flip()),
            _ => continue,
        };
        if v.is_null() {
            continue;
        }
        let Some(i) = cols.iter().position(|x| *x == col) else {
            continue;
        };
        let x = &mut b[i];
        match op {
            BinOp::Eq => x.eq = Some(v.clone()),
            BinOp::Gt => x.lower = Some((v.clone(), false)),
            BinOp::GtEq => x.lower = Some((v.clone(), true)),
            BinOp::Lt => x.upper = Some((v.clone(), false)),
            BinOp::LtEq => x.upper = Some((v.clone(), true)),
            _ => {}
        }
    }
    b
}

fn choose_access(table: &Table, cols: &[ColId], filter: &Expr) -> Access {
    let bounds = column_bounds(filter, cols);
    let ndv = |i: usize| -> f64 { ndv_in(table, cols, cols[i]) };
    let total = table
        .stats
        .as_ref()
        .map_or(1000.0, |s| s.rows.max(1) as f64);
    let mut best = (total, Access::full());
    let mut candidates: Vec<(u32, Vec<usize>, bool)> = vec![(PRIMARY, table.pk.clone(), false)];
    candidates.extend(
        table
            .indexes
            .iter()
            // Only public indexes are readable; write-only ones are being built.
            .filter(|ix| ix.public)
            .map(|ix| (ix.id, ix.columns.clone(), true)),
    );
    for (index, key_cols, secondary) in candidates {
        let mut eq = Vec::new();
        let mut est = total;
        let mut lower = None;
        let mut upper = None;
        for &c in &key_cols {
            match &bounds[c].eq {
                Some(v) => {
                    eq.push(v.clone());
                    est /= ndv(c).max(1.0);
                }
                None => {
                    lower = bounds[c].lower.clone();
                    upper = bounds[c].upper.clone();
                    if lower.is_some() {
                        est /= 3.0;
                    }
                    if upper.is_some() {
                        est /= if lower.is_some() { 1.5 } else { 3.0 };
                    }
                    break;
                }
            }
        }
        if eq.is_empty() && lower.is_none() && upper.is_none() {
            continue;
        }
        // A secondary index pays one more read per row it finds.
        let cost = if secondary { est * 2.0 + 1.0 } else { est };
        if cost < best.0 {
            best = (
                cost,
                Access {
                    index,
                    eq,
                    lower,
                    upper,
                },
            );
        }
    }
    best.1
}
