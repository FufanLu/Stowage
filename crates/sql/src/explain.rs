//! `EXPLAIN` and `EXPLAIN ANALYZE` output.

use std::collections::HashMap;

use crate::exec::{NodeStat, span};
use crate::keycodec::PRIMARY;
use crate::optimizer;
use crate::plan::{ColId, Display, Expr, JoinKind, Plan};

fn names(p: &Plan, out: &mut HashMap<ColId, String>) {
    for f in p.schema() {
        let n = match (&f.qualifier, f.name.is_empty()) {
            (_, true) => format!("#{}", f.id.0),
            (Some(q), false) => format!("{q}.{}", f.name),
            (None, false) => f.name.clone(),
        };
        out.entry(f.id).or_insert(n);
    }
    for c in p.children() {
        names(c, out);
    }
}

fn hex(b: &[u8]) -> String {
    if b.is_empty() {
        return "∞".into();
    }
    b.iter()
        .map(|x| format!("{x:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

/// One line per node, indented by depth.
pub fn render(plan: &Plan, stats: Option<&[NodeStat]>) -> Vec<String> {
    let mut map = HashMap::new();
    names(plan, &mut map);
    let name = |c: ColId| map.get(&c).cloned().unwrap_or_else(|| format!("#{}", c.0));
    let mut lines = Vec::new();
    let mut next = 0usize;
    walk(plan, 0, &mut next, &name, stats, &mut lines);
    lines
}

fn walk(
    p: &Plan,
    depth: usize,
    next: &mut usize,
    name: &dyn Fn(ColId) -> String,
    stats: Option<&[NodeStat]>,
    out: &mut Vec<String>,
) {
    let id = *next;
    *next += 1;
    let e = |x: &Expr| {
        Display {
            expr: x,
            names: name,
        }
        .to_string()
    };
    let list = |xs: &[Expr]| xs.iter().map(e).collect::<Vec<_>>().join(", ");
    let mut line = match p {
        Plan::Scan {
            table,
            alias,
            access,
            filter,
            limit,
            ..
        } => {
            let how = if access.is_full() {
                "full scan".to_owned()
            } else {
                let ix = if access.index == PRIMARY {
                    "primary key".to_owned()
                } else {
                    format!(
                        "index {}",
                        table
                            .indexes
                            .iter()
                            .find(|i| i.id == access.index)
                            .map_or("?", |i| i.name.as_str())
                    )
                };
                let (s, en) = span(table, access);
                let mut parts = Vec::new();
                if !access.eq.is_empty() {
                    parts.push(format!(
                        "= ({})",
                        access
                            .eq
                            .iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                if let Some((v, incl)) = &access.lower {
                    parts.push(format!("{} {v}", if *incl { ">=" } else { ">" }));
                }
                if let Some((v, incl)) = &access.upper {
                    parts.push(format!("{} {v}", if *incl { "<=" } else { "<" }));
                }
                format!(
                    "{ix} {} · span [{}, {})",
                    parts.join(" "),
                    hex(&s),
                    hex(&en)
                )
            };
            let a = if *alias == table.name {
                String::new()
            } else {
                format!(" {alias}")
            };
            let mut s = format!("Scan {}{a} · {how}", table.name);
            if let Some(f) = filter {
                s.push_str(&format!(" · filter {}", e(f)));
            }
            if let Some(l) = limit {
                s.push_str(&format!(" · limit {l}"));
            }
            s
        }
        Plan::Values { rows, .. } => format!("Values ({} rows)", rows.len()),
        Plan::Filter { pred, .. } => format!("Filter {}", e(pred)),
        Plan::Project { exprs, .. } => format!("Project {}", list(exprs)),
        Plan::Join {
            kind,
            equi,
            residual,
            ..
        } => {
            let k = if *kind == JoinKind::Left { "left " } else { "" };
            let s = if equi.is_empty() {
                format!("NestedLoopJoin {k}")
            } else {
                format!(
                    "HashJoin {k}on {}",
                    equi.iter()
                        .map(|(a, b)| format!("{} = {}", e(a), e(b)))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                )
            };
            let mut s = s.trim_end().to_owned();
            if let Some(r) = residual {
                s.push_str(&format!(" · residual {}", e(r)));
            }
            s
        }
        Plan::Aggregate { group, aggs, .. } => {
            let a = aggs
                .iter()
                .map(|a| {
                    let arg = a.arg.as_ref().map_or_else(|| "*".to_owned(), e);
                    format!(
                        "{}({}{arg})",
                        format!("{:?}", a.func)
                            .to_lowercase()
                            .replace("countstar", "count"),
                        if a.distinct { "DISTINCT " } else { "" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            if group.is_empty() {
                format!("Aggregate {a}")
            } else {
                format!("HashAggregate by {} · {a}", list(group))
            }
        }
        Plan::Sort { keys, .. } => format!(
            "Sort {}",
            keys.iter()
                .map(|k| format!("{}{}", e(&k.expr), if k.desc { " DESC" } else { "" }))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Plan::Limit { limit, offset, .. } => format!(
            "Limit {}{}",
            limit.map_or("ALL".into(), |l| l.to_string()),
            if *offset > 0 {
                format!(" offset {offset}")
            } else {
                String::new()
            }
        ),
        Plan::Distinct { .. } => "Distinct".to_owned(),
    };
    line.push_str(&format!("  (rows≈{:.0}", optimizer::rows(p)));
    if let Some(s) = stats.and_then(|s| s.get(id)) {
        line.push_str(&format!(
            ", actual {} rows, {:.3} ms",
            s.rows,
            s.nanos as f64 / 1e6
        ));
        if s.kv_requests > 0 {
            line.push_str(&format!(", {} kv requests", s.kv_requests));
        }
    }
    line.push(')');
    out.push(format!(
        "{}{}{line}",
        "  ".repeat(depth),
        if depth > 0 { "→ " } else { "" }
    ));
    for c in p.children() {
        walk(c, depth + 1, next, name, stats, out);
    }
}
