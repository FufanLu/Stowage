//! Transactional anomaly detection on list-append histories, after Elle
//! (Kingsbury & Alvaro, VLDB 2020).
//!
//! Each key holds a list; transactions append unique values and read whole
//! lists. Because every read returns a prefix of the key's final list, the
//! *version order* of each key is directly observable, and so are three kinds of
//! dependency between committed transactions:
//!
//! - `ww`: T1 appended the element just before T2's in some key's list;
//! - `wr`: T2 read a list whose last element T1 appended;
//! - `rw` (anti-dependency): T1 read a list that T2 later extended.
//!
//! A cycle among these edges is an anomaly. Its class decides which isolation
//! levels forbid it (Adya):
//!
//! | cycle edges            | name     | forbidden by        |
//! |------------------------|----------|---------------------|
//! | only `ww`              | G0       | everything          |
//! | `ww` + `wr`            | G1c      | read committed and up |
//! | exactly one `rw`       | G-single | snapshot isolation and up |
//! | two or more `rw`       | G2       | serializable only (write skew) |

use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mop {
    Append(u64, u64),
    /// The list read, or `None` if the key was absent.
    Read(u64, Vec<u64>),
}

#[derive(Debug, Clone)]
pub struct Txn {
    pub id: usize,
    pub committed: bool,
    pub ops: Vec<Mop>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Edge {
    Ww,
    Wr,
    Rw,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Anomaly {
    /// A read that is not a prefix of the key's longest observed list.
    IncompatibleOrder { key: u64, a: Vec<u64>, b: Vec<u64> },
    /// A committed transaction read a value appended by an aborted one.
    G1a { reader: usize, key: u64, value: u64 },
    /// A value appeared that no transaction appended.
    Phantom { key: u64, value: u64 },
    Cycle {
        kind: &'static str,
        txns: Vec<usize>,
        edges: Vec<Edge>,
    },
}

#[derive(Debug, Default)]
pub struct Report {
    pub anomalies: Vec<Anomaly>,
    pub edges: BTreeMap<Edge, usize>,
    pub committed: usize,
}

impl Report {
    /// Anomalies not allowed under snapshot isolation (everything except G2).
    pub fn violates_si(&self) -> Vec<&Anomaly> {
        self.anomalies
            .iter()
            .filter(|a| !matches!(a, Anomaly::Cycle { kind: "G2", .. }))
            .collect()
    }
    pub fn violates_serializable(&self) -> Vec<&Anomaly> {
        self.anomalies.iter().collect()
    }
}

pub fn check(history: &[Txn]) -> Report {
    let mut report = Report::default();
    let committed: Vec<&Txn> = history.iter().filter(|t| t.committed).collect();
    report.committed = committed.len();
    // Who appended each (key, value)?
    let mut writer: HashMap<(u64, u64), (usize, bool)> = HashMap::new();
    for t in history {
        for op in &t.ops {
            if let Mop::Append(k, v) = op {
                writer.insert((*k, *v), (t.id, t.committed));
            }
        }
    }
    // Version order: the longest list read for each key; every read must be a prefix.
    let mut order: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for t in &committed {
        for op in &t.ops {
            if let Mop::Read(k, list) = op {
                let cur = order.entry(*k).or_default();
                if list.len() > cur.len() {
                    if !list.starts_with(cur) {
                        report.anomalies.push(Anomaly::IncompatibleOrder {
                            key: *k,
                            a: cur.clone(),
                            b: list.clone(),
                        });
                    }
                    *cur = list.clone();
                } else if !cur.starts_with(list) {
                    report.anomalies.push(Anomaly::IncompatibleOrder {
                        key: *k,
                        a: cur.clone(),
                        b: list.clone(),
                    });
                }
            }
        }
    }
    for t in &committed {
        for op in &t.ops {
            if let Mop::Read(k, list) = op {
                for v in list {
                    match writer.get(&(*k, *v)) {
                        None => report
                            .anomalies
                            .push(Anomaly::Phantom { key: *k, value: *v }),
                        Some((_, false)) => report.anomalies.push(Anomaly::G1a {
                            reader: t.id,
                            key: *k,
                            value: *v,
                        }),
                        _ => {}
                    }
                }
            }
        }
    }
    // Dependency graph over committed transactions.
    let mut graph: BTreeMap<usize, BTreeSet<(usize, Edge)>> = BTreeMap::new();
    let mut add = |from: usize, to: usize, e: Edge, report: &mut Report| {
        if from != to && graph.entry(from).or_default().insert((to, e)) {
            *report.edges.entry(e).or_default() += 1;
        }
    };
    let committed_writer =
        |k: u64, v: u64| writer.get(&(k, v)).filter(|(_, c)| *c).map(|(id, _)| *id);
    for (k, list) in &order {
        for w in list.windows(2) {
            if let (Some(a), Some(b)) = (committed_writer(*k, w[0]), committed_writer(*k, w[1])) {
                add(a, b, Edge::Ww, &mut report);
            }
        }
    }
    for t in &committed {
        for op in &t.ops {
            let Mop::Read(k, list) = op else { continue };
            if let Some(last) = list.last()
                && let Some(w) = committed_writer(*k, *last)
            {
                add(w, t.id, Edge::Wr, &mut report);
            }
            // The next version after what we saw was written by someone after us.
            if let Some(full) = order.get(k)
                && let Some(next) = full.get(list.len())
                && let Some(w) = committed_writer(*k, *next)
            {
                add(t.id, w, Edge::Rw, &mut report);
            }
        }
    }
    report.anomalies.extend(cycles(&graph));
    report
}

/// Strongly connected components (Tarjan), then the "least anomalous" cycle in
/// each: one avoiding `rw` edges if possible, else one with a single `rw`.
fn cycles(graph: &BTreeMap<usize, BTreeSet<(usize, Edge)>>) -> Vec<Anomaly> {
    let nodes: Vec<usize> = graph
        .iter()
        .flat_map(|(a, outs)| std::iter::once(*a).chain(outs.iter().map(|(b, _)| *b)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let sccs = tarjan(&nodes, graph);
    let mut out = Vec::new();
    for scc in sccs.into_iter().filter(|s| s.len() > 1) {
        let members: BTreeSet<usize> = scc.iter().copied().collect();
        let edges_in = |allowed: &dyn Fn(Edge) -> bool| -> BTreeMap<usize, Vec<(usize, Edge)>> {
            members
                .iter()
                .map(|&a| {
                    let outs = graph.get(&a).map_or_else(Vec::new, |o| {
                        o.iter()
                            .filter(|(b, e)| members.contains(b) && allowed(*e))
                            .copied()
                            .collect()
                    });
                    (a, outs)
                })
                .collect()
        };
        let no_rw = edges_in(&|e| e != Edge::Rw);
        if let Some(c) = find_cycle(&no_rw) {
            let kind = if c.1.iter().all(|e| *e == Edge::Ww) {
                "G0"
            } else {
                "G1c"
            };
            out.push(Anomaly::Cycle {
                kind,
                txns: c.0,
                edges: c.1,
            });
            continue;
        }
        // Exactly one rw edge: rw a→b, and b reaches a without rw.
        let mut single = None;
        'search: for &a in &members {
            for (b, e) in graph.get(&a).into_iter().flatten() {
                if *e == Edge::Rw
                    && members.contains(b)
                    && let Some((path, pe)) = path(&no_rw, *b, a)
                {
                    let mut txns = vec![a];
                    txns.extend(path);
                    let mut edges = vec![Edge::Rw];
                    edges.extend(pe);
                    single = Some(Anomaly::Cycle {
                        kind: "G-single",
                        txns,
                        edges,
                    });
                    break 'search;
                }
            }
        }
        match single {
            Some(a) => out.push(a),
            None => {
                let all = edges_in(&|_| true);
                if let Some(c) = find_cycle(&all) {
                    out.push(Anomaly::Cycle {
                        kind: "G2",
                        txns: c.0,
                        edges: c.1,
                    });
                }
            }
        }
    }
    out
}

fn tarjan(nodes: &[usize], graph: &BTreeMap<usize, BTreeSet<(usize, Edge)>>) -> Vec<Vec<usize>> {
    struct St {
        index: usize,
        idx: HashMap<usize, usize>,
        low: HashMap<usize, usize>,
        on: BTreeSet<usize>,
        stack: Vec<usize>,
        out: Vec<Vec<usize>>,
    }
    fn visit(v: usize, g: &BTreeMap<usize, BTreeSet<(usize, Edge)>>, s: &mut St) {
        s.idx.insert(v, s.index);
        s.low.insert(v, s.index);
        s.index += 1;
        s.stack.push(v);
        s.on.insert(v);
        for (w, _) in g.get(&v).into_iter().flatten() {
            if !s.idx.contains_key(w) {
                visit(*w, g, s);
                let lw = s.low[w];
                let lv = s.low.get_mut(&v).expect("visited");
                *lv = (*lv).min(lw);
            } else if s.on.contains(w) {
                let iw = s.idx[w];
                let lv = s.low.get_mut(&v).expect("visited");
                *lv = (*lv).min(iw);
            }
        }
        if s.low[&v] == s.idx[&v] {
            let mut comp = Vec::new();
            while let Some(w) = s.stack.pop() {
                s.on.remove(&w);
                comp.push(w);
                if w == v {
                    break;
                }
            }
            s.out.push(comp);
        }
    }
    let mut s = St {
        index: 0,
        idx: HashMap::new(),
        low: HashMap::new(),
        on: BTreeSet::new(),
        stack: Vec::new(),
        out: Vec::new(),
    };
    for &n in nodes {
        if !s.idx.contains_key(&n) {
            visit(n, graph, &mut s);
        }
    }
    s.out
}

/// BFS path from `from` to `to`: (nodes after `from`, edges).
fn path(
    g: &BTreeMap<usize, Vec<(usize, Edge)>>,
    from: usize,
    to: usize,
) -> Option<(Vec<usize>, Vec<Edge>)> {
    let mut prev: HashMap<usize, (usize, Edge)> = HashMap::new();
    let mut queue = std::collections::VecDeque::from([from]);
    let mut seen = BTreeSet::from([from]);
    while let Some(v) = queue.pop_front() {
        if v == to {
            let (mut nodes, mut edges) = (Vec::new(), Vec::new());
            let mut cur = to;
            while cur != from {
                let (p, e) = prev[&cur];
                nodes.push(cur);
                edges.push(e);
                cur = p;
            }
            nodes.push(from);
            nodes.reverse();
            edges.reverse();
            return Some((nodes, edges));
        }
        for (w, e) in g.get(&v).into_iter().flatten() {
            if seen.insert(*w) {
                prev.insert(*w, (v, *e));
                queue.push_back(*w);
            }
        }
    }
    None
}

fn find_cycle(g: &BTreeMap<usize, Vec<(usize, Edge)>>) -> Option<(Vec<usize>, Vec<Edge>)> {
    for (&a, outs) in g {
        for &(b, e) in outs {
            if let Some((mut nodes, mut edges)) = path(g, b, a) {
                nodes.insert(0, a);
                edges.insert(0, e);
                nodes.pop(); // `a` closes the cycle
                return Some((nodes, edges));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(id: usize, ops: Vec<Mop>) -> Txn {
        Txn {
            id,
            committed: true,
            ops,
        }
    }

    #[test]
    fn serial_history_is_clean() {
        let h = vec![
            t(1, vec![Mop::Append(1, 1)]),
            t(2, vec![Mop::Read(1, vec![1]), Mop::Append(1, 2)]),
            t(3, vec![Mop::Read(1, vec![1, 2])]),
        ];
        assert!(check(&h).anomalies.is_empty(), "{:?}", check(&h).anomalies);
    }

    #[test]
    fn write_skew_is_g2() {
        // T1 reads x=[] y=[], appends x; T2 reads x=[] y=[], appends y. Each
        // read misses the other's write: rw both ways.
        let h = vec![
            t(
                1,
                vec![
                    Mop::Read(1, vec![]),
                    Mop::Read(2, vec![]),
                    Mop::Append(1, 10),
                ],
            ),
            t(
                2,
                vec![
                    Mop::Read(1, vec![]),
                    Mop::Read(2, vec![]),
                    Mop::Append(2, 20),
                ],
            ),
            t(3, vec![Mop::Read(1, vec![10]), Mop::Read(2, vec![20])]),
        ];
        let r = check(&h);
        assert!(
            matches!(r.anomalies.as_slice(), [Anomaly::Cycle { kind: "G2", .. }]),
            "{:?}",
            r.anomalies
        );
        assert!(r.violates_si().is_empty());
        assert_eq!(r.violates_serializable().len(), 1);
    }

    #[test]
    fn lost_update_is_g_single() {
        // Both read x=[1] and append; T2's read missed T1's append, yet T1 read
        // before T2 too... T1: r x=[1], a x 2. T2: r x=[1], a x 3. Final [1,2,3].
        let h = vec![
            t(0, vec![Mop::Append(1, 1)]),
            t(1, vec![Mop::Read(1, vec![1]), Mop::Append(1, 2)]),
            t(2, vec![Mop::Read(1, vec![1]), Mop::Append(1, 3)]),
            t(3, vec![Mop::Read(1, vec![1, 2, 3])]),
        ];
        let r = check(&h);
        assert!(
            r.anomalies.iter().any(|a| matches!(
                a,
                Anomaly::Cycle {
                    kind: "G-single",
                    ..
                }
            )),
            "{:?}",
            r.anomalies
        );
        assert!(!r.violates_si().is_empty());
    }

    #[test]
    fn aborted_read_and_bad_order() {
        let h = vec![
            Txn {
                id: 1,
                committed: false,
                ops: vec![Mop::Append(1, 5)],
            },
            t(2, vec![Mop::Read(1, vec![5])]),
            t(3, vec![Mop::Append(2, 1), Mop::Append(2, 2)]),
            t(4, vec![Mop::Read(2, vec![1, 2])]),
            t(5, vec![Mop::Read(2, vec![2])]),
        ];
        let r = check(&h);
        assert!(r.anomalies.iter().any(|a| matches!(a, Anomaly::G1a { .. })));
        assert!(
            r.anomalies
                .iter()
                .any(|a| matches!(a, Anomaly::IncompatibleOrder { .. }))
        );
    }
}
