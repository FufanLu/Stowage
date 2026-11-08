//! Bound expressions and query plans.
//!
//! Every column a plan produces has a [`ColId`], unique within the statement.
//! Expressions refer to columns by id, not by position, so the optimizer can
//! reorder joins and push predicates around without rewriting them.

use std::fmt;

use crate::catalog::Table;
use crate::value::{DataType, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColId(pub u32);

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub id: ColId,
    pub name: String,
    /// The table (or alias) it came from, for qualified names (`t.a`).
    pub qualifier: Option<String>,
    pub ty: DataType,
    pub nullable: bool,
    /// Not part of `SELECT *` (the implicit `rowid`).
    pub hidden: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
}

impl BinOp {
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::NotEq | Self::Lt | Self::LtEq | Self::Gt | Self::GtEq
        )
    }

    pub fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Concat => "||",
        }
    }

    /// `a op b` ⇔ `b op' a`.
    pub fn flip(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            o => o,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Lower,
    Upper,
    Length,
    Abs,
    Round,
    Coalesce,
    Nullif,
    Now,
    Substr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(ColId),
    Literal(Value, DataType),
    Binary(BinOp, Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Neg(Box<Expr>),
    IsNull(Box<Expr>, bool),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `LIKE`/`ILIKE`, or with `regex` the POSIX operators `~`, `~*`, `!~`, `!~*`.
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        insensitive: bool,
        regex: bool,
    },
    Cast(Box<Expr>, DataType),
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    Func(Func, Vec<Expr>),
    /// The result of the i-th aggregate during binding; replaced by a column.
    AggRef(usize),
}

impl Expr {
    pub fn col(id: ColId) -> Self {
        Self::Column(id)
    }

    pub fn and(self, other: Self) -> Self {
        Self::Binary(BinOp::And, Box::new(self), Box::new(other))
    }

    /// Split `a AND b AND c` into its conjuncts.
    pub fn conjuncts(self) -> Vec<Expr> {
        match self {
            Self::Binary(BinOp::And, a, b) => {
                let mut v = a.conjuncts();
                v.extend(b.conjuncts());
                v
            }
            e => vec![e],
        }
    }

    pub fn conjoin(mut es: Vec<Expr>) -> Option<Expr> {
        let first = if es.is_empty() {
            return None;
        } else {
            es.remove(0)
        };
        Some(es.into_iter().fold(first, Expr::and))
    }

    /// Every column this expression reads.
    pub fn columns(&self, out: &mut Vec<ColId>) {
        match self {
            Self::Column(c) => out.push(*c),
            Self::Literal(..) | Self::AggRef(_) => {}
            Self::Binary(_, a, b) => {
                a.columns(out);
                b.columns(out);
            }
            Self::Not(e) | Self::Neg(e) | Self::IsNull(e, _) | Self::Cast(e, _) => e.columns(out),
            Self::InList { expr, list, .. } => {
                expr.columns(out);
                list.iter().for_each(|e| e.columns(out));
            }
            Self::Like { expr, pattern, .. } => {
                expr.columns(out);
                pattern.columns(out);
            }
            Self::Case {
                operand,
                whens,
                otherwise,
            } => {
                if let Some(o) = operand {
                    o.columns(out);
                }
                for (w, t) in whens {
                    w.columns(out);
                    t.columns(out);
                }
                if let Some(o) = otherwise {
                    o.columns(out);
                }
            }
            Self::Func(_, args) => args.iter().for_each(|e| e.columns(out)),
        }
    }

    pub fn column_set(&self) -> Vec<ColId> {
        let mut v = Vec::new();
        self.columns(&mut v);
        v.sort_unstable();
        v.dedup();
        v
    }

    /// The type of the result, given the types of the columns.
    pub fn ty(&self, col: &dyn Fn(ColId) -> DataType) -> DataType {
        match self {
            Self::Column(c) => col(*c),
            Self::Literal(_, t) | Self::Cast(_, t) => *t,
            Self::Binary(op, a, b) => match op {
                BinOp::Concat => DataType::Text,
                o if o.is_comparison() => DataType::Bool,
                BinOp::And | BinOp::Or => DataType::Bool,
                _ => match (a.ty(col), b.ty(col)) {
                    (DataType::Float, _) | (_, DataType::Float) => DataType::Float,
                    (DataType::Timestamp, _) => DataType::Timestamp,
                    _ => DataType::Int,
                },
            },
            Self::Not(_) | Self::IsNull(..) | Self::InList { .. } | Self::Like { .. } => {
                DataType::Bool
            }
            Self::Neg(e) => e.ty(col),
            Self::Case {
                whens, otherwise, ..
            } => whens
                .first()
                .map(|(_, t)| t.ty(col))
                .or_else(|| otherwise.as_ref().map(|o| o.ty(col)))
                .unwrap_or(DataType::Text),
            Self::Func(f, args) => match f {
                Func::Lower | Func::Upper | Func::Substr => DataType::Text,
                Func::Length => DataType::Int,
                Func::Abs | Func::Round | Func::Coalesce | Func::Nullif => {
                    args.first().map_or(DataType::Text, |a| a.ty(col))
                }
                Func::Now => DataType::Timestamp,
            },
            Self::AggRef(_) => DataType::Int,
        }
    }

    /// Replace sub-expressions bottom-up.
    pub fn transform(self, f: &mut dyn FnMut(Expr) -> Expr) -> Expr {
        let e = match self {
            Self::Binary(op, a, b) => {
                Self::Binary(op, Box::new(a.transform(f)), Box::new(b.transform(f)))
            }
            Self::Not(e) => Self::Not(Box::new(e.transform(f))),
            Self::Neg(e) => Self::Neg(Box::new(e.transform(f))),
            Self::IsNull(e, n) => Self::IsNull(Box::new(e.transform(f)), n),
            Self::Cast(e, t) => Self::Cast(Box::new(e.transform(f)), t),
            Self::InList {
                expr,
                list,
                negated,
            } => Self::InList {
                expr: Box::new(expr.transform(f)),
                list: list.into_iter().map(|e| e.transform(f)).collect(),
                negated,
            },
            Self::Like {
                expr,
                pattern,
                negated,
                insensitive,
                regex,
            } => Self::Like {
                expr: Box::new(expr.transform(f)),
                pattern: Box::new(pattern.transform(f)),
                negated,
                insensitive,
                regex,
            },
            Self::Case {
                operand,
                whens,
                otherwise,
            } => Self::Case {
                operand: operand.map(|o| Box::new(o.transform(f))),
                whens: whens
                    .into_iter()
                    .map(|(w, t)| (w.transform(f), t.transform(f)))
                    .collect(),
                otherwise: otherwise.map(|o| Box::new(o.transform(f))),
            },
            Self::Func(func, args) => {
                Self::Func(func, args.into_iter().map(|e| e.transform(f)).collect())
            }
            e => e,
        };
        f(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggCall {
    pub func: AggFunc,
    pub arg: Option<Expr>,
    pub distinct: bool,
}

impl AggCall {
    pub fn ty(&self, col: &dyn Fn(ColId) -> DataType) -> DataType {
        match self.func {
            AggFunc::Count | AggFunc::CountStar => DataType::Int,
            AggFunc::Avg => DataType::Float,
            AggFunc::Sum | AggFunc::Min | AggFunc::Max => {
                self.arg.as_ref().map_or(DataType::Int, |a| a.ty(col))
            }
        }
    }
}

/// How a scan reads its table.
#[derive(Debug, Clone, PartialEq)]
pub struct Access {
    /// `PRIMARY` or a secondary index id.
    pub index: u32,
    /// Equalities on the first columns of the index.
    pub eq: Vec<Value>,
    /// Bounds on the next column: (value, inclusive).
    pub lower: Option<(Value, bool)>,
    pub upper: Option<(Value, bool)>,
}

impl Access {
    pub fn full() -> Self {
        Self {
            index: crate::keycodec::PRIMARY,
            eq: Vec::new(),
            lower: None,
            upper: None,
        }
    }

    pub fn is_full(&self) -> bool {
        self.index == crate::keycodec::PRIMARY
            && self.eq.is_empty()
            && self.lower.is_none()
            && self.upper.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

/// (Plans are small trees built once per statement; boxing the scan's table
/// descriptor would not pay for itself.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Read a table; produces every column (`cols[i]` is column `i`).
    Scan {
        table: Table,
        alias: String,
        cols: Vec<ColId>,
        access: Access,
        filter: Option<Expr>,
        limit: Option<u64>,
    },
    Values {
        rows: Vec<Vec<Expr>>,
        schema: Vec<Field>,
    },
    Filter {
        input: Box<Plan>,
        pred: Expr,
    },
    Project {
        input: Box<Plan>,
        exprs: Vec<Expr>,
        schema: Vec<Field>,
    },
    /// `equi` pairs (left column expr, right column expr) drive a hash join; the
    /// rest of the condition is `residual`.
    Join {
        kind: JoinKind,
        left: Box<Plan>,
        right: Box<Plan>,
        equi: Vec<(Expr, Expr)>,
        residual: Option<Expr>,
    },
    Aggregate {
        input: Box<Plan>,
        group: Vec<Expr>,
        aggs: Vec<AggCall>,
        schema: Vec<Field>,
    },
    Sort {
        input: Box<Plan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<u64>,
        offset: u64,
    },
    Distinct {
        input: Box<Plan>,
    },
    /// `UNION ALL`: the inputs' rows one after another, matched by position.
    /// Each input already has `schema`'s types; `UNION` is `Distinct` over this.
    Union {
        inputs: Vec<Plan>,
        schema: Vec<Field>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: Expr,
    pub desc: bool,
    pub nulls_first: bool,
}

impl Plan {
    pub fn schema(&self) -> Vec<Field> {
        match self {
            Self::Scan {
                table, alias, cols, ..
            } => table
                .columns
                .iter()
                .zip(cols)
                .map(|(c, id)| Field {
                    id: *id,
                    name: c.name.clone(),
                    qualifier: Some(alias.clone()),
                    ty: c.ty,
                    nullable: c.nullable,
                    hidden: c.hidden,
                })
                .collect(),
            Self::Values { schema, .. }
            | Self::Project { schema, .. }
            | Self::Aggregate { schema, .. }
            | Self::Union { schema, .. } => schema.clone(),
            Self::Filter { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. }
            | Self::Distinct { input } => input.schema(),
            Self::Join {
                kind, left, right, ..
            } => {
                let mut s = left.schema();
                s.extend(right.schema().into_iter().map(|mut f| {
                    if *kind == JoinKind::Left {
                        f.nullable = true;
                    }
                    f
                }));
                s
            }
        }
    }

    pub fn children(&self) -> Vec<&Plan> {
        match self {
            Self::Scan { .. } | Self::Values { .. } => vec![],
            Self::Filter { input, .. }
            | Self::Project { input, .. }
            | Self::Aggregate { input, .. }
            | Self::Sort { input, .. }
            | Self::Limit { input, .. }
            | Self::Distinct { input } => vec![input],
            Self::Join { left, right, .. } => vec![left, right],
            Self::Union { inputs, .. } => inputs.iter().collect(),
        }
    }
}

/// Renders expressions with column names (for EXPLAIN).
#[allow(missing_debug_implementations)]
pub struct Display<'a> {
    pub expr: &'a Expr,
    pub names: &'a dyn Fn(ColId) -> String,
}

impl fmt::Display for Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self.names;
        let d = |e: &Expr| Display { expr: e, names }.to_string();
        match self.expr {
            Expr::Column(c) => write!(f, "{}", (self.names)(*c)),
            Expr::Literal(Value::Text(s), _) => write!(f, "'{s}'"),
            Expr::Literal(v, _) => write!(f, "{v}"),
            Expr::Binary(op, a, b) => write!(f, "({} {} {})", d(a), op.symbol(), d(b)),
            Expr::Not(e) => write!(f, "NOT {}", d(e)),
            Expr::Neg(e) => write!(f, "-{}", d(e)),
            Expr::IsNull(e, n) => write!(f, "{} IS {}NULL", d(e), if *n { "NOT " } else { "" }),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                write!(f, "{} {}IN (", d(expr), if *negated { "NOT " } else { "" })?;
                for (i, e) in list.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", d(e))?;
                }
                f.write_str(")")
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                insensitive,
                regex: true,
            } => {
                let op = match (negated, insensitive) {
                    (false, false) => "~",
                    (false, true) => "~*",
                    (true, false) => "!~",
                    (true, true) => "!~*",
                };
                write!(f, "{} {op} {}", d(expr), d(pattern))
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                insensitive,
                regex: false,
            } => {
                write!(
                    f,
                    "{} {}{} {}",
                    d(expr),
                    if *negated { "NOT " } else { "" },
                    if *insensitive { "ILIKE" } else { "LIKE" },
                    d(pattern)
                )
            }
            Expr::Cast(e, t) => write!(f, "CAST({} AS {t})", d(e)),
            Expr::Case { .. } => f.write_str("CASE …"),
            Expr::Func(func, args) => {
                write!(f, "{}(", format!("{func:?}").to_lowercase())?;
                for (i, e) in args.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{}", d(e))?;
                }
                f.write_str(")")
            }
            Expr::AggRef(i) => write!(f, "agg#{i}"),
        }
    }
}
