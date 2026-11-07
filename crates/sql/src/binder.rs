//! From the parser's AST to a [`Plan`]: name resolution, types, aggregation.

use sqlparser::ast;

use crate::catalog;
use crate::kv::{BoxFut, KvTxn};
use crate::plan::{AggCall, AggFunc, BinOp, ColId, Expr, Field, Func, JoinKind, Plan, SortKey};
use crate::value::{DataType, Value};
use crate::{Error, Result};

#[allow(missing_debug_implementations)]
pub struct Binder<'t> {
    pub txn: &'t mut dyn KvTxn,
    next_col: u32,
    /// `$1`, `$2`, … from the extended protocol, as text (NULL: `None`).
    pub params: Vec<Option<String>>,
    /// `now()`, fixed for the statement (microseconds since the epoch).
    pub now: i64,
    /// The authenticated user; `None` when authentication is off.
    pub user: Option<crate::auth::User>,
}

pub fn unsupported<T>(what: impl std::fmt::Display) -> Result<T> {
    Err(Error::Unsupported(what.to_string()))
}

pub fn ident(i: &ast::Ident) -> String {
    if i.quote_style.is_some() {
        i.value.clone()
    } else {
        i.value.to_lowercase()
    }
}

pub fn object_name(n: &ast::ObjectName) -> Result<String> {
    match n.0.last() {
        Some(ast::ObjectNamePart::Identifier(i)) => Ok(ident(i)),
        _ => unsupported(format!("name {n}")),
    }
}

pub fn data_type(t: &ast::DataType) -> Result<DataType> {
    use ast::DataType as T;
    Ok(match t {
        T::Bool | T::Boolean => DataType::Bool,
        T::SmallInt(_)
        | T::Int(_)
        | T::Integer(_)
        | T::BigInt(_)
        | T::Int2(_)
        | T::Int4(_)
        | T::Int8(_)
        | T::TinyInt(_) => DataType::Int,
        T::Real
        | T::Float4
        | T::Float8
        | T::Double(_)
        | T::DoublePrecision
        | T::Float(_)
        | T::Numeric(_)
        | T::Decimal(_)
        | T::Dec(_) => DataType::Float,
        T::Text
        | T::Varchar(_)
        | T::Char(_)
        | T::Character(_)
        | T::CharacterVarying(_)
        | T::CharVarying(_)
        | T::String(_)
        | T::Uuid => DataType::Text,
        T::JSON | T::JSONB => DataType::Text,
        T::Timestamp(..) | T::Datetime(_) | T::Date => DataType::Timestamp,
        T::Bytea | T::Blob(_) | T::Bytes(_) => DataType::Bytes,
        T::Custom(name, _) => match object_name(name)?.as_str() {
            "serial" | "bigserial" | "smallserial" | "serial4" | "serial8" => DataType::Int,
            "timestamptz" => DataType::Timestamp,
            other => return unsupported(format!("type {other}")),
        },
        other => return unsupported(format!("type {other}")),
    })
}

/// Columns visible to an expression: the input's schema.
pub type Scope = [Field];

fn find(scope: &Scope, qualifier: Option<&str>, name: &str) -> Result<Field> {
    let mut hits = scope
        .iter()
        .filter(|f| f.name == name && qualifier.is_none_or(|q| f.qualifier.as_deref() == Some(q)));
    match (hits.next(), hits.next()) {
        (Some(f), None) => Ok(f.clone()),
        (Some(_), Some(_)) => Err(Error::Ambiguous(name.to_owned())),
        (None, _) => Err(Error::UndefinedColumn(match qualifier {
            Some(q) => format!("{q}.{name}"),
            None => name.to_owned(),
        })),
    }
}

pub fn ty_in(scope: &Scope) -> impl Fn(ColId) -> DataType + '_ {
    move |c| {
        scope
            .iter()
            .find(|f| f.id == c)
            .map_or(DataType::Text, |f| f.ty)
    }
}

fn literal_text(s: &str) -> Expr {
    Expr::Literal(Value::Text(s.to_owned()), DataType::Text)
}

/// Make the two sides of a comparison or arithmetic agree on a type: a literal is
/// converted to the other side's type; an integer side is widened to float.
fn unify(a: Expr, b: Expr, scope: &Scope) -> Result<(Expr, Expr)> {
    let ty = ty_in(scope);
    let (ta, tb) = (a.ty(&ty), b.ty(&ty));
    if ta == tb {
        return Ok((a, b));
    }
    let conv = |e: Expr, to: DataType| -> Result<Expr> {
        match e {
            Expr::Literal(v, _) => Ok(Expr::Literal(v.coerce(to)?, to)),
            e => Ok(Expr::Cast(Box::new(e), to)),
        }
    };
    match (&a, &b, ta, tb) {
        (Expr::Literal(Value::Null, _), _, _, _) => Ok((Expr::Literal(Value::Null, tb), b)),
        (_, Expr::Literal(Value::Null, _), _, _) => Ok((a, Expr::Literal(Value::Null, ta))),
        // Numbers widen (7.0 / 2 is 3.5); an untyped string takes the other side's type.
        (_, _, DataType::Int, DataType::Float) => Ok((conv(a, DataType::Float)?, b)),
        (_, _, DataType::Float, DataType::Int) => Ok((a, conv(b, DataType::Float)?)),
        (Expr::Literal(..), _, DataType::Text, _) => Ok((conv(a, tb)?, b)),
        (_, Expr::Literal(..), _, DataType::Text) => Ok((a, conv(b, ta)?)),
        (Expr::Literal(..), _, _, _) => Ok((conv(a, tb)?, b)),
        (_, Expr::Literal(..), _, _) => Ok((a, conv(b, ta)?)),
        (_, _, DataType::Text, _) | (_, _, _, DataType::Text) => {
            Ok((conv(a, DataType::Text)?, conv(b, DataType::Text)?))
        }
        _ => Err(Error::InvalidInput(format!(
            "operator does not exist: {ta} and {tb}"
        ))),
    }
}

impl<'t> Binder<'t> {
    pub fn new(txn: &'t mut dyn KvTxn, params: Vec<Option<String>>, now: i64) -> Self {
        Self {
            txn,
            next_col: 1,
            params,
            now,
            user: None,
        }
    }

    pub fn col_id(&mut self) -> ColId {
        let c = ColId(self.next_col);
        self.next_col += 1;
        c
    }

    pub fn new_cols(&mut self, n: usize) -> Vec<ColId> {
        (0..n).map(|_| self.col_id()).collect()
    }

    /// Fail unless the session's user holds `privilege` on `table`.
    pub fn require(&self, table: &str, privilege: &str) -> Result<()> {
        match &self.user {
            Some(u) if !u.may(table, privilege) => {
                crate::audit::record(
                    "permission_denied",
                    &u.name,
                    &format!("{privilege} on {table}"),
                );
                Err(Error::PermissionDenied(format!("for table {table}")))
            }
            _ => Ok(()),
        }
    }

    /// A scan of `name` under `alias`. Every read of a table comes through
    /// here, subqueries and the WHERE of UPDATE and DELETE included, so this is
    /// where SELECT is checked.
    pub async fn scan(&mut self, name: &str, alias: Option<String>) -> Result<Plan> {
        self.require(name, "SELECT")?;
        let table = catalog::table(self.txn, name).await?;
        let cols = self.new_cols(table.columns.len());
        Ok(Plan::Scan {
            alias: alias.unwrap_or_else(|| name.to_owned()),
            cols,
            table,
            access: crate::plan::Access::full(),
            filter: None,
            limit: None,
        })
    }

    pub fn bind_query<'a>(&'a mut self, q: &'a ast::Query) -> BoxFut<'a, Result<Plan>> {
        Box::pin(async move {
            if q.with.is_some() {
                return unsupported("WITH");
            }
            let (limit, offset) = match &q.limit_clause {
                None => (None, 0),
                Some(ast::LimitClause::LimitOffset { limit, offset, .. }) => (
                    match limit {
                        Some(e) => Some(self.const_u64(e)?),
                        None => None,
                    },
                    match offset {
                        Some(o) => self.const_u64(&o.value)?,
                        None => 0,
                    },
                ),
                Some(ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
                    (Some(self.const_u64(limit)?), self.const_u64(offset)?)
                }
            };
            let order: Vec<ast::OrderByExpr> = match &q.order_by {
                None => Vec::new(),
                Some(ob) => match &ob.kind {
                    ast::OrderByKind::Expressions(v) => v.clone(),
                    ast::OrderByKind::All(_) => return unsupported("ORDER BY ALL"),
                },
            };
            let plan = match q.body.as_ref() {
                ast::SetExpr::Select(s) => self.bind_select(s, &order).await?,
                ast::SetExpr::Values(v) => {
                    let p = self.bind_values(v, None).await?;
                    self.order_output(p, &order).await?
                }
                ast::SetExpr::Query(inner) => {
                    let p = self.bind_query(inner).await?;
                    self.order_output(p, &order).await?
                }
                other => return unsupported(format!("query {other}")),
            };
            Ok(if limit.is_some() || offset > 0 {
                Plan::Limit {
                    input: Box::new(plan),
                    limit,
                    offset,
                }
            } else {
                plan
            })
        })
    }

    fn const_u64(&self, e: &ast::Expr) -> Result<u64> {
        match e {
            ast::Expr::Value(v) => match &v.value {
                ast::Value::Number(n, _) => n
                    .parse()
                    .map_err(|_| Error::InvalidInput(format!("bad LIMIT {n}"))),
                ast::Value::Placeholder(p) => self
                    .param(p)?
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| Error::InvalidInput("bad LIMIT parameter".into())),
                _ => unsupported(format!("LIMIT {e}")),
            },
            _ => unsupported(format!("LIMIT {e}")),
        }
    }

    fn param(&self, p: &str) -> Result<Option<String>> {
        let n: usize = p
            .trim_start_matches('$')
            .parse()
            .map_err(|_| Error::InvalidInput(format!("bad parameter {p}")))?;
        self.params
            .get(n.wrapping_sub(1))
            .cloned()
            .ok_or_else(|| Error::InvalidInput(format!("no value for parameter {p}")))
    }

    /// `VALUES (...), (...)`, optionally typed by `types`.
    pub async fn bind_values(
        &mut self,
        v: &ast::Values,
        types: Option<&[DataType]>,
    ) -> Result<Plan> {
        let mut rows = Vec::new();
        for r in &v.rows {
            let mut out = Vec::new();
            for e in &r.content {
                out.push(self.bind_expr(e, &[], &mut None).await?);
            }
            rows.push(out);
        }
        let width = rows.first().map_or(0, Vec::len);
        if rows.iter().any(|r| r.len() != width) {
            return Err(Error::InvalidInput(
                "VALUES lists must all be the same length".into(),
            ));
        }
        let none = |_| DataType::Text;
        let mut schema = Vec::new();
        for i in 0..width {
            let ty = match types {
                Some(t) => t[i],
                None => rows
                    .iter()
                    .map(|r| r[i].ty(&none))
                    .find(|t| *t != DataType::Text)
                    .unwrap_or_else(|| rows[0][i].ty(&none)),
            };
            for r in &mut rows {
                if let Expr::Literal(v, t) = &r[i]
                    && *t != ty
                {
                    r[i] = Expr::Literal(v.clone().coerce(ty)?, ty);
                }
            }
            schema.push(Field {
                id: self.col_id(),
                name: format!("column{}", i + 1),
                qualifier: None,
                ty,
                nullable: true,
                hidden: false,
            });
        }
        Ok(Plan::Values { rows, schema })
    }

    async fn bind_from(&mut self, from: &[ast::TableWithJoins]) -> Result<Plan> {
        let mut plan: Option<Plan> = None;
        for twj in from {
            let mut p = self.bind_factor(&twj.relation).await?;
            for j in &twj.joins {
                let right = self.bind_factor(&j.relation).await?;
                let (kind, constraint) = match &j.join_operator {
                    ast::JoinOperator::Join(c) | ast::JoinOperator::Inner(c) => {
                        (JoinKind::Inner, Some(c))
                    }
                    ast::JoinOperator::Left(c) | ast::JoinOperator::LeftOuter(c) => {
                        (JoinKind::Left, Some(c))
                    }
                    ast::JoinOperator::CrossJoin(_) => (JoinKind::Inner, None),
                    other => return unsupported(format!("join {other:?}")),
                };
                let mut scope = p.schema();
                scope.extend(right.schema());
                let cond = match constraint {
                    Some(ast::JoinConstraint::On(e)) => Some(self.bind_bool(e, &scope).await?),
                    Some(ast::JoinConstraint::Using(cols)) => {
                        let (ls, rs) = (p.schema(), right.schema());
                        let mut conds = Vec::new();
                        for c in cols {
                            let n = object_name(c)?;
                            let (l, r) = (find(&ls, None, &n)?, find(&rs, None, &n)?);
                            conds.push(Expr::Binary(
                                BinOp::Eq,
                                Box::new(Expr::Column(l.id)),
                                Box::new(Expr::Column(r.id)),
                            ));
                        }
                        Expr::conjoin(conds)
                    }
                    Some(ast::JoinConstraint::None) | None => None,
                    Some(ast::JoinConstraint::Natural) => return unsupported("NATURAL JOIN"),
                };
                p = Plan::Join {
                    kind,
                    left: Box::new(p),
                    right: Box::new(right),
                    equi: Vec::new(),
                    residual: cond,
                };
            }
            plan = Some(match plan {
                None => p,
                Some(l) => Plan::Join {
                    kind: JoinKind::Inner,
                    left: Box::new(l),
                    right: Box::new(p),
                    equi: Vec::new(),
                    residual: None,
                },
            });
        }
        Ok(plan.unwrap_or(Plan::Values {
            rows: vec![vec![]],
            schema: vec![],
        }))
    }

    fn bind_factor<'a>(&'a mut self, f: &'a ast::TableFactor) -> BoxFut<'a, Result<Plan>> {
        Box::pin(async move {
            match f {
                ast::TableFactor::Table {
                    name,
                    alias,
                    args: None,
                    ..
                } => {
                    let n = object_name(name)?;
                    if let Some(p) = self.system_table(&n).await? {
                        return Ok(p);
                    }
                    self.scan(&n, alias.as_ref().map(|a| ident(&a.name))).await
                }
                ast::TableFactor::Derived {
                    subquery, alias, ..
                } => {
                    let p = self.bind_query(subquery).await?;
                    let Some(alias) = alias else {
                        return Ok(p);
                    };
                    let q = ident(&alias.name);
                    let mut schema = p.schema();
                    let renames: Vec<String> =
                        alias.columns.iter().map(|c| ident(&c.name)).collect();
                    for (i, f) in schema.iter_mut().enumerate() {
                        f.qualifier = Some(q.clone());
                        if let Some(n) = renames.get(i) {
                            f.name = n.clone();
                        }
                    }
                    let exprs = schema.iter().map(|f| Expr::Column(f.id)).collect();
                    Ok(Plan::Project {
                        input: Box::new(p),
                        exprs,
                        schema,
                    })
                }
                ast::TableFactor::NestedJoin {
                    table_with_joins, ..
                } => self.bind_from(std::slice::from_ref(table_with_joins)).await,
                other => unsupported(format!("FROM {other}")),
            }
        })
    }

    /// `information_schema` and `pg_catalog` views clients ask for.
    async fn system_table(&mut self, name: &str) -> Result<Option<Plan>> {
        let tables = match name {
            "tables" | "pg_tables" | "columns" => catalog::list_tables(self.txn).await?,
            _ => return Ok(None),
        };
        let text = |s: &str| Expr::Literal(Value::Text(s.to_owned()), DataType::Text);
        let (names, rows): (Vec<&str>, Vec<Vec<Expr>>) = match name {
            "tables" => (
                vec!["table_schema", "table_name", "table_type"],
                tables
                    .iter()
                    .map(|t| vec![text("public"), text(&t.name), text("BASE TABLE")])
                    .collect(),
            ),
            "pg_tables" => (
                vec!["schemaname", "tablename"],
                tables
                    .iter()
                    .map(|t| vec![text("public"), text(&t.name)])
                    .collect(),
            ),
            _ => (
                vec![
                    "table_schema",
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "data_type",
                    "is_nullable",
                ],
                tables
                    .iter()
                    .flat_map(|t| {
                        t.visible_columns().map(move |(i, c)| {
                            vec![
                                text("public"),
                                text(&t.name),
                                text(&c.name),
                                Expr::Literal(Value::Int(i as i64 + 1), DataType::Int),
                                text(c.ty.name()),
                                text(if c.nullable { "YES" } else { "NO" }),
                            ]
                        })
                    })
                    .collect(),
            ),
        };
        let schema = names
            .iter()
            .enumerate()
            .map(|(i, n)| Field {
                id: self.col_id(),
                name: (*n).to_owned(),
                qualifier: Some(name.to_owned()),
                ty: if name == "columns" && i == 3 {
                    DataType::Int
                } else {
                    DataType::Text
                },
                nullable: false,
                hidden: false,
            })
            .collect();
        Ok(Some(Plan::Values { rows, schema }))
    }

    async fn bind_select(&mut self, s: &ast::Select, order: &[ast::OrderByExpr]) -> Result<Plan> {
        let mut plan = self.bind_from(&s.from).await?;
        let input_scope = plan.schema();
        if let Some(w) = &s.selection {
            let pred = self.bind_bool(w, &input_scope).await?;
            plan = Plan::Filter {
                input: Box::new(plan),
                pred,
            };
        }
        // Projection items, expanded.
        let mut items: Vec<(Expr, String)> = Vec::new();
        let mut aggs: Option<Vec<AggCall>> = Some(Vec::new());
        for item in &s.projection {
            match item {
                ast::SelectItem::Wildcard(_) => {
                    for f in input_scope.iter().filter(|f| !f.hidden) {
                        items.push((Expr::Column(f.id), f.name.clone()));
                    }
                }
                ast::SelectItem::QualifiedWildcard(kind, _) => {
                    let q = match kind {
                        ast::SelectItemQualifiedWildcardKind::ObjectName(n) => object_name(n)?,
                        ast::SelectItemQualifiedWildcardKind::Expr(e) => {
                            return unsupported(format!("{e}.*"));
                        }
                    };
                    let mut any = false;
                    for f in input_scope
                        .iter()
                        .filter(|f| !f.hidden && f.qualifier.as_deref() == Some(q.as_str()))
                    {
                        items.push((Expr::Column(f.id), f.name.clone()));
                        any = true;
                    }
                    if !any {
                        return Err(Error::UndefinedTable(q));
                    }
                }
                ast::SelectItem::UnnamedExpr(e) => {
                    let b = self.bind_expr(e, &input_scope, &mut aggs).await?;
                    items.push((b, output_name(e)));
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let b = self.bind_expr(expr, &input_scope, &mut aggs).await?;
                    items.push((b, ident(alias)));
                }
                other => return unsupported(format!("select item {other}")),
            }
        }
        let group: Vec<ast::Expr> = match &s.group_by {
            ast::GroupByExpr::Expressions(v, _) => v.clone(),
            ast::GroupByExpr::All(_) => return unsupported("GROUP BY ALL"),
        };
        let mut having = match &s.having {
            Some(h) => Some(self.bind_expr(h, &input_scope, &mut aggs).await?),
            None => None,
        };
        // ORDER BY items that are not output names or positions bind against the
        // input, and may be aggregates too.
        let mut order_bound: Vec<(OrderTarget, bool, bool)> = Vec::new();
        for o in order {
            let desc = matches!(o.options.sort, Some(ast::OrderBySort::Desc));
            let nulls_first = o.options.nulls_first.unwrap_or(desc);
            let target = match &o.expr {
                ast::Expr::Value(v) if matches!(&v.value, ast::Value::Number(..)) => {
                    let n = self.const_u64(&o.expr)? as usize;
                    if n == 0 || n > items.len() {
                        return Err(Error::InvalidInput(format!(
                            "ORDER BY position {n} is not in select list"
                        )));
                    }
                    OrderTarget::Output(n - 1)
                }
                ast::Expr::Identifier(i)
                    if items.iter().any(|(_, n)| *n == ident(i))
                        && !input_scope.iter().any(|f| f.name == ident(i)) =>
                {
                    OrderTarget::Output(items.iter().position(|(_, n)| *n == ident(i)).unwrap_or(0))
                }
                e => OrderTarget::Expr(self.bind_expr(e, &input_scope, &mut aggs).await?),
            };
            order_bound.push((target, desc, nulls_first));
        }
        let aggs = aggs.unwrap_or_default();
        if !aggs.is_empty() || !group.is_empty() {
            let mut group_exprs = Vec::new();
            for g in &group {
                // GROUP BY may name an output column or a position.
                let b = match g {
                    ast::Expr::Value(v) if matches!(&v.value, ast::Value::Number(..)) => {
                        let n = self.const_u64(g)? as usize;
                        items
                            .get(n.wrapping_sub(1))
                            .map(|(e, _)| e.clone())
                            .ok_or_else(|| {
                                Error::InvalidInput(format!(
                                    "GROUP BY position {n} is not in select list"
                                ))
                            })?
                    }
                    ast::Expr::Identifier(i) if !input_scope.iter().any(|f| f.name == ident(i)) => {
                        match items.iter().find(|(_, n)| *n == ident(i)) {
                            Some((e, _)) => e.clone(),
                            None => self.bind_expr(g, &input_scope, &mut None).await?,
                        }
                    }
                    _ => self.bind_expr(g, &input_scope, &mut None).await?,
                };
                group_exprs.push(b);
            }
            let ty = ty_in(&input_scope);
            let mut schema = Vec::new();
            for g in &group_exprs {
                let (name, q) = match g {
                    Expr::Column(c) => input_scope
                        .iter()
                        .find(|f| f.id == *c)
                        .map_or((String::new(), None), |f| {
                            (f.name.clone(), f.qualifier.clone())
                        }),
                    _ => (String::new(), None),
                };
                schema.push(Field {
                    id: self.col_id(),
                    name,
                    qualifier: q,
                    ty: g.ty(&ty),
                    nullable: true,
                    hidden: false,
                });
            }
            for a in &aggs {
                let name = match a.func {
                    AggFunc::Count | AggFunc::CountStar => "count",
                    AggFunc::Sum => "sum",
                    AggFunc::Avg => "avg",
                    AggFunc::Min => "min",
                    AggFunc::Max => "max",
                };
                schema.push(Field {
                    id: self.col_id(),
                    name: name.to_owned(),
                    qualifier: None,
                    ty: a.ty(&ty),
                    nullable: true,
                    hidden: false,
                });
            }
            let rewrite =
                |e: Expr| -> Result<Expr> { rewrite_agg(e, &group_exprs, &schema, &input_scope) };
            items = items
                .into_iter()
                .map(|(e, n)| Ok((rewrite(e)?, n)))
                .collect::<Result<_>>()?;
            having = having.map(rewrite).transpose()?;
            for (t, _, _) in &mut order_bound {
                if let OrderTarget::Expr(e) = t {
                    *e = rewrite(e.clone())?;
                }
            }
            plan = Plan::Aggregate {
                input: Box::new(plan),
                group: group_exprs,
                aggs,
                schema,
            };
            if let Some(h) = having {
                plan = Plan::Filter {
                    input: Box::new(plan),
                    pred: h,
                };
            }
        } else if having.is_some() {
            return Err(Error::InvalidInput(
                "HAVING needs GROUP BY or an aggregate".into(),
            ));
        }
        let pre = plan.schema();
        let ty = ty_in(&pre);
        // Project the outputs plus any extra sort keys, sort, then drop the extras.
        let mut exprs: Vec<Expr> = items.iter().map(|(e, _)| e.clone()).collect();
        let mut schema: Vec<Field> = Vec::new();
        for (e, n) in &items {
            let (q, nullable) = match e {
                Expr::Column(c) => pre
                    .iter()
                    .find(|f| f.id == *c)
                    .map_or((None, true), |f| (f.qualifier.clone(), f.nullable)),
                _ => (None, true),
            };
            schema.push(Field {
                id: self.col_id(),
                name: n.clone(),
                qualifier: q,
                ty: e.ty(&ty),
                nullable,
                hidden: false,
            });
        }
        let visible = schema.len();
        let mut keys = Vec::new();
        for (t, desc, nulls_first) in order_bound {
            let id = match t {
                OrderTarget::Output(i) => schema[i].id,
                OrderTarget::Expr(e) => match exprs.iter().position(|x| *x == e) {
                    Some(i) => schema[i].id,
                    None => {
                        if s.distinct.is_some() {
                            return Err(Error::InvalidInput("for SELECT DISTINCT, ORDER BY expressions must appear in select list".into()));
                        }
                        let id = self.col_id();
                        schema.push(Field {
                            id,
                            name: String::new(),
                            qualifier: None,
                            ty: e.ty(&ty),
                            nullable: true,
                            hidden: true,
                        });
                        exprs.push(e);
                        id
                    }
                },
            };
            keys.push(SortKey {
                expr: Expr::Column(id),
                desc,
                nulls_first,
            });
        }
        plan = Plan::Project {
            input: Box::new(plan),
            exprs,
            schema: schema.clone(),
        };
        if s.distinct.is_some() {
            if !matches!(s.distinct, Some(ast::Distinct::Distinct)) {
                return unsupported("DISTINCT ON");
            }
            plan = Plan::Distinct {
                input: Box::new(plan),
            };
        }
        if !keys.is_empty() {
            plan = Plan::Sort {
                input: Box::new(plan),
                keys,
            };
        }
        if schema.len() > visible {
            let out: Vec<Field> = schema[..visible].to_vec();
            plan = Plan::Project {
                input: Box::new(plan),
                exprs: out.iter().map(|f| Expr::Column(f.id)).collect(),
                schema: out,
            };
        }
        Ok(plan)
    }

    /// ORDER BY over a query that is not a plain SELECT (VALUES, nested query).
    async fn order_output(&mut self, plan: Plan, order: &[ast::OrderByExpr]) -> Result<Plan> {
        if order.is_empty() {
            return Ok(plan);
        }
        let schema = plan.schema();
        let mut keys = Vec::new();
        for o in order {
            let desc = matches!(o.options.sort, Some(ast::OrderBySort::Desc));
            let e = match &o.expr {
                ast::Expr::Value(v) if matches!(&v.value, ast::Value::Number(..)) => {
                    let n = self.const_u64(&o.expr)? as usize;
                    Expr::Column(
                        schema
                            .get(n.wrapping_sub(1))
                            .ok_or_else(|| {
                                Error::InvalidInput(format!(
                                    "ORDER BY position {n} is not in select list"
                                ))
                            })?
                            .id,
                    )
                }
                e => self.bind_expr(e, &schema, &mut None).await?,
            };
            keys.push(SortKey {
                expr: e,
                desc,
                nulls_first: o.options.nulls_first.unwrap_or(desc),
            });
        }
        Ok(Plan::Sort {
            input: Box::new(plan),
            keys,
        })
    }

    pub async fn bind_bool(&mut self, e: &ast::Expr, scope: &Scope) -> Result<Expr> {
        let b = self.bind_expr(e, scope, &mut None).await?;
        let t = b.ty(&ty_in(scope));
        if t != DataType::Bool && !matches!(b, Expr::Literal(Value::Null, _)) {
            return Err(Error::InvalidInput(format!(
                "argument of WHERE must be type boolean, not type {t}"
            )));
        }
        Ok(b)
    }

    /// Bind an expression over `scope`. With `aggs`, aggregate calls are allowed
    /// and collected (they become [`Expr::AggRef`]).
    pub fn bind_expr<'a>(
        &'a mut self,
        e: &'a ast::Expr,
        scope: &'a Scope,
        aggs: &'a mut Option<Vec<AggCall>>,
    ) -> BoxFut<'a, Result<Expr>> {
        Box::pin(async move {
            use ast::Expr as A;
            Ok(match e {
                A::Identifier(i) => Expr::Column(find(scope, None, &ident(i))?.id),
                A::CompoundIdentifier(parts) => {
                    let n = parts.len();
                    if n < 2 {
                        return unsupported(format!("name {e}"));
                    }
                    Expr::Column(
                        find(scope, Some(&ident(&parts[n - 2])), &ident(&parts[n - 1]))?.id,
                    )
                }
                A::Value(v) => self.literal(&v.value)?,
                A::Nested(e) => self.bind_expr(e, scope, aggs).await?,
                A::BinaryOp { left, op, right } => {
                    let l = self.bind_expr(left, scope, aggs).await?;
                    let r = self.bind_expr(right, scope, aggs).await?;
                    let op = match op {
                        ast::BinaryOperator::Plus => BinOp::Add,
                        ast::BinaryOperator::Minus => BinOp::Sub,
                        ast::BinaryOperator::Multiply => BinOp::Mul,
                        ast::BinaryOperator::Divide => BinOp::Div,
                        ast::BinaryOperator::Modulo => BinOp::Mod,
                        ast::BinaryOperator::Eq => BinOp::Eq,
                        ast::BinaryOperator::NotEq => BinOp::NotEq,
                        ast::BinaryOperator::Lt => BinOp::Lt,
                        ast::BinaryOperator::LtEq => BinOp::LtEq,
                        ast::BinaryOperator::Gt => BinOp::Gt,
                        ast::BinaryOperator::GtEq => BinOp::GtEq,
                        ast::BinaryOperator::And => BinOp::And,
                        ast::BinaryOperator::Or => BinOp::Or,
                        ast::BinaryOperator::StringConcat => BinOp::Concat,
                        other => return unsupported(format!("operator {other}")),
                    };
                    match op {
                        BinOp::And | BinOp::Or => Expr::Binary(op, Box::new(l), Box::new(r)),
                        BinOp::Concat => Expr::Binary(
                            op,
                            Box::new(to_text(l, scope)),
                            Box::new(to_text(r, scope)),
                        ),
                        _ => {
                            let (l, r) = unify(l, r, scope)?;
                            Expr::Binary(op, Box::new(l), Box::new(r))
                        }
                    }
                }
                A::UnaryOp { op, expr } => {
                    let x = self.bind_expr(expr, scope, aggs).await?;
                    match op {
                        ast::UnaryOperator::Not => Expr::Not(Box::new(x)),
                        ast::UnaryOperator::Minus => match x {
                            Expr::Literal(Value::Int(i), t) => Expr::Literal(Value::Int(-i), t),
                            Expr::Literal(Value::Float(f), t) => Expr::Literal(Value::Float(-f), t),
                            x => Expr::Neg(Box::new(x)),
                        },
                        ast::UnaryOperator::Plus => x,
                        other => return unsupported(format!("operator {other}")),
                    }
                }
                A::IsNull(x) => {
                    Expr::IsNull(Box::new(self.bind_expr(x, scope, aggs).await?), false)
                }
                A::IsNotNull(x) => {
                    Expr::IsNull(Box::new(self.bind_expr(x, scope, aggs).await?), true)
                }
                A::IsTrue(x) => {
                    let x = self.bind_expr(x, scope, aggs).await?;
                    Expr::Binary(
                        BinOp::Eq,
                        Box::new(x),
                        Box::new(Expr::Literal(Value::Bool(true), DataType::Bool)),
                    )
                }
                A::IsFalse(x) => {
                    let x = self.bind_expr(x, scope, aggs).await?;
                    Expr::Binary(
                        BinOp::Eq,
                        Box::new(x),
                        Box::new(Expr::Literal(Value::Bool(false), DataType::Bool)),
                    )
                }
                A::Between {
                    expr,
                    negated,
                    low,
                    high,
                } => {
                    let x = self.bind_expr(expr, scope, aggs).await?;
                    let lo = self.bind_expr(low, scope, aggs).await?;
                    let hi = self.bind_expr(high, scope, aggs).await?;
                    let (a, lo) = unify(x.clone(), lo, scope)?;
                    let (b, hi) = unify(x, hi, scope)?;
                    let e = Expr::Binary(BinOp::GtEq, Box::new(a), Box::new(lo)).and(Expr::Binary(
                        BinOp::LtEq,
                        Box::new(b),
                        Box::new(hi),
                    ));
                    if *negated { Expr::Not(Box::new(e)) } else { e }
                }
                A::InList {
                    expr,
                    list,
                    negated,
                } => {
                    let x = self.bind_expr(expr, scope, aggs).await?;
                    let mut items = Vec::new();
                    for i in list {
                        let b = self.bind_expr(i, scope, aggs).await?;
                        items.push(unify(x.clone(), b, scope)?.1);
                    }
                    Expr::InList {
                        expr: Box::new(x),
                        list: items,
                        negated: *negated,
                    }
                }
                A::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    let x = self.bind_expr(expr, scope, aggs).await?;
                    let values = self.run_subquery(subquery).await?;
                    let mut items = Vec::new();
                    for v in values {
                        let t = v.data_type().unwrap_or(DataType::Text);
                        items.push(unify(x.clone(), Expr::Literal(v, t), scope)?.1);
                    }
                    if items.is_empty() {
                        Expr::Literal(Value::Bool(*negated), DataType::Bool)
                    } else {
                        Expr::InList {
                            expr: Box::new(x),
                            list: items,
                            negated: *negated,
                        }
                    }
                }
                A::Subquery(q) => {
                    let values = self.run_subquery(q).await?;
                    if values.len() > 1 {
                        return Err(Error::InvalidInput(
                            "more than one row returned by a subquery used as an expression".into(),
                        ));
                    }
                    let v = values.into_iter().next().unwrap_or(Value::Null);
                    let t = v.data_type().unwrap_or(DataType::Text);
                    Expr::Literal(v, t)
                }
                A::Exists { subquery, negated } => {
                    let values = self.run_subquery(subquery).await;
                    let any = match values {
                        Ok(v) => !v.is_empty(),
                        Err(Error::InvalidInput(m)) if m.contains("one column") => true,
                        Err(e) => return Err(e),
                    };
                    Expr::Literal(Value::Bool(any != *negated), DataType::Bool)
                }
                A::Like {
                    negated,
                    expr,
                    pattern,
                    ..
                }
                | A::ILike {
                    negated,
                    expr,
                    pattern,
                    ..
                } => {
                    let insensitive = matches!(e, A::ILike { .. });
                    let x = to_text(self.bind_expr(expr, scope, aggs).await?, scope);
                    let p = to_text(self.bind_expr(pattern, scope, aggs).await?, scope);
                    Expr::Like {
                        expr: Box::new(x),
                        pattern: Box::new(p),
                        negated: *negated,
                        insensitive,
                    }
                }
                A::Cast {
                    expr, data_type, ..
                } => {
                    let to = data_type_or_regclass(data_type)?;
                    match self.bind_expr(expr, scope, aggs).await? {
                        Expr::Literal(v, _) => Expr::Literal(v.coerce(to)?, to),
                        x => Expr::Cast(Box::new(x), to),
                    }
                }
                A::Case {
                    operand,
                    conditions,
                    else_result,
                    ..
                } => {
                    let operand = match operand {
                        Some(o) => Some(Box::new(self.bind_expr(o, scope, aggs).await?)),
                        None => None,
                    };
                    let mut whens = Vec::new();
                    for c in conditions {
                        let w = self.bind_expr(&c.condition, scope, aggs).await?;
                        let w = match &operand {
                            Some(o) => {
                                let (a, b) = unify((**o).clone(), w, scope)?;
                                Expr::Binary(BinOp::Eq, Box::new(a), Box::new(b))
                            }
                            None => w,
                        };
                        whens.push((w, self.bind_expr(&c.result, scope, aggs).await?));
                    }
                    let otherwise = match else_result {
                        Some(e) => Some(Box::new(self.bind_expr(e, scope, aggs).await?)),
                        None => None,
                    };
                    // All branches take the first non-NULL branch's type.
                    let ty = ty_in(scope);
                    let target = whens
                        .iter()
                        .map(|(_, t)| t)
                        .chain(otherwise.as_deref())
                        .find(|t| !matches!(t, Expr::Literal(Value::Null, _)))
                        .map(|t| t.ty(&ty));
                    let fix = |t: Expr| -> Result<Expr> {
                        match (t, target) {
                            (Expr::Literal(v, _), Some(to)) => Ok(Expr::Literal(v.coerce(to)?, to)),
                            (t, Some(to)) if t.ty(&ty) != to => Ok(Expr::Cast(Box::new(t), to)),
                            (t, _) => Ok(t),
                        }
                    };
                    let whens = whens
                        .into_iter()
                        .map(|(w, t)| Ok((w, fix(t)?)))
                        .collect::<Result<_>>()?;
                    let otherwise = otherwise.map(|o| fix(*o).map(Box::new)).transpose()?;
                    Expr::Case {
                        operand: None,
                        whens,
                        otherwise,
                    }
                }
                A::Function(f) => self.bind_function(f, scope, aggs).await?,
                A::Substring {
                    expr,
                    substring_from,
                    substring_for,
                    ..
                } => {
                    let mut args = vec![to_text(self.bind_expr(expr, scope, aggs).await?, scope)];
                    args.push(match substring_from {
                        Some(f) => self.bind_expr(f, scope, aggs).await?,
                        None => Expr::Literal(Value::Int(1), DataType::Int),
                    });
                    if let Some(l) = substring_for {
                        args.push(self.bind_expr(l, scope, aggs).await?);
                    }
                    Expr::Func(Func::Substr, args)
                }
                A::TypedString(ts) => {
                    let to = data_type(&ts.data_type)?;
                    match &ts.value.value {
                        ast::Value::SingleQuotedString(s) => {
                            Expr::Literal(Value::Text(s.clone()).coerce(to)?, to)
                        }
                        v => return unsupported(format!("typed literal {v}")),
                    }
                }
                other => return unsupported(format!("expression {other}")),
            })
        })
    }

    fn literal(&self, v: &ast::Value) -> Result<Expr> {
        Ok(match v {
            ast::Value::Number(n, _) => match n.parse::<i64>() {
                Ok(i) => Expr::Literal(Value::Int(i), DataType::Int),
                Err(_) => Expr::Literal(
                    Value::Float(
                        n.parse()
                            .map_err(|_| Error::InvalidInput(format!("bad number {n}")))?,
                    ),
                    DataType::Float,
                ),
            },
            ast::Value::SingleQuotedString(s)
            | ast::Value::EscapedStringLiteral(s)
            | ast::Value::DoubleQuotedString(s) => literal_text(s),
            ast::Value::Boolean(b) => Expr::Literal(Value::Bool(*b), DataType::Bool),
            ast::Value::Null => Expr::Literal(Value::Null, DataType::Text),
            ast::Value::Placeholder(p) => match self.param(p)? {
                Some(s) => literal_text(&s),
                None => Expr::Literal(Value::Null, DataType::Text),
            },
            other => return unsupported(format!("literal {other}")),
        })
    }

    async fn run_subquery(&mut self, q: &ast::Query) -> Result<Vec<Value>> {
        let plan = self.bind_query(q).await?;
        if plan.schema().iter().filter(|f| !f.hidden).count() != 1 {
            return Err(Error::InvalidInput(
                "subquery must return only one column".into(),
            ));
        }
        let plan = crate::optimizer::optimize(plan);
        let batch = crate::exec::execute(&plan, self.txn, self.now, None).await?;
        let col = batch.column(0);
        (0..batch.num_rows())
            .map(|r| Value::from_array(col.as_ref(), r))
            .collect()
    }

    async fn bind_function(
        &mut self,
        f: &ast::Function,
        scope: &Scope,
        aggs: &mut Option<Vec<AggCall>>,
    ) -> Result<Expr> {
        let name = object_name(&f.name)?;
        let (args, distinct, star) = match &f.args {
            ast::FunctionArguments::None => (Vec::new(), false, false),
            ast::FunctionArguments::List(l) => {
                let mut star = false;
                let mut args = Vec::new();
                for a in &l.args {
                    match a {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                            args.push(e.clone())
                        }
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => star = true,
                        other => return unsupported(format!("argument {other}")),
                    }
                }
                (
                    args,
                    matches!(
                        l.duplicate_treatment,
                        Some(ast::DuplicateTreatment::Distinct)
                    ),
                    star,
                )
            }
            ast::FunctionArguments::Subquery(_) => return unsupported("subquery argument"),
        };
        let agg = match name.as_str() {
            "count" if star => Some(AggFunc::CountStar),
            "count" => Some(AggFunc::Count),
            "sum" => Some(AggFunc::Sum),
            "avg" => Some(AggFunc::Avg),
            "min" => Some(AggFunc::Min),
            "max" => Some(AggFunc::Max),
            _ => None,
        };
        if let Some(func) = agg {
            let Some(list) = aggs.as_mut() else {
                return Err(Error::InvalidInput(format!(
                    "aggregate function {name} is not allowed here"
                )));
            };
            let _ = list;
            let arg = match args.first() {
                Some(a) => {
                    // Arguments of an aggregate are evaluated per input row.
                    Some(self.bind_expr(a, scope, &mut None).await?)
                }
                None => None,
            };
            let list = aggs
                .as_mut()
                .ok_or_else(|| Error::Internal("aggregate list".into()))?;
            list.push(AggCall {
                func,
                arg,
                distinct,
            });
            return Ok(Expr::AggRef(list.len() - 1));
        }
        let mut bound = Vec::new();
        for a in &args {
            bound.push(self.bind_expr(a, scope, aggs).await?);
        }
        let func = match name.as_str() {
            "lower" => Func::Lower,
            "upper" => Func::Upper,
            "length" | "char_length" => Func::Length,
            "abs" => Func::Abs,
            "round" => Func::Round,
            "coalesce" => Func::Coalesce,
            "nullif" => Func::Nullif,
            "substr" | "substring" => Func::Substr,
            "now" | "current_timestamp" | "localtimestamp" => {
                return Ok(Expr::Literal(
                    Value::Timestamp(self.now),
                    DataType::Timestamp,
                ));
            }
            "version" => return Ok(literal_text("PostgreSQL 16.0 (Stowage 0.1)")),
            "current_database" => return Ok(literal_text("stowage")),
            "current_schema" => return Ok(literal_text("public")),
            "current_user" | "session_user" => return Ok(literal_text("stowage")),
            other => return Err(Error::UndefinedFunction(other.to_owned())),
        };
        if matches!(func, Func::Coalesce | Func::Nullif) && !bound.is_empty() {
            let ty = ty_in(scope);
            let target = bound
                .iter()
                .find(|b| !matches!(b, Expr::Literal(Value::Null, _)))
                .map_or(DataType::Text, |b| b.ty(&ty));
            bound = bound
                .into_iter()
                .map(|b| match b {
                    Expr::Literal(v, _) => Ok(Expr::Literal(v.coerce(target)?, target)),
                    b if b.ty(&ty) != target => Ok(Expr::Cast(Box::new(b), target)),
                    b => Ok(b),
                })
                .collect::<Result<_>>()?;
        }
        Ok(Expr::Func(func, bound))
    }
}

enum OrderTarget {
    Output(usize),
    Expr(Expr),
}

fn data_type_or_regclass(t: &ast::DataType) -> Result<DataType> {
    match t {
        ast::DataType::Regclass => Ok(DataType::Text),
        t => data_type(t),
    }
}

fn to_text(e: Expr, scope: &Scope) -> Expr {
    match e {
        Expr::Literal(v, DataType::Text) => Expr::Literal(v, DataType::Text),
        Expr::Literal(v, _) => Expr::Literal(
            v.coerce(DataType::Text).unwrap_or(Value::Null),
            DataType::Text,
        ),
        e if e.ty(&ty_in(scope)) == DataType::Text => e,
        e => Expr::Cast(Box::new(e), DataType::Text),
    }
}

/// Over an aggregate's output: group expressions and aggregate results become
/// columns; any other column is an error.
fn rewrite_agg(e: Expr, group: &[Expr], schema: &[Field], input: &Scope) -> Result<Expr> {
    if let Some(i) = group.iter().position(|g| *g == e) {
        return Ok(Expr::Column(schema[i].id));
    }
    let mut err = None;
    let out = match e {
        Expr::AggRef(i) => Expr::Column(schema[group.len() + i].id),
        Expr::Column(c) => {
            let name = input.iter().find(|f| f.id == c).map_or_else(
                || format!("#{}", c.0),
                |f| match &f.qualifier {
                    Some(q) => format!("{q}.{}", f.name),
                    None => f.name.clone(),
                },
            );
            return Err(Error::Grouping(name));
        }
        Expr::Literal(..) => e,
        other => {
            // Rewrite children one level at a time so group expressions inside
            // larger expressions are found.
            let mut rec = |x: Expr| match rewrite_agg(x, group, schema, input) {
                Ok(v) => v,
                Err(e2) => {
                    err.get_or_insert(e2);
                    Expr::Literal(Value::Null, DataType::Text)
                }
            };
            match other {
                Expr::Binary(op, a, b) => Expr::Binary(op, Box::new(rec(*a)), Box::new(rec(*b))),
                Expr::Not(x) => Expr::Not(Box::new(rec(*x))),
                Expr::Neg(x) => Expr::Neg(Box::new(rec(*x))),
                Expr::IsNull(x, n) => Expr::IsNull(Box::new(rec(*x)), n),
                Expr::Cast(x, t) => Expr::Cast(Box::new(rec(*x)), t),
                Expr::InList {
                    expr,
                    list,
                    negated,
                } => Expr::InList {
                    expr: Box::new(rec(*expr)),
                    list: list.into_iter().map(&mut rec).collect(),
                    negated,
                },
                Expr::Like {
                    expr,
                    pattern,
                    negated,
                    insensitive,
                } => Expr::Like {
                    expr: Box::new(rec(*expr)),
                    pattern: Box::new(rec(*pattern)),
                    negated,
                    insensitive,
                },
                Expr::Case {
                    operand,
                    whens,
                    otherwise,
                } => Expr::Case {
                    operand: operand.map(|o| Box::new(rec(*o))),
                    whens: whens.into_iter().map(|(w, t)| (rec(w), rec(t))).collect(),
                    otherwise: otherwise.map(|o| Box::new(rec(*o))),
                },
                Expr::Func(f, args) => Expr::Func(f, args.into_iter().map(&mut rec).collect()),
                e => e,
            }
        }
    };
    match err {
        Some(e) => Err(e),
        None => Ok(out),
    }
}

/// The column name Postgres gives an unaliased select item.
fn output_name(e: &ast::Expr) -> String {
    match e {
        ast::Expr::Identifier(i) => ident(i),
        ast::Expr::CompoundIdentifier(p) => p.last().map(ident).unwrap_or_default(),
        ast::Expr::Function(f) => object_name(&f.name).unwrap_or_else(|_| "?column?".into()),
        ast::Expr::Cast { expr, .. } | ast::Expr::Nested(expr) => output_name(expr),
        ast::Expr::Case { .. } => "case".into(),
        _ => "?column?".into(),
    }
}
