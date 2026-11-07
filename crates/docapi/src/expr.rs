//! DynamoDB's expression languages: condition / filter / key condition,
//! update, and projection. One tokenizer, recursive-descent parsers, and
//! evaluators over items.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::{Map, Value as Json, json};

use crate::model::{self, Item, add_numbers, compare, equal, tag};
use crate::{DdbError, Result};

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Name(String),
    Value(String),
    Num(usize),
    Sym(&'static str),
}

fn tokenize(s: &str) -> Result<Vec<Tok>> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let two = s.get(i..i + 2).unwrap_or("");
        if let Some(sym) = ["<>", "<=", ">="].into_iter().find(|x| *x == two) {
            out.push(Tok::Sym(sym));
            i += 2;
            continue;
        }
        if let Some(sym) = ["(", ")", ",", ".", "[", "]", "=", "<", ">", "+", "-"]
            .into_iter()
            .find(|x| x.as_bytes()[0] == c)
        {
            out.push(Tok::Sym(sym));
            i += 1;
            continue;
        }
        let start = i;
        if c == b':' || c == b'#' {
            i += 1;
        }
        while i < b.len()
            && (b[i].is_ascii_alphanumeric()
                || b[i] == b'_'
                || b[i] == b'-' && start < i && b[start] == b':')
        {
            i += 1;
        }
        if i == start || (i == start + 1 && (c == b':' || c == b'#')) {
            return Err(DdbError::validation(format!(
                "invalid expression near {:?}",
                &s[start..]
            )));
        }
        let word = &s[start..i];
        out.push(if c == b':' {
            Tok::Value(word.to_owned())
        } else if c.is_ascii_digit() {
            Tok::Num(
                word.parse()
                    .map_err(|_| DdbError::validation(format!("bad number {word}")))?,
            )
        } else {
            Tok::Name(word.to_owned())
        });
    }
    Ok(out)
}

/// One step of a document path.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Attr(String),
    Index(usize),
}

pub type Path = Vec<Step>;

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Path(Path),
    Value(Json),
    Size(Path),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Cond {
    Cmp(Operand, &'static str, Operand),
    Between(Operand, Operand, Operand),
    In(Operand, Vec<Operand>),
    And(Box<Cond>, Box<Cond>),
    Or(Box<Cond>, Box<Cond>),
    Not(Box<Cond>),
    Exists(Path, bool),
    Type(Path, Operand),
    BeginsWith(Operand, Operand),
    Contains(Operand, Operand),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetValue {
    Operand(Operand),
    Plus(Box<SetValue>, Box<SetValue>),
    Minus(Box<SetValue>, Box<SetValue>),
    IfNotExists(Path, Box<SetValue>),
    ListAppend(Box<SetValue>, Box<SetValue>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Set(Path, SetValue),
    Remove(Path),
    Add(Path, Json),
    Delete(Path, Json),
}

/// Placeholders: `#name` → attribute name, `:value` → attribute value.
#[derive(Debug, Default, Clone)]
pub struct Bindings {
    pub names: HashMap<String, String>,
    pub values: HashMap<String, Json>,
}

impl Bindings {
    pub fn from_request(req: &Json) -> Self {
        let names = req["ExpressionAttributeNames"]
            .as_object()
            .map(|o| {
                o.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                    .collect()
            })
            .unwrap_or_default();
        let values = req["ExpressionAttributeValues"]
            .as_object()
            .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        Self { names, values }
    }
}

struct Parser<'a> {
    t: Vec<Tok>,
    i: usize,
    b: &'a Bindings,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Tok> {
        self.t.get(self.i)
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.t.get(self.i).cloned();
        self.i += 1;
        t
    }
    fn keyword(&self, k: &str) -> bool {
        matches!(self.peek(), Some(Tok::Name(n)) if n.eq_ignore_ascii_case(k))
    }
    fn sym(&self, s: &str) -> bool {
        matches!(self.peek(), Some(Tok::Sym(x)) if *x == s)
    }
    fn expect(&mut self, s: &str) -> Result<()> {
        if self.sym(s) {
            self.i += 1;
            Ok(())
        } else {
            Err(DdbError::validation(format!(
                "expected '{s}' in expression"
            )))
        }
    }
    fn done(&self) -> Result<()> {
        if self.i < self.t.len() {
            Err(DdbError::validation(format!(
                "unexpected token {:?} in expression",
                self.t[self.i]
            )))
        } else {
            Ok(())
        }
    }

    fn name(&mut self) -> Result<String> {
        match self.next() {
            Some(Tok::Name(n)) if n.starts_with('#') => {
                self.b.names.get(&n).cloned().ok_or_else(|| DdbError::validation(format!("An expression attribute name used in the document path is not defined; attribute name: {n}")))
            }
            Some(Tok::Name(n)) => Ok(n),
            t => Err(DdbError::validation(format!("expected an attribute name, got {t:?}"))),
        }
    }

    fn path(&mut self) -> Result<Path> {
        let mut p = vec![Step::Attr(self.name()?)];
        loop {
            if self.sym(".") {
                self.i += 1;
                p.push(Step::Attr(self.name()?));
            } else if self.sym("[") {
                self.i += 1;
                match self.next() {
                    Some(Tok::Num(n)) => p.push(Step::Index(n)),
                    t => {
                        return Err(DdbError::validation(format!(
                            "expected a list index, got {t:?}"
                        )));
                    }
                }
                self.expect("]")?;
            } else {
                return Ok(p);
            }
        }
    }

    fn value(&mut self) -> Result<Json> {
        match self.next() {
            Some(Tok::Value(v)) => self.b.values.get(&v).cloned().ok_or_else(|| DdbError::validation(format!("An expression attribute value used in expression is not defined; attribute value: {v}"))),
            t => Err(DdbError::validation(format!("expected a :value, got {t:?}"))),
        }
    }

    fn operand(&mut self) -> Result<Operand> {
        match self.peek() {
            Some(Tok::Value(_)) => Ok(Operand::Value(self.value()?)),
            Some(Tok::Name(n))
                if n.eq_ignore_ascii_case("size")
                    && matches!(self.t.get(self.i + 1), Some(Tok::Sym("("))) =>
            {
                self.i += 2;
                let p = self.path()?;
                self.expect(")")?;
                Ok(Operand::Size(p))
            }
            _ => Ok(Operand::Path(self.path()?)),
        }
    }

    fn cond(&mut self) -> Result<Cond> {
        let mut l = self.and()?;
        while self.keyword("OR") {
            self.i += 1;
            l = Cond::Or(Box::new(l), Box::new(self.and()?));
        }
        Ok(l)
    }
    fn and(&mut self) -> Result<Cond> {
        let mut l = self.not()?;
        while self.keyword("AND") {
            self.i += 1;
            l = Cond::And(Box::new(l), Box::new(self.not()?));
        }
        Ok(l)
    }
    fn not(&mut self) -> Result<Cond> {
        if self.keyword("NOT") {
            self.i += 1;
            return Ok(Cond::Not(Box::new(self.not()?)));
        }
        self.atom()
    }
    fn atom(&mut self) -> Result<Cond> {
        if self.sym("(") {
            self.i += 1;
            let c = self.cond()?;
            self.expect(")")?;
            return Ok(c);
        }
        if let Some(Tok::Name(f)) = self.peek().cloned()
            && matches!(self.t.get(self.i + 1), Some(Tok::Sym("(")))
            && !f.eq_ignore_ascii_case("size")
        {
            self.i += 2;
            let c = match f.as_str() {
                "attribute_exists" => Cond::Exists(self.path()?, true),
                "attribute_not_exists" => Cond::Exists(self.path()?, false),
                "attribute_type" => {
                    let p = self.path()?;
                    self.expect(",")?;
                    Cond::Type(p, self.operand()?)
                }
                "begins_with" => {
                    let a = self.operand()?;
                    self.expect(",")?;
                    Cond::BeginsWith(a, self.operand()?)
                }
                "contains" => {
                    let a = self.operand()?;
                    self.expect(",")?;
                    Cond::Contains(a, self.operand()?)
                }
                other => {
                    return Err(DdbError::validation(format!(
                        "Invalid function name; function: {other}"
                    )));
                }
            };
            self.expect(")")?;
            return Ok(c);
        }
        let l = self.operand()?;
        if self.keyword("BETWEEN") {
            self.i += 1;
            let lo = self.operand()?;
            if !self.keyword("AND") {
                return Err(DdbError::validation("BETWEEN needs AND"));
            }
            self.i += 1;
            return Ok(Cond::Between(l, lo, self.operand()?));
        }
        if self.keyword("IN") {
            self.i += 1;
            self.expect("(")?;
            let mut list = vec![self.operand()?];
            while self.sym(",") {
                self.i += 1;
                list.push(self.operand()?);
            }
            self.expect(")")?;
            return Ok(Cond::In(l, list));
        }
        let op = match self.next() {
            Some(Tok::Sym(s @ ("=" | "<>" | "<" | "<=" | ">" | ">="))) => s,
            t => {
                return Err(DdbError::validation(format!(
                    "expected a comparison, got {t:?}"
                )));
            }
        };
        Ok(Cond::Cmp(l, op, self.operand()?))
    }

    fn set_value(&mut self) -> Result<SetValue> {
        let mut l = self.set_atom()?;
        loop {
            if self.sym("+") {
                self.i += 1;
                l = SetValue::Plus(Box::new(l), Box::new(self.set_atom()?));
            } else if self.sym("-") {
                self.i += 1;
                l = SetValue::Minus(Box::new(l), Box::new(self.set_atom()?));
            } else {
                return Ok(l);
            }
        }
    }
    fn set_atom(&mut self) -> Result<SetValue> {
        if let Some(Tok::Name(f)) = self.peek().cloned()
            && matches!(self.t.get(self.i + 1), Some(Tok::Sym("(")))
        {
            self.i += 2;
            let v = match f.as_str() {
                "if_not_exists" => {
                    let p = self.path()?;
                    self.expect(",")?;
                    SetValue::IfNotExists(p, Box::new(self.set_value()?))
                }
                "list_append" => {
                    let a = self.set_value()?;
                    self.expect(",")?;
                    SetValue::ListAppend(Box::new(a), Box::new(self.set_value()?))
                }
                other => {
                    return Err(DdbError::validation(format!(
                        "Invalid function name; function: {other}"
                    )));
                }
            };
            self.expect(")")?;
            return Ok(v);
        }
        Ok(SetValue::Operand(self.operand()?))
    }
}

fn parser<'a>(s: &str, b: &'a Bindings) -> Result<Parser<'a>> {
    Ok(Parser {
        t: tokenize(s)?,
        i: 0,
        b,
    })
}

pub fn parse_condition(s: &str, b: &Bindings) -> Result<Cond> {
    let mut p = parser(s, b)?;
    let c = p.cond()?;
    p.done()?;
    Ok(c)
}

pub fn parse_projection(s: &str, b: &Bindings) -> Result<Vec<Path>> {
    let mut p = parser(s, b)?;
    let mut out = vec![p.path()?];
    while p.sym(",") {
        p.i += 1;
        out.push(p.path()?);
    }
    p.done()?;
    Ok(out)
}

pub fn parse_update(s: &str, b: &Bindings) -> Result<Vec<Action>> {
    let mut p = parser(s, b)?;
    let mut out = Vec::new();
    while p.peek().is_some() {
        let clause = p.name()?.to_ascii_uppercase();
        loop {
            match clause.as_str() {
                "SET" => {
                    let path = p.path()?;
                    p.expect("=")?;
                    out.push(Action::Set(path, p.set_value()?));
                }
                "REMOVE" => out.push(Action::Remove(p.path()?)),
                "ADD" => {
                    let path = p.path()?;
                    out.push(Action::Add(path, p.value()?));
                }
                "DELETE" => {
                    let path = p.path()?;
                    out.push(Action::Delete(path, p.value()?));
                }
                other => {
                    return Err(DdbError::validation(format!(
                        "Invalid UpdateExpression: unknown clause {other}"
                    )));
                }
            }
            if p.sym(",") {
                p.i += 1;
            } else {
                break;
            }
        }
    }
    if out.is_empty() {
        return Err(DdbError::validation("Invalid UpdateExpression: empty"));
    }
    Ok(out)
}

// ---------------------------------------------------------------- evaluation

pub fn get<'a>(item: &'a Item, path: &Path) -> Option<&'a Json> {
    let mut cur: Option<&Json> = None;
    for (i, step) in path.iter().enumerate() {
        cur = match (i, step, cur) {
            (0, Step::Attr(a), _) => item.get(a),
            (_, Step::Attr(a), Some(v)) => match tag(v).ok()? {
                ("M", Json::Object(m)) => m.get(a),
                _ => None,
            },
            (_, Step::Index(n), Some(v)) => match tag(v).ok()? {
                ("L", Json::Array(l)) => l.get(*n),
                _ => None,
            },
            _ => None,
        };
        cur?;
    }
    cur
}

fn operand<'a>(item: &'a Item, o: &'a Operand) -> Option<std::borrow::Cow<'a, Json>> {
    match o {
        Operand::Value(v) => Some(std::borrow::Cow::Borrowed(v)),
        Operand::Path(p) => get(item, p).map(std::borrow::Cow::Borrowed),
        Operand::Size(p) => get(item, p)
            .and_then(model::size)
            .map(|n| std::borrow::Cow::Owned(json!({ "N": n.to_string() }))),
    }
}

pub fn eval(c: &Cond, item: &Item) -> bool {
    match c {
        Cond::And(a, b) => eval(a, item) && eval(b, item),
        Cond::Or(a, b) => eval(a, item) || eval(b, item),
        Cond::Not(a) => !eval(a, item),
        Cond::Exists(p, want) => get(item, p).is_some() == *want,
        Cond::Type(p, t) => match (get(item, p), operand(item, t)) {
            (Some(v), Some(t)) => {
                tag(v).ok().map(|x| x.0) == tag(&t).ok().and_then(|x| x.1.as_str())
            }
            _ => false,
        },
        Cond::Cmp(l, op, r) => {
            let (Some(a), Some(b)) = (operand(item, l), operand(item, r)) else {
                return *op == "<>" && (operand(item, l).is_some() || operand(item, r).is_some());
            };
            match *op {
                "=" => equal(&a, &b),
                "<>" => !equal(&a, &b),
                op => match compare(&a, &b) {
                    None => false,
                    Some(o) => match op {
                        "<" => o == Ordering::Less,
                        "<=" => o != Ordering::Greater,
                        ">" => o == Ordering::Greater,
                        _ => o != Ordering::Less,
                    },
                },
            }
        }
        Cond::Between(x, lo, hi) => {
            match (operand(item, x), operand(item, lo), operand(item, hi)) {
                (Some(v), Some(l), Some(h)) => {
                    compare(&v, &l).is_some_and(|o| o != Ordering::Less)
                        && compare(&v, &h).is_some_and(|o| o != Ordering::Greater)
                }
                _ => false,
            }
        }
        Cond::In(x, list) => operand(item, x).is_some_and(|v| {
            list.iter()
                .any(|o| operand(item, o).is_some_and(|w| equal(&v, &w)))
        }),
        Cond::BeginsWith(a, p) => match (
            operand(item, a).as_deref().map(tag),
            operand(item, p).as_deref().map(tag),
        ) {
            (Some(Ok(("S", Json::String(s)))), Some(Ok(("S", Json::String(p))))) => {
                s.starts_with(p.as_str())
            }
            (Some(Ok(("B", Json::String(s)))), Some(Ok(("B", Json::String(p))))) => {
                matches!((model::b64(s), model::b64(p)), (Ok(s), Ok(p)) if s.starts_with(&p))
            }
            _ => false,
        },
        Cond::Contains(a, x) => {
            let (Some(a), Some(x)) = (operand(item, a), operand(item, x)) else {
                return false;
            };
            match (tag(&a), tag(&x)) {
                (Ok(("S", Json::String(s))), Ok(("S", Json::String(sub)))) => {
                    s.contains(sub.as_str())
                }
                (Ok(("SS" | "NS" | "BS", Json::Array(set))), Ok((_, elem))) => set.contains(elem),
                (Ok(("L", Json::Array(list))), _) => list.iter().any(|v| equal(v, &x)),
                _ => false,
            }
        }
    }
}

fn set_value(item: &Item, v: &SetValue) -> Result<Json> {
    Ok(match v {
        SetValue::Operand(o) => operand(item, o).map(std::borrow::Cow::into_owned).ok_or_else(|| DdbError::validation("The provided expression refers to an attribute that does not exist in the item"))?,
        SetValue::IfNotExists(p, d) => match get(item, p) {
            Some(v) => v.clone(),
            None => set_value(item, d)?,
        },
        SetValue::Plus(a, b) | SetValue::Minus(a, b) => {
            let (x, y) = (set_value(item, a)?, set_value(item, b)?);
            match (tag(&x)?, tag(&y)?) {
                (("N", Json::String(m)), ("N", Json::String(n))) => json!({ "N": add_numbers(m, n, matches!(v, SetValue::Minus(..)))? }),
                _ => return Err(DdbError::validation("An operand in the update expression has an incorrect data type")),
            }
        }
        SetValue::ListAppend(a, b) => {
            let (x, y) = (set_value(item, a)?, set_value(item, b)?);
            match (tag(&x)?, tag(&y)?) {
                (("L", Json::Array(l)), ("L", Json::Array(r))) => json!({ "L": l.iter().chain(r).cloned().collect::<Vec<_>>() }),
                _ => return Err(DdbError::validation("list_append needs two lists")),
            }
        }
    })
}

/// Write `v` at `path`, creating nothing but the last step.
fn put(item: &mut Item, path: &Path, v: Json) -> Result<()> {
    let bad = || {
        DdbError::validation(
            "The document path provided in the update expression is invalid for update",
        )
    };
    let (last, init) = path.split_last().ok_or_else(bad)?;
    if init.is_empty() {
        let Step::Attr(a) = last else {
            return Err(bad());
        };
        item.insert(a.clone(), v);
        return Ok(());
    }
    let parent = get_mut(item, init).ok_or_else(bad)?;
    match (
        last,
        parent.as_object_mut().and_then(|o| o.iter_mut().next()),
    ) {
        (Step::Attr(a), Some((t, Json::Object(m)))) if t == "M" => {
            m.insert(a.clone(), v);
        }
        (Step::Index(n), Some((t, Json::Array(l)))) if t == "L" => {
            if *n < l.len() {
                l[*n] = v;
            } else {
                l.push(v);
            }
        }
        _ => return Err(bad()),
    }
    Ok(())
}

fn get_mut<'a>(item: &'a mut Item, path: &[Step]) -> Option<&'a mut Json> {
    let (first, rest) = path.split_first()?;
    let Step::Attr(a) = first else { return None };
    let mut cur = item.get_mut(a)?;
    for step in rest {
        let obj = cur.as_object_mut()?;
        cur = match step {
            Step::Attr(a) => obj.get_mut("M")?.as_object_mut()?.get_mut(a)?,
            Step::Index(n) => obj.get_mut("L")?.as_array_mut()?.get_mut(*n)?,
        };
    }
    Some(cur)
}

fn remove(item: &mut Item, path: &Path) {
    let Some((last, init)) = path.split_last() else {
        return;
    };
    if init.is_empty() {
        if let Step::Attr(a) = last {
            item.remove(a);
        }
        return;
    }
    if let Some(parent) = get_mut(item, init)
        && let Some(inner) = parent.as_object_mut().and_then(|o| o.iter_mut().next())
    {
        match (last, inner) {
            (Step::Attr(a), (_, Json::Object(m))) => {
                m.remove(a);
            }
            (Step::Index(n), (_, Json::Array(l))) if *n < l.len() => {
                l.remove(*n);
            }
            _ => {}
        }
    }
}

/// Apply an update expression; values are computed against the item as it was.
pub fn apply(item: &mut Item, actions: &[Action]) -> Result<()> {
    let before = item.clone();
    for a in actions {
        match a {
            Action::Set(p, v) => {
                let v = set_value(&before, v)?;
                put(item, p, v)?;
            }
            Action::Remove(p) => remove(item, p),
            Action::Add(p, v) => {
                let cur = get(item, p).cloned();
                let new = match (cur.as_ref().map(tag).transpose()?, tag(v)?) {
                    (None, _) => v.clone(),
                    (Some(("N", Json::String(a))), ("N", Json::String(b))) => {
                        json!({ "N": add_numbers(a, b, false)? })
                    }
                    (Some((t @ ("SS" | "NS" | "BS"), Json::Array(a))), (u, Json::Array(b)))
                        if t == u =>
                    {
                        let mut s = a.clone();
                        for x in b {
                            if !s.contains(x) {
                                s.push(x.clone());
                            }
                        }
                        json!({ t: s })
                    }
                    _ => {
                        return Err(DdbError::validation(
                            "An operand in the update expression has an incorrect data type",
                        ));
                    }
                };
                put(item, p, new)?;
            }
            Action::Delete(p, v) => {
                if let Some(cur) = get(item, p).cloned() {
                    match (tag(&cur)?, tag(v)?) {
                        ((t, Json::Array(a)), (u, Json::Array(b))) if t == u => {
                            let s: Vec<Json> =
                                a.iter().filter(|x| !b.contains(x)).cloned().collect();
                            if s.is_empty() {
                                remove(item, p);
                            } else {
                                put(item, p, json!({ t: s }))?;
                            }
                        }
                        _ => return Err(DdbError::validation("DELETE needs a set")),
                    }
                }
            }
        }
    }
    Ok(())
}

/// Keep only the projected paths.
pub fn project(item: &Item, paths: &[Path]) -> Item {
    let mut out = Map::new();
    for p in paths {
        if let Some(v) = get(item, p) {
            match p.as_slice() {
                [Step::Attr(a)] => {
                    out.insert(a.clone(), v.clone());
                }
                [Step::Attr(a), ..] => {
                    // Nested paths: keep the top-level attribute (a simplification).
                    if let Some(top) = item.get(a) {
                        out.insert(a.clone(), top.clone());
                    }
                }
                _ => {}
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(values: &[(&str, Json)]) -> Bindings {
        Bindings {
            names: [("#n".to_owned(), "name".to_owned())].into(),
            values: values
                .iter()
                .map(|(k, v)| ((*k).to_owned(), v.clone()))
                .collect(),
        }
    }

    fn item(v: Json) -> Item {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn conditions() {
        let it = item(
            json!({"id": {"S": "a"}, "qty": {"N": "5"}, "name": {"S": "bolt"}, "tags": {"SS": ["x", "y"]}, "m": {"M": {"k": {"N": "1"}}}}),
        );
        let bb = b(&[
            (":five", json!({"N": "5"})),
            (":p", json!({"S": "bo"})),
            (":x", json!({"S": "x"})),
            (":one", json!({"N": "1.0"})),
        ]);
        for (s, want) in [
            ("qty = :five", true),
            ("qty > :five", false),
            ("qty BETWEEN :one AND :five", true),
            ("begins_with(#n, :p) AND attribute_exists(id)", true),
            ("attribute_not_exists(missing) AND NOT qty <> :five", true),
            ("contains(tags, :x)", true),
            ("m.k = :one", true),
            ("size(tags) = :five OR qty IN (:one, :five)", true),
            ("(qty < :one)", false),
        ] {
            assert_eq!(eval(&parse_condition(s, &bb).unwrap(), &it), want, "{s}");
        }
        assert!(parse_condition("qty = :undefined", &bb).is_err());
    }

    #[test]
    fn updates() {
        let mut it = item(
            json!({"id": {"S": "a"}, "qty": {"N": "5"}, "l": {"L": [{"N": "1"}]}, "s": {"SS": ["a"]}}),
        );
        let bb = b(&[
            (":d", json!({"N": "-2"})),
            (":l", json!({"L": [{"N": "2"}]})),
            (":z", json!({"N": "0"})),
            (":s", json!({"SS": ["b"]})),
        ]);
        let acts = parse_update("SET qty = qty + :d, l = list_append(l, :l), c = if_not_exists(c, :z) REMOVE gone ADD s :s, n :d", &bb).unwrap();
        apply(&mut it, &acts).unwrap();
        assert_eq!(it["qty"], json!({"N": "3"}));
        assert_eq!(it["l"], json!({"L": [{"N": "1"}, {"N": "2"}]}));
        assert_eq!(it["c"], json!({"N": "0"}));
        assert_eq!(it["s"], json!({"SS": ["a", "b"]}));
        assert_eq!(it["n"], json!({"N": "-2"}));
    }
}
