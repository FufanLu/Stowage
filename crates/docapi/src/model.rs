//! DynamoDB attribute values (as their JSON wire form) and how they become keys.
//!
//! ```text
//!  item        0x03 ‖ table id ‖ 0x00000000 ‖ key(hash) [‖ key(range)]          → item JSON
//!  GSI entry   0x03 ‖ table id ‖ gsi id     ‖ key(gsi hash) [‖ key(gsi range)]
//!                                           ‖ key(hash) [‖ key(range)]          → item JSON
//!  GSI ledger  0x04 ‖ table id ‖ base key                                        → the entries the item has
//!  table       0x01 "ddb/" ‖ name                                                → descriptor JSON
//! ```
//!
//! `key(v)` reuses the SQL layer's memcomparable encoding: strings as text,
//! numbers as IEEE doubles (exact up to 2⁵³; DynamoDB allows 38 digits, a
//! documented gap), binaries as bytes.

use std::cmp::Ordering;

use base64::Engine as _;
use serde_json::{Map, Value as Json};
use stowage_sql::keycodec;
use stowage_sql::value::Value;

use crate::{DdbError, Result};

pub const ITEMS: u8 = 0x03;
pub const LEDGER: u8 = 0x04;
pub const BASE: u32 = 0;

pub type Item = Map<String, Json>;

pub fn b64(s: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|_| DdbError::validation("invalid base64 binary value"))
}

/// The single type tag and payload of an attribute value: `{"S": "x"}` → ("S", "x").
pub fn tag(v: &Json) -> Result<(&str, &Json)> {
    match v.as_object() {
        Some(o) if o.len() == 1 => {
            let (k, v) = o
                .iter()
                .next()
                .ok_or_else(|| DdbError::validation("empty attribute value"))?;
            Ok((k.as_str(), v))
        }
        _ => Err(DdbError::validation(format!("invalid attribute value {v}"))),
    }
}

/// A key attribute as a sortable SQL value.
pub fn key_value(v: &Json) -> Result<Value> {
    Ok(match tag(v)? {
        ("S", Json::String(s)) => Value::Text(s.clone()),
        ("N", Json::String(n)) => Value::Float(
            n.parse()
                .map_err(|_| DdbError::validation(format!("invalid number {n}")))?,
        ),
        ("B", Json::String(b)) => Value::Bytes(b64(b)?),
        (t, _) => {
            return Err(DdbError::validation(format!(
                "key attributes must be S, N or B, not {t}"
            )));
        }
    })
}

pub fn encode_key_attr(v: &Json, out: &mut Vec<u8>) -> Result<()> {
    keycodec::encode_key(&key_value(v)?, out);
    Ok(())
}

pub fn prefix(table: u32, index: u32) -> Vec<u8> {
    let mut k = vec![ITEMS];
    k.extend_from_slice(&table.to_be_bytes());
    k.extend_from_slice(&index.to_be_bytes());
    k
}

pub fn ledger_key(table: u32, base_key: &[u8]) -> Vec<u8> {
    let mut k = vec![LEDGER];
    k.extend_from_slice(&table.to_be_bytes());
    k.extend_from_slice(base_key);
    k
}

/// `key(v)` of a string or binary without its terminator: every key starting
/// with these bytes has a value that starts with `v` (for `begins_with`).
pub fn key_prefix(v: &Json) -> Result<Vec<u8>> {
    let mut k = Vec::new();
    encode_key_attr(v, &mut k)?;
    match tag(v)?.0 {
        "S" | "B" => {
            k.truncate(k.len() - 2);
            Ok(k)
        }
        _ => Err(DdbError::validation("begins_with needs a string or binary")),
    }
}

pub fn encode_item(item: &Item) -> Vec<u8> {
    serde_json::to_vec(item).unwrap_or_default()
}

pub fn decode_item(b: &[u8]) -> Result<Item> {
    serde_json::from_slice(b).map_err(|e| DdbError::internal(format!("corrupt item: {e}")))
}

/// DynamoDB's number arithmetic on decimal strings: exact for integers, IEEE
/// doubles otherwise.
pub fn add_numbers(a: &str, b: &str, negate_b: bool) -> Result<String> {
    if let (Ok(x), Ok(y)) = (a.parse::<i128>(), b.parse::<i128>()) {
        let r = if negate_b {
            x.checked_sub(y)
        } else {
            x.checked_add(y)
        };
        return r
            .map(|v| v.to_string())
            .ok_or_else(|| DdbError::validation("number overflow"));
    }
    let (x, y): (f64, f64) = (
        a.parse()
            .map_err(|_| DdbError::validation(format!("invalid number {a}")))?,
        b.parse()
            .map_err(|_| DdbError::validation(format!("invalid number {b}")))?,
    );
    let r = if negate_b { x - y } else { x + y };
    Ok(if r.fract() == 0.0 && r.abs() < 1e15 {
        format!("{}", r as i64)
    } else {
        format!("{r}")
    })
}

/// Comparison for conditions: only between values of the same scalar type.
pub fn compare(a: &Json, b: &Json) -> Option<Ordering> {
    let (ta, va) = tag(a).ok()?;
    let (tb, vb) = tag(b).ok()?;
    if ta != tb {
        return None;
    }
    match (ta, va, vb) {
        ("N", Json::String(x), Json::String(y)) => {
            if let (Ok(i), Ok(j)) = (x.parse::<i128>(), y.parse::<i128>()) {
                return Some(i.cmp(&j));
            }
            x.parse::<f64>().ok()?.partial_cmp(&y.parse::<f64>().ok()?)
        }
        ("S", Json::String(x), Json::String(y)) => Some(x.as_bytes().cmp(y.as_bytes())),
        ("B", Json::String(x), Json::String(y)) => Some(b64(x).ok()?.cmp(&b64(y).ok()?)),
        _ => (va == vb).then_some(Ordering::Equal),
    }
}

/// Equality as DynamoDB means it (numbers by value, sets ignoring order).
pub fn equal(a: &Json, b: &Json) -> bool {
    match (tag(a), tag(b)) {
        (Ok((ta, va)), Ok((tb, vb))) if ta == tb => match ta {
            "N" | "S" | "B" => compare(a, b) == Some(Ordering::Equal),
            "SS" | "NS" | "BS" => {
                let set = |v: &Json| -> Vec<String> {
                    let mut s: Vec<String> = v
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(str::to_owned))
                                .collect()
                        })
                        .unwrap_or_default();
                    s.sort();
                    s
                };
                set(va) == set(vb)
            }
            _ => va == vb,
        },
        _ => false,
    }
}

/// Bytes of a value as `size()` counts them.
pub fn size(v: &Json) -> Option<usize> {
    let (t, x) = tag(v).ok()?;
    Some(match (t, x) {
        ("S", Json::String(s)) => s.len(),
        ("B", Json::String(s)) => b64(s).ok()?.len(),
        ("L" | "SS" | "NS" | "BS", Json::Array(a)) => a.len(),
        ("M", Json::Object(o)) => o.len(),
        _ => return None,
    })
}
