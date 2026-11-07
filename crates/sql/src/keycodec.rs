//! Order-preserving ("memcomparable") key encoding, and row values.
//!
//! ```text
//!  table row     0x02 ‖ table id (u32 BE) ‖ 0x00000001 ‖ key(pk col)…          → row value
//!  index entry   0x02 ‖ table id (u32 BE) ‖ index id   ‖ key(col)… ‖ key(pk)…  → ""
//!  unique index  0x02 ‖ table id (u32 BE) ‖ index id   ‖ key(col)…             → pk key bytes
//!  catalog       0x01 ‖ "table/" ‖ name                                        → table descriptor
//! ```
//!
//! `key(v)` sorts exactly like the SQL values: `0x00` for NULL (first), else
//! `0x01` followed by
//! - integers and timestamps: big-endian with the sign bit flipped;
//! - floats: the IEEE bits, all flipped if negative, else just the sign bit;
//! - text and bytes: `0x00` escaped as `00 ff`, terminated by `00 01`
//!   (the same escaping as the MVCC layer, so prefixes never collide).

use crate::value::{DataType, Value};
use crate::{Error, Result};

pub const META: u8 = 0x01;
pub const TABLES: u8 = 0x02;
/// The primary index of every table.
pub const PRIMARY: u32 = 1;

pub fn index_prefix(table: u32, index: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(TABLES);
    k.extend_from_slice(&table.to_be_bytes());
    k.extend_from_slice(&index.to_be_bytes());
    k
}

pub fn table_prefix(table: u32) -> Vec<u8> {
    let mut k = vec![TABLES];
    k.extend_from_slice(&table.to_be_bytes());
    k
}

/// The smallest key greater than every key that starts with `p`.
pub fn prefix_end(p: &[u8]) -> Vec<u8> {
    let mut e = p.to_vec();
    while let Some(last) = e.pop() {
        if last < 0xff {
            e.push(last + 1);
            return e;
        }
    }
    Vec::new() // all 0xff: unbounded
}

pub fn encode_key(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(0x00),
        v => {
            out.push(0x01);
            match v {
                Value::Bool(b) => out.push(u8::from(*b)),
                Value::Int(i) | Value::Timestamp(i) => {
                    out.extend_from_slice(&((*i as u64) ^ (1 << 63)).to_be_bytes())
                }
                Value::Float(f) => {
                    let f = if *f == 0.0 { 0.0 } else { *f }; // -0.0 == 0.0
                    let bits = f.to_bits();
                    let k = if bits >> 63 == 1 {
                        !bits
                    } else {
                        bits ^ (1 << 63)
                    };
                    out.extend_from_slice(&k.to_be_bytes());
                }
                Value::Text(s) => escape(s.as_bytes(), out),
                Value::Bytes(b) => escape(b, out),
                Value::Null => {}
            }
        }
    }
}

fn escape(b: &[u8], out: &mut Vec<u8>) {
    for &x in b {
        out.push(x);
        if x == 0 {
            out.push(0xff);
        }
    }
    out.extend_from_slice(&[0x00, 0x01]);
}

/// Decode one key value of type `ty` from the front of `buf`; returns the rest.
pub fn decode_key(ty: DataType, buf: &[u8]) -> Result<(Value, &[u8])> {
    let bad = || Error::Internal("corrupt key encoding".into());
    let (&marker, rest) = buf.split_first().ok_or_else(bad)?;
    if marker == 0 {
        return Ok((Value::Null, rest));
    }
    let fixed = |rest: &[u8]| -> Result<(u64, usize)> {
        let b: [u8; 8] = rest
            .get(..8)
            .ok_or_else(bad)?
            .try_into()
            .map_err(|_| bad())?;
        Ok((u64::from_be_bytes(b), 8))
    };
    Ok(match ty {
        DataType::Bool => (Value::Bool(*rest.first().ok_or_else(bad)? == 1), &rest[1..]),
        DataType::Int | DataType::Timestamp => {
            let (u, n) = fixed(rest)?;
            let i = (u ^ (1 << 63)) as i64;
            (
                if ty == DataType::Int {
                    Value::Int(i)
                } else {
                    Value::Timestamp(i)
                },
                &rest[n..],
            )
        }
        DataType::Float => {
            let (k, n) = fixed(rest)?;
            let bits = if k >> 63 == 1 { k ^ (1 << 63) } else { !k };
            (Value::Float(f64::from_bits(bits)), &rest[n..])
        }
        DataType::Text | DataType::Bytes => {
            let mut out = Vec::new();
            let mut i = 0;
            loop {
                let x = *rest.get(i).ok_or_else(bad)?;
                if x == 0 {
                    match rest.get(i + 1) {
                        Some(0xff) => {
                            out.push(0);
                            i += 2;
                        }
                        Some(0x01) => break,
                        _ => return Err(bad()),
                    }
                } else {
                    out.push(x);
                    i += 1;
                }
            }
            let v = if ty == DataType::Text {
                Value::Text(String::from_utf8(out).map_err(|_| bad())?)
            } else {
                Value::Bytes(out)
            };
            (v, &rest[i + 2..])
        }
    })
}

/// A row value: every column, in table order.
///
/// ```text
///  0x01 (version) ‖ per column: 0x00 (NULL) | type tag ‖ payload
///  payload: bool 1 B · int/timestamp i64 LE · float f64 LE · text/bytes u32 LE length ‖ bytes
/// ```
pub fn encode_row(row: &[Value]) -> Vec<u8> {
    let mut out = vec![0x01];
    for v in row {
        match v {
            Value::Null => out.push(0),
            v => {
                out.push(v.data_type().map_or(0, DataType::tag));
                match v {
                    Value::Bool(b) => out.push(u8::from(*b)),
                    Value::Int(i) | Value::Timestamp(i) => out.extend_from_slice(&i.to_le_bytes()),
                    Value::Float(f) => out.extend_from_slice(&f.to_le_bytes()),
                    Value::Text(s) => {
                        out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                        out.extend_from_slice(s.as_bytes());
                    }
                    Value::Bytes(b) => {
                        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                        out.extend_from_slice(b);
                    }
                    Value::Null => {}
                }
            }
        }
    }
    out
}

pub fn decode_row(buf: &[u8], columns: usize) -> Result<Vec<Value>> {
    let bad = || Error::Internal("corrupt row encoding".into());
    if buf.first() != Some(&1) {
        return Err(bad());
    }
    let mut i = 1;
    let mut take = |n: usize| -> Result<&[u8]> {
        let s = buf.get(i..i + n).ok_or_else(bad)?;
        i += n;
        Ok(s)
    };
    let mut row = Vec::with_capacity(columns);
    for _ in 0..columns {
        let tag = take(1)?[0];
        if tag == 0 {
            row.push(Value::Null);
            continue;
        }
        let le8 = |s: &[u8]| -> Result<[u8; 8]> { s.try_into().map_err(|_| bad()) };
        row.push(match DataType::from_tag(tag)? {
            DataType::Bool => Value::Bool(take(1)?[0] == 1),
            DataType::Int => Value::Int(i64::from_le_bytes(le8(take(8)?)?)),
            DataType::Timestamp => Value::Timestamp(i64::from_le_bytes(le8(take(8)?)?)),
            DataType::Float => Value::Float(f64::from_le_bytes(le8(take(8)?)?)),
            DataType::Text | DataType::Bytes => {
                let n = u32::from_le_bytes(take(4)?.try_into().map_err(|_| bad())?) as usize;
                let b = take(n)?.to_vec();
                if tag == DataType::Text.tag() {
                    Value::Text(String::from_utf8(b).map_err(|_| bad())?)
                } else {
                    Value::Bytes(b)
                }
            }
        });
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(v: &Value) -> Vec<u8> {
        let mut k = Vec::new();
        encode_key(v, &mut k);
        k
    }

    #[test]
    fn keys_sort_like_values() {
        let ints = [i64::MIN, -5, -1, 0, 1, 7, i64::MAX];
        for w in ints.windows(2) {
            assert!(key(&Value::Int(w[0])) < key(&Value::Int(w[1])));
        }
        let floats = [f64::NEG_INFINITY, -2.5, -0.0, 0.5, 1e300, f64::INFINITY];
        for w in floats.windows(2) {
            assert!(
                key(&Value::Float(w[0])) <= key(&Value::Float(w[1])),
                "{w:?}"
            );
        }
        let texts = ["", "a", "a\0", "a\0b", "ab", "b"];
        for w in texts.windows(2) {
            assert!(
                key(&Value::Text(w[0].into())) < key(&Value::Text(w[1].into())),
                "{w:?}"
            );
        }
        assert!(key(&Value::Null) < key(&Value::Int(i64::MIN)));
        // A composite key: (text, int) never confuses where the text ends.
        let mut a = key(&Value::Text("a".into()));
        a.extend(key(&Value::Int(9)));
        let mut b = key(&Value::Text("a\0".into()));
        b.extend(key(&Value::Int(1)));
        assert!(a < b);
    }

    #[test]
    fn keys_and_rows_roundtrip() {
        let vals = [
            (DataType::Int, Value::Int(-42)),
            (DataType::Float, Value::Float(-1.5)),
            (DataType::Text, Value::Text("x\0y".into())),
            (DataType::Bool, Value::Bool(true)),
            (DataType::Timestamp, Value::Timestamp(1_700_000_000_000_000)),
            (DataType::Bytes, Value::Bytes(vec![0, 255, 0])),
            (DataType::Int, Value::Null),
        ];
        let mut k = Vec::new();
        for (_, v) in &vals {
            encode_key(v, &mut k);
        }
        let mut rest = k.as_slice();
        for (ty, v) in &vals {
            let (got, r) = decode_key(*ty, rest).unwrap();
            assert_eq!(&got, v);
            rest = r;
        }
        assert!(rest.is_empty());
        let row: Vec<Value> = vals.iter().map(|(_, v)| v.clone()).collect();
        assert_eq!(decode_row(&encode_row(&row), row.len()).unwrap(), row);
    }
}
