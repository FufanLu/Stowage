//! SQL types and values, and their Arrow equivalents.

use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float64Array,
    Float64Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
    TimestampMicrosecondArray, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowType, TimeUnit};

use crate::{Error, Result};

/// The column types Stowage stores. Postgres types map onto these
/// (`int`/`smallint`/`bigint` → `Int`, `real`/`double`/`numeric` → `Float`, …).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataType {
    Bool,
    Int,
    Float,
    Text,
    /// Microseconds since the Unix epoch.
    Timestamp,
    Bytes,
}

impl DataType {
    pub fn arrow(self) -> ArrowType {
        match self {
            Self::Bool => ArrowType::Boolean,
            Self::Int => ArrowType::Int64,
            Self::Float => ArrowType::Float64,
            Self::Text => ArrowType::Utf8,
            Self::Timestamp => ArrowType::Timestamp(TimeUnit::Microsecond, None),
            Self::Bytes => ArrowType::Binary,
        }
    }

    pub fn from_arrow(t: &ArrowType) -> Result<Self> {
        Ok(match t {
            ArrowType::Boolean => Self::Bool,
            ArrowType::Int64 => Self::Int,
            ArrowType::Float64 => Self::Float,
            ArrowType::Utf8 => Self::Text,
            ArrowType::Timestamp(TimeUnit::Microsecond, _) => Self::Timestamp,
            ArrowType::Binary => Self::Bytes,
            ArrowType::Null => Self::Text,
            other => return Err(Error::Internal(format!("no SQL type for arrow {other}"))),
        })
    }

    /// The Postgres type OID (for pgwire row descriptions).
    pub fn pg_oid(self) -> u32 {
        match self {
            Self::Bool => 16,
            Self::Int => 20,
            Self::Float => 701,
            Self::Text => 25,
            Self::Timestamp => 1114,
            Self::Bytes => 17,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Bool => "boolean",
            Self::Int => "bigint",
            Self::Float => "double precision",
            Self::Text => "text",
            Self::Timestamp => "timestamp",
            Self::Bytes => "bytea",
        }
    }

    pub fn tag(self) -> u8 {
        match self {
            Self::Bool => 1,
            Self::Int => 2,
            Self::Float => 3,
            Self::Text => 4,
            Self::Timestamp => 5,
            Self::Bytes => 6,
        }
    }

    pub fn from_tag(t: u8) -> Result<Self> {
        Ok(match t {
            1 => Self::Bool,
            2 => Self::Int,
            3 => Self::Float,
            4 => Self::Text,
            5 => Self::Timestamp,
            6 => Self::Bytes,
            _ => return Err(Error::Internal(format!("bad type tag {t}"))),
        })
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// One SQL value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Timestamp(i64),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn data_type(&self) -> Option<DataType> {
        Some(match self {
            Self::Null => return None,
            Self::Bool(_) => DataType::Bool,
            Self::Int(_) => DataType::Int,
            Self::Float(_) => DataType::Float,
            Self::Text(_) => DataType::Text,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::Bytes(_) => DataType::Bytes,
        })
    }

    /// Convert to `ty` (an assignment cast: what INSERT and UPDATE apply).
    pub fn coerce(self, ty: DataType) -> Result<Self> {
        Ok(match (self, ty) {
            (Self::Null, _) => Self::Null,
            (v @ Self::Bool(_), DataType::Bool)
            | (v @ Self::Int(_), DataType::Int)
            | (v @ Self::Float(_), DataType::Float)
            | (v @ Self::Text(_), DataType::Text)
            | (v @ Self::Timestamp(_), DataType::Timestamp)
            | (v @ Self::Bytes(_), DataType::Bytes) => v,
            (Self::Int(i), DataType::Float) => Self::Float(i as f64),
            (Self::Float(f), DataType::Int) => {
                if f.fract() != 0.0 || !f.is_finite() {
                    Self::Int(f.round() as i64)
                } else {
                    Self::Int(f as i64)
                }
            }
            (Self::Int(i), DataType::Text) => Self::Text(i.to_string()),
            (Self::Float(f), DataType::Text) => Self::Text(f.to_string()),
            (Self::Bool(b), DataType::Text) => Self::Text(b.to_string()),
            (Self::Text(s), DataType::Int) => Self::Int(s.trim().parse().map_err(|_| {
                Error::InvalidInput(format!("invalid input syntax for type bigint: \"{s}\""))
            })?),
            (Self::Text(s), DataType::Float) => Self::Float(s.trim().parse().map_err(|_| {
                Error::InvalidInput(format!(
                    "invalid input syntax for type double precision: \"{s}\""
                ))
            })?),
            (Self::Text(s), DataType::Bool) => {
                Self::Bool(match s.trim().to_ascii_lowercase().as_str() {
                    "t" | "true" | "yes" | "on" | "1" => true,
                    "f" | "false" | "no" | "off" | "0" => false,
                    _ => {
                        return Err(Error::InvalidInput(format!(
                            "invalid input syntax for type boolean: \"{s}\""
                        )));
                    }
                })
            }
            (Self::Text(s), DataType::Timestamp) => Self::Timestamp(parse_timestamp(&s)?),
            (Self::Text(s), DataType::Bytes) => Self::Bytes(s.into_bytes()),
            (Self::Timestamp(t), DataType::Text) => Self::Text(format_timestamp(t)),
            (v, ty) => return Err(Error::InvalidInput(format!("cannot convert {v} to {ty}"))),
        })
    }

    /// SQL ordering for non-null values of one type (NULLs sort last).
    pub fn sql_cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Null, Self::Null) => Ordering::Equal,
            (Self::Null, _) => Ordering::Greater,
            (_, Self::Null) => Ordering::Less,
            (Self::Bool(a), Self::Bool(b)) => a.cmp(b),
            (Self::Int(a), Self::Int(b)) | (Self::Timestamp(a), Self::Timestamp(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(b),
            (Self::Int(a), Self::Float(b)) => (*a as f64).total_cmp(b),
            (Self::Float(a), Self::Int(b)) => a.total_cmp(&(*b as f64)),
            (Self::Text(a), Self::Text(b)) => a.cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }

    /// The value at `row` of an Arrow array.
    pub fn from_array(a: &dyn Array, row: usize) -> Result<Self> {
        if a.is_null(row) {
            return Ok(Self::Null);
        }
        let any = a.as_any();
        Ok(if let Some(x) = any.downcast_ref::<Int64Array>() {
            Self::Int(x.value(row))
        } else if let Some(x) = any.downcast_ref::<Float64Array>() {
            Self::Float(x.value(row))
        } else if let Some(x) = any.downcast_ref::<BooleanArray>() {
            Self::Bool(x.value(row))
        } else if let Some(x) = any.downcast_ref::<StringArray>() {
            Self::Text(x.value(row).to_owned())
        } else if let Some(x) = any.downcast_ref::<TimestampMicrosecondArray>() {
            Self::Timestamp(x.value(row))
        } else if let Some(x) = any.downcast_ref::<BinaryArray>() {
            Self::Bytes(x.value(row).to_vec())
        } else if a.data_type() == &ArrowType::Null {
            Self::Null
        } else {
            return Err(Error::Internal(format!(
                "unsupported array type {}",
                a.data_type()
            )));
        })
    }

    /// Render for the text protocol (what psql prints).
    pub fn to_pg_text(&self) -> Option<String> {
        Some(match self {
            Self::Null => return None,
            Self::Bool(b) => if *b { "t" } else { "f" }.to_owned(),
            Self::Int(i) => i.to_string(),
            Self::Float(f) => format_float(*f),
            Self::Text(s) => s.clone(),
            Self::Timestamp(t) => format_timestamp(*t),
            Self::Bytes(b) => {
                let mut s = String::from("\\x");
                for x in b {
                    s.push_str(&format!("{x:02x}"));
                }
                s
            }
        })
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_pg_text() {
            None => f.write_str("NULL"),
            Some(s) => f.write_str(&s),
        }
    }
}

fn format_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".into()
    } else if f.is_infinite() {
        if f > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        }
    } else {
        // Postgres prints the shortest representation that round-trips.
        format!("{f}")
    }
}

/// Build one Arrow array of `ty` from values (which must already have that type).
pub fn build_array(ty: DataType, values: impl IntoIterator<Item = Value>) -> Result<ArrayRef> {
    let wrong = |v: &Value| Error::Internal(format!("{v:?} in a {ty} column"));
    Ok(match ty {
        DataType::Int => {
            let mut b = Int64Builder::new();
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::Int(i) => b.append_value(i),
                    v => return Err(wrong(&v)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float => {
            let mut b = Float64Builder::new();
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::Float(x) => b.append_value(x),
                    Value::Int(i) => b.append_value(i as f64),
                    v => return Err(wrong(&v)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Bool => {
            let mut b = BooleanBuilder::new();
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::Bool(x) => b.append_value(x),
                    v => return Err(wrong(&v)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Text => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::Text(x) => b.append_value(x),
                    v => return Err(wrong(&v)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Timestamp => {
            let mut b = TimestampMicrosecondBuilder::new();
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::Timestamp(x) => b.append_value(x),
                    v => return Err(wrong(&v)),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Bytes => {
            let mut b = BinaryBuilder::new();
            for v in values {
                match v {
                    Value::Null => b.append_null(),
                    Value::Bytes(x) => b.append_value(x),
                    v => return Err(wrong(&v)),
                }
            }
            Arc::new(b.finish())
        }
    })
}

/// `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` → microseconds since the epoch (UTC).
pub fn parse_timestamp(s: &str) -> Result<i64> {
    let bad = || Error::InvalidInput(format!("invalid input syntax for type timestamp: \"{s}\""));
    let s = s.trim().trim_end_matches('Z');
    let (date, time) = match s.split_once([' ', 'T']) {
        Some((d, t)) => (d, t),
        None => (s, "00:00:00"),
    };
    let mut d = date.split('-');
    let (y, m, day): (i64, i64, i64) = (
        d.next().and_then(|x| x.parse().ok()).ok_or_else(bad)?,
        d.next().and_then(|x| x.parse().ok()).ok_or_else(bad)?,
        d.next().and_then(|x| x.parse().ok()).ok_or_else(bad)?,
    );
    let time = time.split(['+']).next().unwrap_or(time);
    let mut t = time.split(':');
    let h: i64 = t.next().and_then(|x| x.parse().ok()).ok_or_else(bad)?;
    let mi: i64 = t.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let sec: f64 = t.next().and_then(|x| x.parse().ok()).unwrap_or(0.0);
    if !(1..=12).contains(&m) || !(1..=31).contains(&day) {
        return Err(bad());
    }
    let days = days_from_civil(y, m, day);
    Ok(((days * 86_400 + h * 3600 + mi * 60) * 1_000_000) + (sec * 1_000_000.0).round() as i64)
}

pub fn format_timestamp(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let frac = us.rem_euclid(1_000_000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    let base = format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    );
    if frac == 0 {
        base
    } else {
        format!("{base}.{}", format!("{frac:06}").trim_end_matches('0'))
    }
}

// Howard Hinnant's civil calendar algorithms.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_roundtrip() {
        for s in [
            "1970-01-01 00:00:00",
            "2026-11-06 13:45:07.25",
            "1999-12-31 23:59:59",
            "2000-02-29 00:00:00",
        ] {
            assert_eq!(format_timestamp(parse_timestamp(s).unwrap()), s);
        }
        assert_eq!(parse_timestamp("1970-01-02").unwrap(), 86_400_000_000);
    }
}
