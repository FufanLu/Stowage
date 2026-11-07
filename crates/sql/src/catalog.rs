//! Table descriptors, stored transactionally in the key space itself.
//!
//! ```text
//!  0x01 "table/" ‖ name   → descriptor (columns, primary key, indexes, statistics)
//!  0x01 "seq/tables"      → last table id
//!  0x01 "seq/" ‖ id ‖ col → last value of a SERIAL column
//! ```
//!
//! DDL runs in a transaction like any other write, so a table appears to others
//! exactly when the transaction that created it commits.

use stowage_kv::codec::{R, W};

use crate::keycodec::{self, META, PRIMARY};
use crate::kv::KvTxn;
use crate::value::{DataType, Value};
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
    /// A constant default.
    pub default: Option<Value>,
    /// `SERIAL`: filled from a per-column sequence when not given.
    pub serial: bool,
    /// The implicit `rowid` key of a table declared without a primary key; not
    /// shown by `SELECT *`.
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Index {
    pub id: u32,
    pub name: String,
    /// Positions in the table's columns.
    pub columns: Vec<usize>,
    pub unique: bool,
    /// Readable. A new index is first write-only: writers maintain it while it
    /// is backfilled, readers ignore it (the F1 / CockroachDB schema-change states).
    pub public: bool,
}

/// What `ANALYZE` measured.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Stats {
    pub rows: u64,
    /// Distinct non-null values per column.
    pub distinct: Vec<u64>,
    pub nulls: Vec<u64>,
    /// Equi-depth histogram bounds per column (up to 16 buckets).
    pub bounds: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub id: u32,
    pub name: String,
    pub columns: Vec<Column>,
    /// Positions of the primary-key columns, in key order.
    pub pk: Vec<usize>,
    pub indexes: Vec<Index>,
    pub next_index_id: u32,
    pub stats: Option<Stats>,
}

impl Table {
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    /// Key of the row whose primary-key values are `row[pk]`.
    pub fn row_key(&self, row: &[Value]) -> Vec<u8> {
        let mut k = keycodec::index_prefix(self.id, PRIMARY);
        for &c in &self.pk {
            keycodec::encode_key(&row[c], &mut k);
        }
        k
    }

    /// Key of `row` in secondary index `ix`. A unique index keys on its columns
    /// only (so a duplicate collides) unless one of them is NULL, which never
    /// collides in SQL: then the primary key is appended, as for a non-unique one.
    pub fn index_key(&self, ix: &Index, row: &[Value]) -> (Vec<u8>, bool) {
        let mut k = keycodec::index_prefix(self.id, ix.id);
        for &c in &ix.columns {
            keycodec::encode_key(&row[c], &mut k);
        }
        let unique = ix.unique && ix.columns.iter().all(|&c| !row[c].is_null());
        if !unique {
            for &c in &self.pk {
                keycodec::encode_key(&row[c], &mut k);
            }
        }
        (k, unique)
    }

    pub fn visible_columns(&self) -> impl Iterator<Item = (usize, &Column)> {
        self.columns.iter().enumerate().filter(|(_, c)| !c.hidden)
    }

    fn encode(&self) -> Vec<u8> {
        let mut w = W::new();
        w.u8(2)
            .u32(self.id)
            .bytes(self.name.as_bytes())
            .u32(self.columns.len() as u32);
        for c in &self.columns {
            w.bytes(c.name.as_bytes())
                .u8(c.ty.tag())
                .u8(u8::from(c.nullable))
                .u8(u8::from(c.serial))
                .u8(u8::from(c.hidden));
            w.opt_bytes(
                c.default
                    .as_ref()
                    .map(|d| keycodec::encode_row(std::slice::from_ref(d)))
                    .as_deref(),
            );
        }
        w.u32(self.pk.len() as u32);
        for &p in &self.pk {
            w.u32(p as u32);
        }
        w.u32(self.indexes.len() as u32);
        for ix in &self.indexes {
            w.u32(ix.id)
                .bytes(ix.name.as_bytes())
                .u8(u8::from(ix.unique))
                .u8(u8::from(ix.public))
                .u32(ix.columns.len() as u32);
            for &c in &ix.columns {
                w.u32(c as u32);
            }
        }
        w.u32(self.next_index_id);
        match &self.stats {
            None => {
                w.u8(0);
            }
            Some(s) => {
                w.u8(1).u64(s.rows);
                for i in 0..self.columns.len() {
                    w.u64(s.distinct.get(i).copied().unwrap_or(0))
                        .u64(s.nulls.get(i).copied().unwrap_or(0));
                    let b = s.bounds.get(i).cloned().unwrap_or_default();
                    w.u32(b.len() as u32).bytes(&keycodec::encode_row(&b));
                }
            }
        }
        w.finish()
    }

    fn decode(b: &[u8]) -> Result<Self> {
        let e = |e: stowage_kv::Error| Error::Internal(format!("corrupt descriptor: {e}"));
        let s = |b: Vec<u8>| {
            String::from_utf8(b).map_err(|_| Error::Internal("corrupt descriptor".into()))
        };
        let mut r = R(b);
        let version = r.u8().map_err(e)?;
        let id = r.u32().map_err(e)?;
        let name = s(r.bytes().map_err(e)?)?;
        let n = r.u32().map_err(e)? as usize;
        let mut columns = Vec::with_capacity(n);
        for _ in 0..n {
            let name = s(r.bytes().map_err(e)?)?;
            let ty = DataType::from_tag(r.u8().map_err(e)?)?;
            let nullable = r.u8().map_err(e)? == 1;
            let serial = r.u8().map_err(e)? == 1;
            let hidden = r.u8().map_err(e)? == 1;
            let default = match r.opt_bytes().map_err(e)? {
                Some(d) => keycodec::decode_row(&d, 1)?.pop(),
                None => None,
            };
            columns.push(Column {
                name,
                ty,
                nullable,
                default,
                serial,
                hidden,
            });
        }
        let pk = (0..r.u32().map_err(e)?)
            .map(|_| r.u32().map(|x| x as usize))
            .collect::<std::result::Result<_, _>>()
            .map_err(e)?;
        let ni = r.u32().map_err(e)?;
        let mut indexes = Vec::new();
        for _ in 0..ni {
            let id = r.u32().map_err(e)?;
            let name = s(r.bytes().map_err(e)?)?;
            let unique = r.u8().map_err(e)? == 1;
            let public = if version >= 2 {
                r.u8().map_err(e)? == 1
            } else {
                true
            };
            let cols = (0..r.u32().map_err(e)?)
                .map(|_| r.u32().map(|x| x as usize))
                .collect::<std::result::Result<_, _>>()
                .map_err(e)?;
            indexes.push(Index {
                id,
                name,
                columns: cols,
                unique,
                public,
            });
        }
        let next_index_id = r.u32().map_err(e)?;
        let stats = if r.u8().map_err(e)? == 1 {
            let rows = r.u64().map_err(e)?;
            let mut st = Stats {
                rows,
                ..Stats::default()
            };
            for _ in 0..n {
                st.distinct.push(r.u64().map_err(e)?);
                st.nulls.push(r.u64().map_err(e)?);
                let len = r.u32().map_err(e)? as usize;
                st.bounds
                    .push(keycodec::decode_row(&r.bytes().map_err(e)?, len)?);
            }
            Some(st)
        } else {
            None
        };
        Ok(Self {
            id,
            name,
            columns,
            pk,
            indexes,
            next_index_id,
            stats,
        })
    }
}

fn table_key(name: &str) -> Vec<u8> {
    let mut k = vec![META];
    k.extend_from_slice(b"table/");
    k.extend_from_slice(name.as_bytes());
    k
}

pub async fn get_table(txn: &mut dyn KvTxn, name: &str) -> Result<Option<Table>> {
    match txn.get(&table_key(name)).await? {
        Some(b) => Ok(Some(Table::decode(&b)?)),
        None => Ok(None),
    }
}

pub async fn table(txn: &mut dyn KvTxn, name: &str) -> Result<Table> {
    get_table(txn, name)
        .await?
        .ok_or_else(|| Error::UndefinedTable(name.to_owned()))
}

pub async fn put_table(txn: &mut dyn KvTxn, t: &Table) -> Result<()> {
    txn.put(&table_key(&t.name), &t.encode()).await
}

pub async fn drop_table(txn: &mut dyn KvTxn, name: &str) -> Result<()> {
    txn.delete(&table_key(name)).await
}

pub async fn list_tables(txn: &mut dyn KvTxn) -> Result<Vec<Table>> {
    let start = table_key("");
    let end = keycodec::prefix_end(&start);
    txn.scan(&start, &end, u32::MAX)
        .await?
        .into_iter()
        .map(|(_, v)| Table::decode(&v))
        .collect()
}

/// Increment and return a counter stored at `0x01 "seq/" ‖ name`.
pub async fn next_seq(txn: &mut dyn KvTxn, name: &[u8]) -> Result<i64> {
    let mut k = vec![META];
    k.extend_from_slice(b"seq/");
    k.extend_from_slice(name);
    let cur = match txn.get(&k).await? {
        Some(b) => i64::from_le_bytes(
            b.get(..8)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| Error::Internal("corrupt sequence".into()))?,
        ),
        None => 0,
    };
    let next = cur + 1;
    txn.put(&k, &next.to_le_bytes()).await?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_roundtrip() {
        let t = Table {
            id: 7,
            name: "orders".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    ty: DataType::Int,
                    nullable: false,
                    default: None,
                    serial: true,
                    hidden: false,
                },
                Column {
                    name: "note".into(),
                    ty: DataType::Text,
                    nullable: true,
                    default: Some(Value::Text("-".into())),
                    serial: false,
                    hidden: false,
                },
            ],
            pk: vec![0],
            indexes: vec![Index {
                id: 2,
                name: "orders_note".into(),
                columns: vec![1],
                unique: false,
                public: true,
            }],
            next_index_id: 3,
            stats: Some(Stats {
                rows: 10,
                distinct: vec![10, 3],
                nulls: vec![0, 1],
                bounds: vec![vec![Value::Int(1), Value::Int(10)], vec![]],
            }),
        };
        assert_eq!(Table::decode(&t.encode()).unwrap(), t);
    }
}
