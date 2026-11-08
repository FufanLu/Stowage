//! The parts of `pg_catalog` that drivers and migration tools read, derived
//! from Stowage's own catalog on every query.
//!
//! Flyway, pgjdbc's `DatabaseMetaData` and `psql`'s `\d` look up schemas,
//! relations and columns here rather than in `information_schema`. Object ids
//! are stable functions of Stowage ids:
//!
//! ```text
//! namespace public          2200  (Postgres' own value)
//! table   t.id              16384 + t.id * 64
//! index   (t.id, index i)   16384 + t.id * 64 + i
//! type    a DataType        its Postgres type OID (bigint 20, text 25, …)
//! ```
//!
//! Catalogs that describe objects Stowage does not have (dependencies,
//! comments, functions) exist and are empty, so joins against them work.

use crate::Result;
use crate::catalog::{self, Table};
use crate::kv::KvTxn;
use crate::value::{DataType, Value};

pub const PUBLIC: i64 = 2200;
const PG_CATALOG: i64 = 11;
const INFORMATION_SCHEMA: i64 = 13000;
const OWNER: i64 = 10;

pub fn table_oid(t: &Table) -> i64 {
    16384 + i64::from(t.id) * 64
}

pub type View = (Vec<(&'static str, DataType)>, Vec<Vec<Value>>);

fn t(s: &str) -> Value {
    Value::Text(s.to_owned())
}
fn i(v: i64) -> Value {
    Value::Int(v)
}
fn b(v: bool) -> Value {
    Value::Bool(v)
}

/// Names a catalog view, or `None` when `name` is not one.
pub fn is_view(name: &str) -> bool {
    matches!(
        name,
        "pg_namespace"
            | "pg_class"
            | "pg_attribute"
            | "pg_index"
            | "pg_type"
            | "pg_depend"
            | "pg_description"
            | "pg_proc"
            | "pg_roles"
            | "pg_user"
            | "pg_database"
            | "pg_settings"
            | "pg_constraint"
            | "pg_inherits"
            | "pg_extension"
            | "pg_am"
            | "pg_attrdef"
            | "pg_views"
            | "pg_matviews"
            | "pg_sequences"
            | "pg_trigger"
            | "pg_rewrite"
            | "pg_enum"
            | "pg_collation"
            | "schemata"
    )
}

const TYPES: [DataType; 6] = [
    DataType::Bool,
    DataType::Int,
    DataType::Float,
    DataType::Text,
    DataType::Timestamp,
    DataType::Bytes,
];

fn type_name(d: DataType) -> &'static str {
    match d {
        DataType::Bool => "bool",
        DataType::Int => "int8",
        DataType::Float => "float8",
        DataType::Text => "text",
        DataType::Timestamp => "timestamp",
        DataType::Bytes => "bytea",
    }
}

pub async fn view(txn: &mut dyn KvTxn, name: &str) -> Result<Option<View>> {
    use DataType::{Bool, Int, Text};
    if !is_view(name) {
        return Ok(None);
    }
    let tables = catalog::list_tables(txn).await?;
    let v: View = match name {
        "pg_namespace" => (
            vec![
                ("oid", Int),
                ("nspname", Text),
                ("nspowner", Int),
                ("nspacl", Text),
            ],
            vec![
                vec![i(PG_CATALOG), t("pg_catalog"), i(OWNER), Value::Null],
                vec![i(PUBLIC), t("public"), i(OWNER), Value::Null],
                vec![
                    i(INFORMATION_SCHEMA),
                    t("information_schema"),
                    i(OWNER),
                    Value::Null,
                ],
            ],
        ),
        "schemata" => (
            vec![
                ("catalog_name", Text),
                ("schema_name", Text),
                ("schema_owner", Text),
            ],
            ["pg_catalog", "public", "information_schema"]
                .iter()
                .map(|s| vec![t("stowage"), t(s), t("stowage")])
                .collect(),
        ),
        "pg_class" => (
            vec![
                ("oid", Int),
                ("relname", Text),
                ("relnamespace", Int),
                ("relkind", Text),
                ("relowner", Int),
                ("reltuples", DataType::Float),
                ("relhasindex", Bool),
                ("relpersistence", Text),
                ("relispartition", Bool),
                ("relam", Int),
                ("reltype", Int),
                ("relacl", Text),
                ("reloptions", Text),
            ],
            tables
                .iter()
                .flat_map(|tb| {
                    let rows = tb.stats.as_ref().map_or(-1.0, |s| s.rows as f64);
                    let base = table_oid(tb);
                    std::iter::once(vec![
                        i(base),
                        t(&tb.name),
                        i(PUBLIC),
                        t("r"),
                        i(OWNER),
                        Value::Float(rows),
                        b(true),
                        t("p"),
                        b(false),
                        i(2),
                        i(0),
                        Value::Null,
                        Value::Null,
                    ])
                    .chain(std::iter::once(vec![
                        i(base + 1),
                        t(&format!("{}_pkey", tb.name)),
                        i(PUBLIC),
                        t("i"),
                        i(OWNER),
                        Value::Float(rows),
                        b(false),
                        t("p"),
                        b(false),
                        i(403),
                        i(0),
                        Value::Null,
                        Value::Null,
                    ]))
                    .chain(tb.indexes.iter().map(move |ix| {
                        vec![
                            i(base + i64::from(ix.id)),
                            t(&ix.name),
                            i(PUBLIC),
                            t("i"),
                            i(OWNER),
                            Value::Float(rows),
                            b(false),
                            t("p"),
                            b(false),
                            i(403),
                            i(0),
                            Value::Null,
                            Value::Null,
                        ]
                    }))
                })
                .collect(),
        ),
        "pg_attribute" => (
            vec![
                ("attrelid", Int),
                ("attname", Text),
                ("atttypid", Int),
                ("attnum", Int),
                ("attnotnull", Bool),
                ("atthasdef", Bool),
                ("attisdropped", Bool),
                ("attlen", Int),
                ("atttypmod", Int),
                ("attidentity", Text),
                ("attgenerated", Text),
            ],
            tables
                .iter()
                .flat_map(|tb| {
                    tb.visible_columns().map(move |(n, c)| {
                        vec![
                            i(table_oid(tb)),
                            t(&c.name),
                            i(i64::from(c.ty.pg_oid())),
                            i(n as i64 + 1),
                            b(!c.nullable),
                            b(c.default.is_some() || c.serial),
                            b(false),
                            i(-1),
                            i(-1),
                            t(""),
                            t(""),
                        ]
                    })
                })
                .collect(),
        ),
        "pg_index" => (
            vec![
                ("indexrelid", Int),
                ("indrelid", Int),
                ("indisunique", Bool),
                ("indisprimary", Bool),
                ("indkey", Text),
                ("indisvalid", Bool),
            ],
            tables
                .iter()
                .flat_map(|tb| {
                    let base = table_oid(tb);
                    let key = |cols: &[usize]| {
                        cols.iter()
                            .map(|c| (c + 1).to_string())
                            .collect::<Vec<_>>()
                            .join(" ")
                    };
                    std::iter::once(vec![
                        i(base + 1),
                        i(base),
                        b(true),
                        b(true),
                        t(&key(&tb.pk)),
                        b(true),
                    ])
                    .chain(tb.indexes.iter().map(move |ix| {
                        vec![
                            i(base + i64::from(ix.id)),
                            i(base),
                            b(ix.unique),
                            b(false),
                            t(&key(&ix.columns)),
                            b(ix.public),
                        ]
                    }))
                    .collect::<Vec<_>>()
                })
                .collect(),
        ),
        "pg_type" => (
            vec![
                ("oid", Int),
                ("typname", Text),
                ("typnamespace", Int),
                ("typtype", Text),
                ("typrelid", Int),
                ("typcategory", Text),
                ("typelem", Int),
                ("typbasetype", Int),
                ("typnotnull", Bool),
                ("typlen", Int),
                ("typowner", Int),
            ],
            TYPES
                .iter()
                .map(|d| {
                    vec![
                        i(i64::from(d.pg_oid())),
                        t(type_name(*d)),
                        i(PG_CATALOG),
                        t("b"),
                        i(0),
                        t(match d {
                            DataType::Bool => "B",
                            DataType::Int | DataType::Float => "N",
                            DataType::Text => "S",
                            DataType::Timestamp => "D",
                            DataType::Bytes => "U",
                        }),
                        i(0),
                        i(0),
                        b(false),
                        i(-1),
                        i(OWNER),
                    ]
                })
                .collect(),
        ),
        "pg_roles" | "pg_user" => (
            vec![
                ("oid", Int),
                ("rolname", Text),
                ("usename", Text),
                ("rolsuper", Bool),
                ("usesuper", Bool),
            ],
            vec![vec![i(OWNER), t("stowage"), t("stowage"), b(true), b(true)]],
        ),
        "pg_database" => (
            vec![
                ("oid", Int),
                ("datname", Text),
                ("datdba", Int),
                ("encoding", Int),
            ],
            vec![vec![i(16383), t("stowage"), i(OWNER), i(6)]],
        ),
        "pg_settings" => (
            vec![("name", Text), ("setting", Text)],
            vec![
                vec![t("server_version"), t("16.0")],
                vec![t("server_version_num"), t("160000")],
                vec![t("standard_conforming_strings"), t("on")],
            ],
        ),
        "pg_depend" => (
            vec![
                ("classid", Int),
                ("objid", Int),
                ("objsubid", Int),
                ("refclassid", Int),
                ("refobjid", Int),
                ("refobjsubid", Int),
                ("deptype", Text),
            ],
            vec![],
        ),
        "pg_description" => (
            vec![
                ("objoid", Int),
                ("classoid", Int),
                ("objsubid", Int),
                ("description", Text),
            ],
            vec![],
        ),
        "pg_proc" => (
            vec![
                ("oid", Int),
                ("proname", Text),
                ("pronamespace", Int),
                ("prokind", Text),
                ("proowner", Int),
            ],
            vec![],
        ),
        "pg_constraint" => (
            vec![
                ("oid", Int),
                ("conname", Text),
                ("connamespace", Int),
                ("contype", Text),
                ("conrelid", Int),
                ("confrelid", Int),
                ("conindid", Int),
            ],
            vec![],
        ),
        "pg_inherits" => (vec![("inhrelid", Int), ("inhparent", Int)], vec![]),
        "pg_extension" => (
            vec![
                ("oid", Int),
                ("extname", Text),
                ("extnamespace", Int),
                ("extversion", Text),
            ],
            vec![],
        ),
        "pg_am" => (
            vec![("oid", Int), ("amname", Text)],
            vec![vec![i(2), t("heap")], vec![i(403), t("btree")]],
        ),
        "pg_attrdef" => (
            vec![
                ("oid", Int),
                ("adrelid", Int),
                ("adnum", Int),
                ("adbin", Text),
            ],
            vec![],
        ),
        "pg_views" | "pg_matviews" => (
            vec![
                ("schemaname", Text),
                ("viewname", Text),
                ("matviewname", Text),
                ("definition", Text),
            ],
            vec![],
        ),
        "pg_sequences" => (vec![("schemaname", Text), ("sequencename", Text)], vec![]),
        "pg_trigger" => (
            vec![("oid", Int), ("tgrelid", Int), ("tgname", Text)],
            vec![],
        ),
        "pg_rewrite" => (
            vec![("oid", Int), ("ev_class", Int), ("rulename", Text)],
            vec![],
        ),
        "pg_enum" => (
            vec![("oid", Int), ("enumtypid", Int), ("enumlabel", Text)],
            vec![],
        ),
        "pg_collation" => (
            vec![("oid", Int), ("collname", Text), ("collnamespace", Int)],
            vec![],
        ),
        _ => return Ok(None),
    };
    Ok(Some(v))
}
