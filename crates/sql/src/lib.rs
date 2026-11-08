//! Stowage SQL: a Postgres-dialect SQL engine over the transactional key space.
//!
//! ```text
//!  text ─► sqlparser AST ─► binder (names, types, aggregates) ─► Plan
//!       ─► optimizer (pushdown, join order by DP, access paths) ─► Plan
//!       ─► executor (Arrow batches, vectorized kernels) ─► rows
//! ```
//!
//! Tables live in the same ordered key space as everything else, keyed by the
//! primary key ([`keycodec`]); their descriptors too ([`catalog`]). Every
//! statement runs in an interactive transaction (SSI by default).

pub mod audit;
pub mod auth;
pub mod binder;
pub mod catalog;
pub mod dml;
pub mod exec;
pub mod explain;
pub mod keycodec;
pub mod kv;
pub mod optimizer;
pub mod plan;
pub mod session;
pub mod stats;
pub mod value;

pub use kv::{ClusterKv, KvStore, MemKv};
pub use session::{Output, Session, TxStatus};
pub use value::{DataType, Value};

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("syntax error: {0}")]
    Parse(String),
    #[error("not supported: {0}")]
    Unsupported(String),
    #[error("{0}")]
    InvalidInput(String),
    #[error("relation \"{0}\" does not exist")]
    UndefinedTable(String),
    #[error("column \"{0}\" does not exist")]
    UndefinedColumn(String),
    #[error("function {0} does not exist")]
    UndefinedFunction(String),
    #[error("{0} does not exist")]
    UndefinedObject(String),
    #[error("column reference \"{0}\" is ambiguous")]
    Ambiguous(String),
    #[error(
        "column \"{0}\" must appear in the GROUP BY clause or be used in an aggregate function"
    )]
    Grouping(String),
    #[error("relation \"{0}\" already exists")]
    DuplicateTable(String),
    #[error("relation \"{0}\" already exists")]
    DuplicateObject(String),
    #[error("duplicate key value violates unique constraint \"{0}\"")]
    UniqueViolation(String),
    #[error("null value in column \"{0}\" of relation \"{1}\" violates not-null constraint")]
    NotNullViolation(String, String),
    #[error("division by zero")]
    DivisionByZero,
    #[error("{0}")]
    OutOfRange(String),
    #[error("restart transaction: {0}")]
    Retry(String),
    #[error("current transaction is aborted, commands ignored until end of transaction block")]
    InFailedTransaction,
    #[error("storage: {0}")]
    Kv(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("permission denied {0}")]
    PermissionDenied(String),
}

impl Error {
    /// The Postgres SQLSTATE code, which drivers and ORMs act on.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Parse(_) => "42601",
            Self::Unsupported(_) => "0A000",
            Self::InvalidInput(_) => "22P02",
            Self::UndefinedTable(_) => "42P01",
            Self::UndefinedColumn(_) => "42703",
            Self::UndefinedFunction(_) => "42883",
            Self::UndefinedObject(_) => "42704",
            Self::Ambiguous(_) => "42702",
            Self::Grouping(_) => "42803",
            Self::DuplicateTable(_) => "42P07",
            Self::DuplicateObject(_) => "42710",
            Self::UniqueViolation(_) => "23505",
            Self::NotNullViolation(..) => "23502",
            Self::DivisionByZero => "22012",
            Self::OutOfRange(_) => "22003",
            Self::Retry(_) => "40001",
            Self::InFailedTransaction => "25P02",
            Self::Kv(_) => "58000",
            Self::Internal(_) => "XX000",
            Self::PermissionDenied(_) => "42501",
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
