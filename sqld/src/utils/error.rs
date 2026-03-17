use std::fmt;
use std::io;

// ---------------------------------------------------------------------------
// Sub-error categories
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum TypeError {
    InvalidCoercion { from: String, to: String },
    TypeMismatch { expected: String, found: String },
    ArithmeticOverflow,
    DivisionByZero,
    InvalidComparison { lhs: String, rhs: String },
}

#[derive(Debug)]
pub enum ConfigError {
    FileNotFound(String),
    ParseError(String),
    InvalidValue { key: String, reason: String },
}

#[derive(Debug)]
pub enum StorageError {
    PageFull,
    InvalidPageId(u64),
    CorruptedPage { page_id: u64, reason: String },
    BufferPoolExhausted,
    DuplicateKey,
    KeyNotFound,
    BTreeCorrupted(String),
}

#[derive(Debug)]
pub enum WalError {
    LogCorrupted(String),
    CheckpointFailed(String),
}

#[derive(Debug)]
pub enum TransactionError {
    SerializationFailure { txn_id: u64 },
    DeadlockDetected { txn_id: u64 },
    LockTimeout { txn_id: u64 },
}

#[derive(Debug)]
pub enum SqlError {
    ParseError(String),
    PlanError(String),
    ExecutionError(String),
}

// ---------------------------------------------------------------------------
// Unified Error
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum Error {
    Type(TypeError),
    Config(ConfigError),
    Storage(StorageError),
    Wal(WalError),
    Transaction(TransactionError),
    Sql(SqlError),
    Io(io::Error),
    Serialization(String),
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

// ---------------------------------------------------------------------------
// Display implementations
// ---------------------------------------------------------------------------

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeError::InvalidCoercion { from, to } => {
                write!(f, "cannot coerce {from} to {to}")
            }
            TypeError::TypeMismatch { expected, found } => {
                write!(f, "type mismatch: expected {expected}, found {found}")
            }
            TypeError::ArithmeticOverflow => write!(f, "arithmetic overflow"),
            TypeError::DivisionByZero => write!(f, "division by zero"),
            TypeError::InvalidComparison { lhs, rhs } => {
                write!(f, "cannot compare {lhs} with {rhs}")
            }
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::FileNotFound(path) => write!(f, "config file not found: {path}"),
            ConfigError::ParseError(msg) => write!(f, "config parse error: {msg}"),
            ConfigError::InvalidValue { key, reason } => {
                write!(f, "invalid config value for '{key}': {reason}")
            }
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::PageFull => write!(f, "page is full"),
            StorageError::InvalidPageId(id) => write!(f, "invalid page id: {id}"),
            StorageError::CorruptedPage { page_id, reason } => {
                write!(f, "corrupted page {page_id}: {reason}")
            }
            StorageError::BufferPoolExhausted => write!(f, "buffer pool exhausted"),
            StorageError::DuplicateKey => write!(f, "duplicate key"),
            StorageError::KeyNotFound => write!(f, "key not found"),
            StorageError::BTreeCorrupted(msg) => write!(f, "B+ tree corrupted: {msg}"),
        }
    }
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::LogCorrupted(msg) => write!(f, "WAL log corrupted: {msg}"),
            WalError::CheckpointFailed(msg) => write!(f, "checkpoint failed: {msg}"),
        }
    }
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionError::SerializationFailure { txn_id } => {
                write!(f, "serialization failure for txn {txn_id}")
            }
            TransactionError::DeadlockDetected { txn_id } => {
                write!(f, "deadlock detected for txn {txn_id}")
            }
            TransactionError::LockTimeout { txn_id } => {
                write!(f, "lock timeout for txn {txn_id}")
            }
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::ParseError(msg) => write!(f, "SQL parse error: {msg}"),
            SqlError::PlanError(msg) => write!(f, "SQL plan error: {msg}"),
            SqlError::ExecutionError(msg) => write!(f, "SQL execution error: {msg}"),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Type(e) => write!(f, "type error: {e}"),
            Error::Config(e) => write!(f, "config error: {e}"),
            Error::Storage(e) => write!(f, "storage error: {e}"),
            Error::Wal(e) => write!(f, "WAL error: {e}"),
            Error::Transaction(e) => write!(f, "transaction error: {e}"),
            Error::Sql(e) => write!(f, "SQL error: {e}"),
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Serialization(msg) => write!(f, "serialization error: {msg}"),
            Error::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// From conversions
// ---------------------------------------------------------------------------

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<TypeError> for Error {
    fn from(e: TypeError) -> Self {
        Error::Type(e)
    }
}

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Error::Config(e)
    }
}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::Storage(e)
    }
}

impl From<WalError> for Error {
    fn from(e: WalError) -> Self {
        Error::Wal(e)
    }
}

impl From<TransactionError> for Error {
    fn from(e: TransactionError) -> Self {
        Error::Transaction(e)
    }
}

impl From<SqlError> for Error {
    fn from(e: SqlError) -> Self {
        Error::Sql(e)
    }
}
