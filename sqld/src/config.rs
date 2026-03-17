use std::path::Path;

use serde::Deserialize;

use crate::utils::error::{ConfigError, Error};

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// Configuration loaded from `sqld_config.toml`.
///
/// Every section is optional and falls back to sensible defaults when absent.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub buffer_pool: BufferPoolConfig,
    pub wal: WalConfig,
    pub optimizer: OptimizerConfig,
    pub transactions: TransactionConfig,
    pub vacuum: VacuumConfig,
    pub logging: LoggingConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            buffer_pool: BufferPoolConfig::default(),
            wal: WalConfig::default(),
            optimizer: OptimizerConfig::default(),
            transactions: TransactionConfig::default(),
            vacuum: VacuumConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Config {
    /// Parse a TOML string into a [`Config`].
    pub fn from_str(toml_str: &str) -> Result<Self, Error> {
        toml::from_str(toml_str).map_err(|e| ConfigError::ParseError(e.to_string()).into())
    }

    /// Read and parse the config from a file path.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|_| {
            ConfigError::FileNotFound(path.display().to_string())
        })?;
        Self::from_str(&content)
    }
}

// ---------------------------------------------------------------------------
// Section: server
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5433,
            max_connections: 128,
        }
    }
}

// ---------------------------------------------------------------------------
// Section: storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    pub data_dir: String,
    pub page_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "data".into(),
            page_size: 8192,
        }
    }
}

// ---------------------------------------------------------------------------
// Section: buffer_pool
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BufferPoolConfig {
    /// Number of pages held in the buffer pool.
    pub pool_size: usize,
    /// Eviction policy (only "lru-k" currently).
    pub eviction_policy: String,
    /// K parameter for LRU-K.
    pub lru_k: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            pool_size: 1024,
            eviction_policy: "lru-k".into(),
            lru_k: 2,
        }
    }
}

// ---------------------------------------------------------------------------
// Section: wal
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    pub enabled: bool,
    /// Checkpoint interval in seconds.
    pub checkpoint_interval_secs: u64,
    /// Maximum WAL size in bytes before forced checkpoint.
    pub max_wal_size: u64,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            checkpoint_interval_secs: 300,
            max_wal_size: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

// ---------------------------------------------------------------------------
// Section: optimizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OptimizerConfig {
    pub enable_cost_based: bool,
    pub statistics_sample_size: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            enable_cost_based: true,
            statistics_sample_size: 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// Section: transactions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TransactionConfig {
    /// Default isolation level: "snapshot" or "serializable".
    pub isolation_level: String,
    /// Lock wait timeout in milliseconds.
    pub lock_timeout_ms: u64,
    /// Deadlock detection interval in milliseconds.
    pub deadlock_detection_interval_ms: u64,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            isolation_level: "snapshot".into(),
            lock_timeout_ms: 5000,
            deadlock_detection_interval_ms: 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// Section: vacuum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VacuumConfig {
    pub auto_vacuum: bool,
    /// Fraction of dead tuples before auto-vacuum triggers (0.0–1.0).
    pub vacuum_threshold: f64,
}

impl Default for VacuumConfig {
    fn default() -> Self {
        Self {
            auto_vacuum: true,
            vacuum_threshold: 0.2,
        }
    }
}

// ---------------------------------------------------------------------------
// Section: logging
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// "trace", "debug", "info", "warn", "error"
    pub level: String,
    /// Log file path; empty means stderr.
    pub file: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            file: String::new(),
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.server.port, 5433);
        assert_eq!(cfg.storage.page_size, 8192);
        assert_eq!(cfg.buffer_pool.pool_size, 1024);
        assert!(cfg.wal.enabled);
        assert!(cfg.optimizer.enable_cost_based);
        assert_eq!(cfg.transactions.isolation_level, "snapshot");
        assert!(cfg.vacuum.auto_vacuum);
        assert_eq!(cfg.logging.level, "info");
    }

    #[test]
    fn parse_empty_string() {
        // An empty TOML string should produce default config.
        let cfg = Config::from_str("").unwrap();
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 5433);
    }

    #[test]
    fn parse_partial_override() {
        let toml = r#"
[server]
port = 9999

[storage]
data_dir = "/var/sqld"
page_size = 16384

[buffer_pool]
pool_size = 4096
"#;
        let cfg = Config::from_str(toml).unwrap();
        assert_eq!(cfg.server.port, 9999);
        assert_eq!(cfg.server.host, "127.0.0.1"); // default preserved
        assert_eq!(cfg.storage.data_dir, "/var/sqld");
        assert_eq!(cfg.storage.page_size, 16384);
        assert_eq!(cfg.buffer_pool.pool_size, 4096);
        // Sections not mentioned keep defaults
        assert!(cfg.wal.enabled);
    }

    #[test]
    fn parse_full_config() {
        let toml = r#"
[server]
host = "0.0.0.0"
port = 5432
max_connections = 256

[storage]
data_dir = "/data/sqld"
page_size = 4096

[buffer_pool]
pool_size = 8192
eviction_policy = "lru-k"
lru_k = 3

[wal]
enabled = true
checkpoint_interval_secs = 120
max_wal_size = 134217728

[optimizer]
enable_cost_based = true
statistics_sample_size = 5000

[transactions]
isolation_level = "serializable"
lock_timeout_ms = 10000
deadlock_detection_interval_ms = 500

[vacuum]
auto_vacuum = false
vacuum_threshold = 0.5

[logging]
level = "debug"
file = "/var/log/sqld.log"
"#;
        let cfg = Config::from_str(toml).unwrap();
        assert_eq!(cfg.server.host, "0.0.0.0");
        assert_eq!(cfg.server.port, 5432);
        assert_eq!(cfg.server.max_connections, 256);
        assert_eq!(cfg.storage.data_dir, "/data/sqld");
        assert_eq!(cfg.storage.page_size, 4096);
        assert_eq!(cfg.buffer_pool.pool_size, 8192);
        assert_eq!(cfg.buffer_pool.lru_k, 3);
        assert!(cfg.wal.enabled);
        assert_eq!(cfg.wal.checkpoint_interval_secs, 120);
        assert_eq!(cfg.wal.max_wal_size, 134_217_728);
        assert!(cfg.optimizer.enable_cost_based);
        assert_eq!(cfg.optimizer.statistics_sample_size, 5000);
        assert_eq!(cfg.transactions.isolation_level, "serializable");
        assert_eq!(cfg.transactions.lock_timeout_ms, 10000);
        assert_eq!(cfg.transactions.deadlock_detection_interval_ms, 500);
        assert!(!cfg.vacuum.auto_vacuum);
        assert!((cfg.vacuum.vacuum_threshold - 0.5).abs() < f64::EPSILON);
        assert_eq!(cfg.logging.level, "debug");
        assert_eq!(cfg.logging.file, "/var/log/sqld.log");
    }

    #[test]
    fn parse_invalid_toml() {
        let bad = "this is [[[not valid toml";
        assert!(Config::from_str(bad).is_err());
    }

    #[test]
    fn file_not_found() {
        assert!(Config::from_file("/nonexistent/sqld_config.toml").is_err());
    }
}
