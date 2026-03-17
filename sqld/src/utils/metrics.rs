use std::sync::atomic::{AtomicU64, Ordering};

/// A lock-free monotonic counter backed by an atomic u64.
pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.0.store(0, Ordering::Relaxed);
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Counter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Counter").field(&self.get()).finish()
    }
}

/// Global performance counters for the database engine.
pub struct Metrics {
    // Storage
    pub pages_read: Counter,
    pub pages_written: Counter,
    pub cache_hits: Counter,
    pub cache_misses: Counter,

    // Executor
    pub rows_scanned: Counter,
    pub rows_returned: Counter,

    // Transactions
    pub transactions_committed: Counter,
    pub transactions_aborted: Counter,

    // WAL
    pub wal_bytes_written: Counter,
    pub checkpoints_completed: Counter,

    // Buffer pool
    pub buffer_pool_evictions: Counter,
}

impl Metrics {
    pub const fn new() -> Self {
        Self {
            pages_read: Counter::new(),
            pages_written: Counter::new(),
            cache_hits: Counter::new(),
            cache_misses: Counter::new(),
            rows_scanned: Counter::new(),
            rows_returned: Counter::new(),
            transactions_committed: Counter::new(),
            transactions_aborted: Counter::new(),
            wal_bytes_written: Counter::new(),
            checkpoints_completed: Counter::new(),
            buffer_pool_evictions: Counter::new(),
        }
    }

    /// Reset every counter to zero.
    pub fn reset_all(&self) {
        self.pages_read.reset();
        self.pages_written.reset();
        self.cache_hits.reset();
        self.cache_misses.reset();
        self.rows_scanned.reset();
        self.rows_returned.reset();
        self.transactions_committed.reset();
        self.transactions_aborted.reset();
        self.wal_bytes_written.reset();
        self.checkpoints_completed.reset();
        self.buffer_pool_evictions.reset();
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_basic_ops() {
        let c = Counter::new();
        assert_eq!(c.get(), 0);
        c.increment();
        assert_eq!(c.get(), 1);
        c.add(9);
        assert_eq!(c.get(), 10);
        c.reset();
        assert_eq!(c.get(), 0);
    }

    #[test]
    fn metrics_reset_all() {
        let m = Metrics::new();
        m.pages_read.add(100);
        m.rows_scanned.add(500);
        m.reset_all();
        assert_eq!(m.pages_read.get(), 0);
        assert_eq!(m.rows_scanned.get(), 0);
    }
}
