// ---------------------------------------------------------------------------
// Savepoint
// ---------------------------------------------------------------------------

/// A savepoint captures a position in the transaction's write/read sets so
/// that a partial rollback can discard only the work done after the savepoint.
#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    /// Index into `Transaction::write_set` at creation time.
    pub write_set_position: usize,
    /// Index into `Transaction::read_set` at creation time.
    pub read_set_position: usize,
}

impl Savepoint {
    pub fn new(name: String, write_set_position: usize, read_set_position: usize) -> Self {
        Self {
            name,
            write_set_position,
            read_set_position,
        }
    }
}
