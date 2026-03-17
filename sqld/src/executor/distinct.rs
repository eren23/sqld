use std::collections::HashSet;

use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::Executor;

// ---------------------------------------------------------------------------
// HashDistinct — duplicate elimination using a hash set
// ---------------------------------------------------------------------------

pub struct HashDistinctExecutor {
    child: Box<dyn Executor>,
    schema: Schema,
    seen: HashSet<Vec<DatumKey>>,
    initialized: bool,
}

impl HashDistinctExecutor {
    pub fn new(child: Box<dyn Executor>) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            seen: HashSet::new(),
            initialized: false,
        }
    }
}

impl Executor for HashDistinctExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        self.seen.clear();
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    let key: Vec<DatumKey> =
                        tuple.values().iter().cloned().map(DatumKey).collect();
                    if self.seen.insert(key) {
                        return Ok(Some(tuple));
                    }
                    // Already seen, skip
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.seen.clear();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---------------------------------------------------------------------------
// SortDistinct — assumes input is sorted, eliminates consecutive duplicates
// ---------------------------------------------------------------------------

pub struct SortDistinctExecutor {
    child: Box<dyn Executor>,
    schema: Schema,
    last: Option<Vec<Datum>>,
    initialized: bool,
}

impl SortDistinctExecutor {
    pub fn new(child: Box<dyn Executor>) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            schema,
            last: None,
            initialized: false,
        }
    }
}

impl Executor for SortDistinctExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        self.last = None;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    let vals = tuple.values().to_vec();
                    if let Some(ref prev) = self.last {
                        if datums_equal(prev, &vals) {
                            continue; // Duplicate, skip
                        }
                    }
                    self.last = Some(vals);
                    return Ok(Some(tuple));
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.last = None;
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn datums_equal(a: &[Datum], b: &[Datum]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|(da, db)| da == db)
}

/// Wrapper for Datum that implements Hash+Eq for use in HashSet.
#[derive(Clone, Debug)]
pub struct DatumKey(pub Datum);

impl std::hash::Hash for DatumKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl PartialEq for DatumKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for DatumKey {}
