use std::collections::HashMap;

use crate::types::{Schema, Tuple};
use crate::utils::error::Result;

use super::distinct::DatumKey;
use super::executor::Executor;

// ---------------------------------------------------------------------------
// Union (ALL and DISTINCT)
// ---------------------------------------------------------------------------

pub struct UnionExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    all: bool,
    schema: Schema,
    reading_left: bool,
    seen: Option<std::collections::HashSet<Vec<DatumKey>>>,
    initialized: bool,
}

impl UnionExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        all: bool,
    ) -> Self {
        let schema = left.schema().clone();
        Self {
            left,
            right,
            all,
            schema,
            reading_left: true,
            seen: None,
            initialized: false,
        }
    }
}

impl Executor for UnionExecutor {
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;
        self.reading_left = true;
        if !self.all {
            self.seen = Some(std::collections::HashSet::new());
        }
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            let tuple = if self.reading_left {
                match self.left.next()? {
                    Some(t) => t,
                    None => {
                        self.reading_left = false;
                        continue;
                    }
                }
            } else {
                match self.right.next()? {
                    Some(t) => t,
                    None => return Ok(None),
                }
            };

            // UNION ALL: return all rows
            if self.all {
                return Ok(Some(tuple));
            }

            // UNION DISTINCT: deduplicate
            let key: Vec<DatumKey> =
                tuple.values().iter().cloned().map(DatumKey).collect();
            if self.seen.as_mut().unwrap().insert(key) {
                return Ok(Some(tuple));
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.seen = None;
        self.left.close()?;
        self.right.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---------------------------------------------------------------------------
// Intersect (ALL and DISTINCT)
// ---------------------------------------------------------------------------

pub struct IntersectExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    all: bool,
    schema: Schema,
    results: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl IntersectExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        all: bool,
    ) -> Self {
        let schema = left.schema().clone();
        Self {
            left,
            right,
            all,
            schema,
            results: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for IntersectExecutor {
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;

        // Build a bag (multiset) from the right side
        let mut right_bag: HashMap<Vec<DatumKey>, usize> = HashMap::new();
        while let Some(t) = self.right.next()? {
            let key: Vec<DatumKey> =
                t.values().iter().cloned().map(DatumKey).collect();
            *right_bag.entry(key).or_insert(0) += 1;
        }

        // Scan left side, emit rows that appear in right
        self.results.clear();
        if self.all {
            // INTERSECT ALL: emit min(left_count, right_count) copies
            while let Some(t) = self.left.next()? {
                let key: Vec<DatumKey> =
                    t.values().iter().cloned().map(DatumKey).collect();
                if let Some(count) = right_bag.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        self.results.push(t);
                    }
                }
            }
        } else {
            // INTERSECT DISTINCT: emit at most one copy
            let mut seen = std::collections::HashSet::new();
            while let Some(t) = self.left.next()? {
                let key: Vec<DatumKey> =
                    t.values().iter().cloned().map(DatumKey).collect();
                if right_bag.contains_key(&key) && seen.insert(key) {
                    self.results.push(t);
                }
            }
        }

        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.position < self.results.len() {
            let t = self.results[self.position].clone();
            self.position += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.results.clear();
        self.left.close()?;
        self.right.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

// ---------------------------------------------------------------------------
// Except (ALL and DISTINCT)
// ---------------------------------------------------------------------------

pub struct ExceptExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    all: bool,
    schema: Schema,
    results: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl ExceptExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        all: bool,
    ) -> Self {
        let schema = left.schema().clone();
        Self {
            left,
            right,
            all,
            schema,
            results: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for ExceptExecutor {
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;

        // Build a bag from the right side
        let mut right_bag: HashMap<Vec<DatumKey>, usize> = HashMap::new();
        while let Some(t) = self.right.next()? {
            let key: Vec<DatumKey> =
                t.values().iter().cloned().map(DatumKey).collect();
            *right_bag.entry(key).or_insert(0) += 1;
        }

        // Scan left side, emit rows NOT in right (or with excess count)
        self.results.clear();
        if self.all {
            // EXCEPT ALL: subtract right counts from left
            while let Some(t) = self.left.next()? {
                let key: Vec<DatumKey> =
                    t.values().iter().cloned().map(DatumKey).collect();
                if let Some(count) = right_bag.get_mut(&key) {
                    if *count > 0 {
                        *count -= 1;
                        continue; // Consumed by right side
                    }
                }
                self.results.push(t);
            }
        } else {
            // EXCEPT DISTINCT: emit left rows not in right, deduplicated
            let mut seen = std::collections::HashSet::new();
            while let Some(t) = self.left.next()? {
                let key: Vec<DatumKey> =
                    t.values().iter().cloned().map(DatumKey).collect();
                if !right_bag.contains_key(&key) && seen.insert(key) {
                    self.results.push(t);
                }
            }
        }

        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.position < self.results.len() {
            let t = self.results[self.position].clone();
            self.position += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.results.clear();
        self.left.close()?;
        self.right.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
