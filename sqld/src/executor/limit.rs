use crate::types::{Schema, Tuple};
use crate::utils::error::Result;

use super::executor::Executor;

// ---------------------------------------------------------------------------
// Limit + Offset
// ---------------------------------------------------------------------------

pub struct LimitExecutor {
    child: Box<dyn Executor>,
    count: Option<usize>,
    offset: usize,
    schema: Schema,
    skipped: usize,
    returned: usize,
    initialized: bool,
}

impl LimitExecutor {
    pub fn new(
        child: Box<dyn Executor>,
        count: Option<usize>,
        offset: usize,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            count,
            offset,
            schema,
            skipped: 0,
            returned: 0,
            initialized: false,
        }
    }
}

impl Executor for LimitExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        self.skipped = 0;
        self.returned = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        // Check if we've hit the count limit
        if let Some(limit) = self.count {
            if self.returned >= limit {
                return Ok(None);
            }
        }

        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    // Skip offset rows first
                    if self.skipped < self.offset {
                        self.skipped += 1;
                        continue;
                    }
                    self.returned += 1;
                    return Ok(Some(tuple));
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
