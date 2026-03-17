/// A simple object pool for reusable allocations.
///
/// Items are handed out via `get` (from the free-list when available, or
/// freshly allocated via the factory) and returned via `put`.  The pool
/// keeps at most `max_idle` items on the free-list to bound memory usage.
pub struct Pool<T> {
    free_list: Vec<T>,
    factory: Box<dyn Fn() -> T>,
    max_idle: usize,
}

impl<T> Pool<T> {
    pub fn new(max_idle: usize, factory: impl Fn() -> T + 'static) -> Self {
        Self {
            free_list: Vec::with_capacity(max_idle),
            factory: Box::new(factory),
            max_idle,
        }
    }

    /// Pre-populate the pool with `n` items (capped at `max_idle`).
    pub fn prefill(&mut self, n: usize) {
        let n = n.min(self.max_idle);
        while self.free_list.len() < n {
            let item = (self.factory)();
            self.free_list.push(item);
        }
    }

    /// Obtain an item — recycled if available, otherwise freshly created.
    pub fn get(&mut self) -> T {
        self.free_list.pop().unwrap_or_else(|| (self.factory)())
    }

    /// Return an item to the pool for reuse.  Dropped if the pool is full.
    pub fn put(&mut self, item: T) {
        if self.free_list.len() < self.max_idle {
            self.free_list.push(item);
        }
        // else: item is dropped
    }

    /// Number of items currently idle in the pool.
    pub fn idle_count(&self) -> usize {
        self.free_list.len()
    }

    /// Discard all pooled items.
    pub fn clear(&mut self) {
        self.free_list.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_factory_item_when_empty() {
        let mut pool = Pool::new(4, || vec![0u8; 4096]);
        let buf = pool.get();
        assert_eq!(buf.len(), 4096);
    }

    #[test]
    fn put_and_get_recycles() {
        let mut pool = Pool::new(4, || Vec::<u8>::new());
        let mut buf = pool.get();
        buf.extend_from_slice(&[1, 2, 3]);
        pool.put(buf);
        assert_eq!(pool.idle_count(), 1);
        let recycled = pool.get();
        assert_eq!(recycled, vec![1, 2, 3]);
    }

    #[test]
    fn respects_max_idle() {
        let mut pool = Pool::new(2, || 0u32);
        pool.put(1);
        pool.put(2);
        pool.put(3); // should be dropped
        assert_eq!(pool.idle_count(), 2);
    }

    #[test]
    fn prefill_populates() {
        let mut pool = Pool::new(8, || Vec::<u8>::with_capacity(1024));
        pool.prefill(4);
        assert_eq!(pool.idle_count(), 4);
    }
}
