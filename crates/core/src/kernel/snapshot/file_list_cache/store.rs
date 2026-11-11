//! Where the cache keeps its entries.

use bytes::Bytes;

use crate::DeltaResult;

/// A key-value store for cache entries.
///
/// The store can lose any entry at any time. It must not return a value for a key other than
/// a value that was put under that key.
#[async_trait::async_trait]
pub(super) trait CacheStore: std::fmt::Debug + Send + Sync {
    async fn get(&self, key: &str) -> DeltaResult<Option<Bytes>>;

    /// Stores `value`. When this returns, the store has accepted or dropped the value.
    async fn put(&self, key: String, value: Bytes) -> DeltaResult<()>;

    async fn remove(&self, key: &str) -> DeltaResult<()>;

    /// Stops the store. Later calls can fail.
    async fn close(&self) {}
}

#[cfg(test)]
pub(super) mod memory {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A store for tests. It keeps all entries and counts the calls.
    #[derive(Debug, Default)]
    pub(in super::super) struct MemoryStore {
        entries: Mutex<HashMap<String, Bytes>>,
        puts: AtomicUsize,
    }

    impl MemoryStore {
        pub(in super::super) fn keys(&self) -> Vec<String> {
            let mut keys: Vec<_> = self.entries.lock().unwrap().keys().cloned().collect();
            keys.sort();
            keys
        }

        pub(in super::super) fn puts(&self) -> usize {
            self.puts.load(Ordering::SeqCst)
        }

        pub(in super::super) fn forget(&self, key: &str) {
            self.entries.lock().unwrap().remove(key);
        }
    }

    #[async_trait::async_trait]
    impl CacheStore for MemoryStore {
        async fn get(&self, key: &str) -> DeltaResult<Option<Bytes>> {
            Ok(self.entries.lock().unwrap().get(key).cloned())
        }

        async fn put(&self, key: String, value: Bytes) -> DeltaResult<()> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            self.entries.lock().unwrap().insert(key, value);
            Ok(())
        }

        async fn remove(&self, key: &str) -> DeltaResult<()> {
            self.forget(key);
            Ok(())
        }
    }
}
