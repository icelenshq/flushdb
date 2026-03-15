use async_trait::async_trait;
use bytes::Bytes;

use crate::error::FlushResult;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Overwrites if exists. Creates intermediate path components.
    async fn put(&self, key: &str, value: Bytes) -> FlushResult<()>;

    /// Returns `NotFound` if key doesn't exist.
    async fn get(&self, key: &str) -> FlushResult<Bytes>;

    /// Returns `NotFound` if key doesn't exist, even when length is 0.
    /// If range extends beyond object, returns available bytes (partial content).
    /// If offset is at or beyond file end with length > 0, returns error.
    async fn get_range(&self, key: &str, offset: u64, length: u64) -> FlushResult<Bytes>;

    /// Idempotent — deleting a non-existent key succeeds silently.
    async fn delete(&self, key: &str) -> FlushResult<()>;

    /// CAS write — succeeds only if key does NOT exist.
    /// Returns `PreconditionFailed` if key already exists.
    async fn conditional_put(&self, key: &str, value: Bytes) -> FlushResult<()>;

    /// Returns all keys matching prefix, lexicographically sorted.
    /// Returns empty vec if no keys match.
    async fn list_prefix(&self, prefix: &str) -> FlushResult<Vec<String>>;
}
