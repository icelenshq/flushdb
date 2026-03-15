use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue, FlushResult};

use flushdb_engine::block_fetcher::BlockFetcher;
use flushdb_engine::cache::{
    BlockCache, CacheConfig, CachingBlockFetcher, sst_id_from_path,
};
use flushdb_engine::sstable::{BlockEntry, CompressionType};

fn make_entry(record_id: &str, item_key: &str, value: &[u8]) -> BlockEntry {
    BlockEntry {
        composite_key: CompositeKey::new(record_id.as_bytes(), item_key.as_bytes())
            .expect("valid composite key"),
        value: EntryValue::Inline(Bytes::copy_from_slice(value)),
        metadata: Bytes::new(),
        entry_type: EntryType::Put,
        sequence_number: 1,
    }
}

struct CountingFetcher {
    fetch_count: Arc<AtomicU32>,
    raw_fetch_count: Arc<AtomicU32>,
}

impl CountingFetcher {
    fn new() -> (Self, Arc<AtomicU32>, Arc<AtomicU32>) {
        let fetch_count = Arc::new(AtomicU32::new(0));
        let raw_fetch_count = Arc::new(AtomicU32::new(0));
        (
            Self {
                fetch_count: Arc::clone(&fetch_count),
                raw_fetch_count: Arc::clone(&raw_fetch_count),
            },
            fetch_count,
            raw_fetch_count,
        )
    }
}

#[async_trait]
impl BlockFetcher for CountingFetcher {
    async fn fetch_block(
        &self,
        _sst_path: &str,
        _offset: u64,
        _size: u32,
        _compression: CompressionType,
    ) -> FlushResult<Vec<BlockEntry>> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(vec![make_entry("rec1", "item1", b"value1")])
    }

    async fn fetch_raw_block(
        &self,
        _sst_path: &str,
        _offset: u64,
        _size: u32,
    ) -> FlushResult<Bytes> {
        self.raw_fetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(Bytes::from_static(b"raw-block-data"))
    }
}

#[test]
fn test_sst_id_from_l0_path() {
    assert_eq!(
        sst_id_from_path("flushdb/ns/sstables/L0/abc123.sst"),
        "abc123"
    );
}

#[test]
fn test_sst_id_from_fragment_path() {
    assert_eq!(
        sst_id_from_path("flushdb/ns/sstables/L1/run-xyz/frag-0001.sst"),
        "frag-0001"
    );
}

#[test]
fn test_sst_id_from_simple_filename() {
    assert_eq!(sst_id_from_path("data.sst"), "data");
}

#[test]
fn test_sst_id_no_extension() {
    assert_eq!(sst_id_from_path("noext"), "noext");
}

#[tokio::test]
async fn test_cache_miss_then_hit() {
    let (fetcher, fetch_count, _raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);

    let entries = caching
        .fetch_block("data/L0/sst-1.sst", 0, 4096, CompressionType::None)
        .await
        .expect("first fetch should succeed");
    assert_eq!(entries.len(), 1);
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    let entries = caching
        .fetch_block("data/L0/sst-1.sst", 0, 4096, CompressionType::None)
        .await
        .expect("second fetch should succeed");
    assert_eq!(entries.len(), 1);
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        1,
        "inner fetcher should not be called on cache hit"
    );
}

#[tokio::test]
async fn test_raw_block_not_cached() {
    let (fetcher, _fetch_count, raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);

    let data = caching
        .fetch_raw_block("data/L0/sst-1.sst", 0, 4096)
        .await
        .expect("first raw fetch should succeed");
    assert_eq!(&data[..], b"raw-block-data");
    assert_eq!(raw_count.load(Ordering::SeqCst), 1);

    let data = caching
        .fetch_raw_block("data/L0/sst-1.sst", 0, 4096)
        .await
        .expect("second raw fetch should succeed");
    assert_eq!(&data[..], b"raw-block-data");
    assert_eq!(
        raw_count.load(Ordering::SeqCst),
        2,
        "inner fetcher should be called every time for raw blocks"
    );
}

#[tokio::test]
async fn test_different_blocks_independent() {
    let (fetcher, fetch_count, _raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);

    // Fetch block A
    caching
        .fetch_block("data/L0/sst-1.sst", 0, 4096, CompressionType::None)
        .await
        .expect("fetch block A should succeed");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    // Fetch block B (different offset) — should miss cache
    caching
        .fetch_block("data/L0/sst-1.sst", 4096, 4096, CompressionType::None)
        .await
        .expect("fetch block B should succeed");
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        2,
        "different block offsets should be independent cache entries"
    );

    // Fetch block A again — should hit cache
    caching
        .fetch_block("data/L0/sst-1.sst", 0, 4096, CompressionType::None)
        .await
        .expect("re-fetch block A should succeed");
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        2,
        "block A should still be cached"
    );

    // Fetch block B again — should hit cache
    caching
        .fetch_block("data/L0/sst-1.sst", 4096, 4096, CompressionType::None)
        .await
        .expect("re-fetch block B should succeed");
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        2,
        "block B should still be cached"
    );
}
