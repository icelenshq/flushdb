use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue, FlushError, FlushResult};

use flushdb_engine::block_fetcher::BlockFetcher;
use flushdb_engine::cache::{BlockCache, CacheConfig, CachingBlockFetcher};
use flushdb_engine::sstable::{BlockEntry, CompressionType};
use flushdb_engine::ReadBudget;

fn make_test_entry(record_id: &str, item_key: &str, value: &[u8]) -> BlockEntry {
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
}

impl CountingFetcher {
    fn new() -> (Self, Arc<AtomicU32>) {
        let fetch_count = Arc::new(AtomicU32::new(0));
        (
            Self {
                fetch_count: Arc::clone(&fetch_count),
            },
            fetch_count,
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
        Ok(vec![make_test_entry("rec1", "item1", b"value1")])
    }

    async fn fetch_raw_block(
        &self,
        _sst_path: &str,
        _offset: u64,
        _size: u32,
    ) -> FlushResult<Bytes> {
        Ok(Bytes::from_static(b"raw"))
    }
}

#[test]
fn test_budget_spend() {
    let mut budget = ReadBudget::new(3);

    assert!(budget.try_spend());
    assert!(budget.try_spend());
    assert!(budget.try_spend());
    assert!(!budget.try_spend());
}

#[test]
fn test_budget_exhausted_flag() {
    let mut budget = ReadBudget::new(1);

    assert!(!budget.is_exhausted());
    assert!(budget.try_spend());
    assert!(!budget.is_exhausted());

    assert!(!budget.try_spend());
    assert!(budget.is_exhausted());
}

#[test]
fn test_budget_used_tracking() {
    let mut budget = ReadBudget::new(8);
    budget.spend(3);

    assert_eq!(budget.used(), 3);
    assert_eq!(budget.remaining(), 5);
}

#[test]
fn test_budget_spend_multiple() {
    let mut budget = ReadBudget::new(5);
    budget.spend(3);

    assert_eq!(budget.remaining(), 2);
    assert!(!budget.is_exhausted());
}

#[test]
fn test_budget_spend_saturating() {
    let mut budget = ReadBudget::new(3);
    budget.spend(10);

    assert_eq!(budget.remaining(), 0);
    assert!(budget.is_exhausted());
}

#[test]
fn test_budget_fresh() {
    let budget = ReadBudget::new(5);

    assert_eq!(budget.remaining(), 5);
    assert_eq!(budget.used(), 0);
    assert!(!budget.is_exhausted());
}

#[tokio::test]
async fn test_cache_hit_does_not_consume_budget() {
    let (fetcher, fetch_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);

    // Populate the cache with a normal (non-budgeted) fetch
    caching
        .fetch_block("data/L0/sst-1.sst", 0, 4096, CompressionType::None)
        .await
        .expect("initial fetch should succeed");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    // Now use budgeted fetch — should hit cache, budget stays at 0 used
    let mut budget = ReadBudget::new(5);
    let entries = caching
        .fetch_block_budgeted(
            "data/L0/sst-1.sst",
            0,
            4096,
            CompressionType::None,
            &mut budget,
        )
        .await
        .expect("budgeted fetch with cache hit should succeed");

    assert_eq!(entries.len(), 1);
    assert_eq!(budget.used(), 0, "cache hit should not consume budget");
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        1,
        "inner fetcher should not be called on cache hit"
    );
}

#[tokio::test]
async fn test_cache_miss_consumes_budget() {
    let (fetcher, fetch_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);

    let mut budget = ReadBudget::new(5);
    let entries = caching
        .fetch_block_budgeted(
            "data/L0/sst-1.sst",
            0,
            4096,
            CompressionType::None,
            &mut budget,
        )
        .await
        .expect("budgeted fetch with cache miss should succeed");

    assert_eq!(entries.len(), 1);
    assert_eq!(budget.used(), 1, "cache miss should consume 1 budget unit");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_budget_exhausted_returns_error() {
    let (fetcher, _fetch_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);

    let mut budget = ReadBudget::new(1);

    // First fetch succeeds — uses the single budget unit
    caching
        .fetch_block_budgeted(
            "data/L0/sst-1.sst",
            0,
            4096,
            CompressionType::None,
            &mut budget,
        )
        .await
        .expect("first budgeted fetch should succeed");
    assert_eq!(budget.used(), 1);

    // Second fetch to a different block should fail — budget exhausted
    let result = caching
        .fetch_block_budgeted(
            "data/L0/sst-1.sst",
            4096,
            4096,
            CompressionType::None,
            &mut budget,
        )
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::ResourceExhausted { resource, .. } => {
            assert_eq!(resource, "GET budget");
        }
        e => panic!("expected ResourceExhausted, got {:?}", e),
    }
}
