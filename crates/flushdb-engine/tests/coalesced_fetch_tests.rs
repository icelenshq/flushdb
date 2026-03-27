use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue, FlushResult, IdempotencyToken};

use flushdb_engine::block_fetcher::BlockFetcher;
use flushdb_engine::cache::{
    is_adjacent, BlockCache, BlockRequest, CacheConfig, CachingBlockFetcher, CoalescingFetcher,
};
use flushdb_engine::sstable::{BlockBuilder, BlockEntry, CompressionType};

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
fn test_adjacent_blocks() {
    let a = BlockRequest {
        block_index: 0,
        block_offset: 0,
        block_size: 100,
    };
    let b = BlockRequest {
        block_index: 1,
        block_offset: 100,
        block_size: 100,
    };
    assert!(is_adjacent(&a, &b));
}

#[test]
fn test_non_adjacent_blocks() {
    let a = BlockRequest {
        block_index: 0,
        block_offset: 0,
        block_size: 100,
    };
    let b = BlockRequest {
        block_index: 1,
        block_offset: 200,
        block_size: 100,
    };
    assert!(!is_adjacent(&a, &b));
}

#[test]
fn test_adjacent_blocks_reverse_not_adjacent() {
    let a = BlockRequest {
        block_index: 0,
        block_offset: 100,
        block_size: 100,
    };
    let b = BlockRequest {
        block_index: 1,
        block_offset: 0,
        block_size: 100,
    };
    assert!(
        !is_adjacent(&a, &b),
        "adjacency is directional: a must end where b starts"
    );
}

#[test]
fn test_adjacent_different_sizes() {
    let a = BlockRequest {
        block_index: 0,
        block_offset: 0,
        block_size: 50,
    };
    let b = BlockRequest {
        block_index: 1,
        block_offset: 50,
        block_size: 200,
    };
    assert!(is_adjacent(&a, &b));
}

#[tokio::test]
async fn test_empty_request_list() {
    let (fetcher, fetch_count, raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);
    let coalescing = CoalescingFetcher::new(caching);

    let result = coalescing
        .fetch_blocks("data/L0/sst-1.sst", &[], CompressionType::None)
        .await
        .expect("empty request list should succeed");

    assert!(result.blocks.is_empty());
    assert_eq!(fetch_count.load(Ordering::SeqCst), 0);
    assert_eq!(raw_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_single_block_uses_normal_path() {
    let (fetcher, fetch_count, raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);
    let coalescing = CoalescingFetcher::new(caching);

    let requests = vec![BlockRequest {
        block_index: 0,
        block_offset: 0,
        block_size: 4096,
    }];

    let result = coalescing
        .fetch_blocks("data/L0/sst-1.sst", &requests, CompressionType::None)
        .await
        .expect("single block fetch should succeed");

    assert_eq!(result.blocks.len(), 1);
    assert_eq!(result.blocks[0].0, 0);
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
    assert_eq!(
        raw_count.load(Ordering::SeqCst),
        0,
        "single block should use fetch_block, not fetch_raw_block"
    );
}

#[tokio::test]
async fn test_cached_blocks_skip_fetch() {
    let (fetcher, fetch_count, raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);
    let coalescing = CoalescingFetcher::new(caching.clone());

    // Pre-populate cache via a normal fetch
    caching
        .fetch_block("data/L0/sst-1.sst", 0, 4096, CompressionType::None)
        .await
        .expect("pre-populate should succeed");
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

    // Now request that same block via coalescing — should hit cache
    let requests = vec![BlockRequest {
        block_index: 0,
        block_offset: 0,
        block_size: 4096,
    }];
    let result = coalescing
        .fetch_blocks("data/L0/sst-1.sst", &requests, CompressionType::None)
        .await
        .expect("cached block fetch should succeed");

    assert_eq!(result.blocks.len(), 1);
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        1,
        "cached block should not trigger inner fetch"
    );
    assert_eq!(raw_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_results_sorted_by_block_index() {
    let (fetcher, _fetch_count, _raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);
    let coalescing = CoalescingFetcher::new(caching);

    // Use non-adjacent blocks so each goes through single-block fetch_block path
    let requests = vec![
        BlockRequest {
            block_index: 2,
            block_offset: 20000,
            block_size: 4096,
        },
        BlockRequest {
            block_index: 0,
            block_offset: 0,
            block_size: 4096,
        },
        BlockRequest {
            block_index: 1,
            block_offset: 10000,
            block_size: 4096,
        },
    ];

    let result = coalescing
        .fetch_blocks("data/L0/sst-1.sst", &requests, CompressionType::None)
        .await
        .expect("multi-block fetch should succeed");

    assert_eq!(result.blocks.len(), 3);
    assert_eq!(result.blocks[0].0, 0);
    assert_eq!(result.blocks[1].0, 1);
    assert_eq!(result.blocks[2].0, 2);
}

#[tokio::test]
async fn test_convenience_fetch_block_delegates() {
    let (fetcher, fetch_count, _raw_count) = CountingFetcher::new();
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);
    let coalescing = CoalescingFetcher::new(caching);

    let entries = coalescing
        .fetch_block("data/L0/sst-1.sst", 0, 4096, CompressionType::None)
        .await
        .expect("convenience fetch_block should succeed");

    assert_eq!(entries.len(), 1);
    assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
}

fn build_encoded_block(record_id: &str, item_key: &str, value: &[u8]) -> Bytes {
    let key =
        CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).expect("valid composite key");
    let mut builder = BlockBuilder::new(4096);
    builder.add_entry(
        &key,
        value,
        &[],
        EntryType::Put,
        1,
        IdempotencyToken::none(),
    );
    let finished = builder.finish(CompressionType::None).expect("finish block");
    finished.data
}

struct EncodedBlockFetcher {
    fetch_count: Arc<AtomicU32>,
    raw_fetch_count: Arc<AtomicU32>,
    block_a: Bytes,
    block_b: Bytes,
}

impl EncodedBlockFetcher {
    fn new() -> (Self, Arc<AtomicU32>, Arc<AtomicU32>) {
        let fetch_count = Arc::new(AtomicU32::new(0));
        let raw_fetch_count = Arc::new(AtomicU32::new(0));
        let block_a = build_encoded_block("rec1", "item_a", b"value_a");
        let block_b = build_encoded_block("rec1", "item_b", b"value_b");
        (
            Self {
                fetch_count: Arc::clone(&fetch_count),
                raw_fetch_count: Arc::clone(&raw_fetch_count),
                block_a,
                block_b,
            },
            fetch_count,
            raw_fetch_count,
        )
    }
}

#[async_trait]
impl BlockFetcher for EncodedBlockFetcher {
    async fn fetch_block(
        &self,
        _sst_path: &str,
        _offset: u64,
        _size: u32,
        _compression: CompressionType,
    ) -> FlushResult<Vec<BlockEntry>> {
        self.fetch_count.fetch_add(1, Ordering::SeqCst);
        Ok(vec![make_entry("rec1", "item_a", b"value_a")])
    }

    async fn fetch_raw_block(
        &self,
        _sst_path: &str,
        _offset: u64,
        _size: u32,
    ) -> FlushResult<Bytes> {
        self.raw_fetch_count.fetch_add(1, Ordering::SeqCst);
        let mut combined = Vec::with_capacity(self.block_a.len() + self.block_b.len());
        combined.extend_from_slice(&self.block_a);
        combined.extend_from_slice(&self.block_b);
        Ok(Bytes::from(combined))
    }
}

#[tokio::test]
async fn test_adjacent_blocks_coalesce_into_single_fetch() {
    let (fetcher, fetch_count, raw_fetch_count) = EncodedBlockFetcher::new();
    let block_a_size = fetcher.block_a.len() as u32;
    let block_b_size = fetcher.block_b.len() as u32;

    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(Arc::new(fetcher), cache);
    let coalescing = CoalescingFetcher::new(caching);

    let requests = vec![
        BlockRequest {
            block_index: 0,
            block_offset: 0,
            block_size: block_a_size,
        },
        BlockRequest {
            block_index: 1,
            block_offset: block_a_size as u64,
            block_size: block_b_size,
        },
    ];

    let result = coalescing
        .fetch_blocks("data/L0/sst-1.sst", &requests, CompressionType::None)
        .await
        .expect("coalesced fetch should succeed");

    assert_eq!(result.blocks.len(), 2, "should return two decoded blocks");
    assert_eq!(
        raw_fetch_count.load(Ordering::SeqCst),
        1,
        "two adjacent blocks should be fetched in a single raw_fetch call"
    );
    assert_eq!(
        fetch_count.load(Ordering::SeqCst),
        0,
        "coalesced path should not use individual fetch_block calls"
    );

    // Verify the decoded entries from each block
    let (idx_0, ref entries_0) = result.blocks[0];
    assert_eq!(idx_0, 0);
    assert_eq!(entries_0.len(), 1);
    assert_eq!(entries_0[0].composite_key.item_key(), b"item_a");

    let (idx_1, ref entries_1) = result.blocks[1];
    assert_eq!(idx_1, 1);
    assert_eq!(entries_1.len(), 1);
    assert_eq!(entries_1[0].composite_key.item_key(), b"item_b");
}
