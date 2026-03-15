use bytes::Bytes;
use flushdb_types::{
    CompositeKey, EntryValue, FlushError, FlushResult, IdempotencyToken, MemtableEntry,
    StorageBackend,
};

use super::block_builder::BlockBuilder;
use super::block_reader::BlockEntry;
use super::bloom_filter::{BloomFilterBuilder, FilterBlock};
use super::dedup_block::DedupBlockBuilder;
use super::footer::{SstFooter, SstHeader};
use super::index_block::IndexBlockBuilder;
use super::types::{CompressionType, SstConfig, FORMAT_VERSION, HEADER_SIZE};

#[derive(Debug, Clone)]
pub struct SstInfo {
    pub path: String,
    pub entry_count: u64,
    pub file_size: u64,
    pub min_key: CompositeKey,
    pub max_key: CompositeKey,
    pub bloom_filter_offset: u64,
    pub bloom_filter_size: u32,
    pub index_block_offset: u64,
    pub index_block_size: u32,
    pub dedup_block_size: u32,
    pub compression: CompressionType,
}

struct WriteState {
    output: Vec<u8>,
    block_builder: BlockBuilder,
    bloom_builder: BloomFilterBuilder,
    dedup_builder: DedupBlockBuilder,
    index_builder: IndexBlockBuilder,
    entry_count: u64,
    min_key: Option<CompositeKey>,
    max_key: Option<CompositeKey>,
}

impl WriteState {
    fn new(config: &SstConfig) -> Self {
        let output = vec![0u8; HEADER_SIZE];
        Self {
            output,
            block_builder: BlockBuilder::new(config.block_size_target),
            bloom_builder: BloomFilterBuilder::new(config.bloom_bits_per_key),
            dedup_builder: DedupBlockBuilder::new(),
            index_builder: IndexBlockBuilder::new(),
            entry_count: 0,
            min_key: None,
            max_key: None,
        }
    }

    fn track_key(&mut self, key: CompositeKey) {
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key);
        self.entry_count += 1;
    }
}

pub struct SSTableWriter {
    config: SstConfig,
}

impl SSTableWriter {
    pub fn new(config: SstConfig) -> Self {
        Self { config }
    }

    pub async fn write<B: StorageBackend>(
        &self,
        backend: &B,
        path: &str,
        entries: impl Iterator<Item = MemtableEntry>,
    ) -> FlushResult<SstInfo> {
        let mut state = WriteState::new(&self.config);

        for entry in entries {
            state.block_builder.add_entry(
                &entry.composite_key,
                &entry.value,
                &entry.metadata,
                entry.entry_type,
                entry.sequence_number,
                entry.idempotency_key,
            );

            state.track_key(entry.composite_key);

            if state.block_builder.is_full() {
                Self::flush_block(&self.config, &mut state)?;
            }
        }

        self.finish(backend, path, state).await
    }

    pub async fn write_entries<B: StorageBackend>(
        &self,
        backend: &B,
        path: &str,
        entries: impl Iterator<Item = BlockEntry>,
    ) -> FlushResult<SstInfo> {
        let mut state = WriteState::new(&self.config);

        for entry in entries {
            let value_bytes = match &entry.value {
                EntryValue::Inline(b) => b.clone(),
                EntryValue::BlobRef { .. } => Bytes::new(),
            };

            state.block_builder.add_entry(
                &entry.composite_key,
                &value_bytes,
                &entry.metadata,
                entry.entry_type,
                entry.sequence_number,
                IdempotencyToken::none(),
            );

            state.track_key(entry.composite_key);

            if state.block_builder.is_full() {
                Self::flush_block(&self.config, &mut state)?;
            }
        }

        self.finish(backend, path, state).await
    }

    fn flush_block(config: &SstConfig, state: &mut WriteState) -> FlushResult<()> {
        let block_offset = state.output.len() as u64;

        let mut old_builder = BlockBuilder::new(config.block_size_target);
        std::mem::swap(&mut state.block_builder, &mut old_builder);

        let finished = old_builder.finish(config.compression)?;

        state.output.extend_from_slice(&finished.data);

        state.index_builder.add(
            finished.first_key,
            block_offset,
            finished.data.len() as u32,
            finished.uncompressed_size,
        );
        state.bloom_builder.add_all(&finished.record_ids);
        state.dedup_builder.add_all(&finished.idempotency_tokens);

        Ok(())
    }

    async fn finish<B: StorageBackend>(
        &self,
        backend: &B,
        path: &str,
        mut state: WriteState,
    ) -> FlushResult<SstInfo> {
        if state.entry_count == 0 {
            return Err(FlushError::InvalidArgument {
                message: "cannot write empty SSTable".into(),
            });
        }

        if !state.block_builder.is_empty() {
            Self::flush_block(&self.config, &mut state)?;
        }

        // Dedup block
        let dedup = state.dedup_builder.build();
        let dedup_bytes = dedup.serialize();
        let dedup_block_size = dedup_bytes.len() as u32;
        state.output.extend_from_slice(&dedup_bytes);

        // Bloom filter
        let bloom = state.bloom_builder.build();
        let filter = FilterBlock::Bloom(bloom);
        let filter_bytes = filter.serialize();
        let bloom_filter_offset = state.output.len() as u64;
        let bloom_filter_size = filter_bytes.len() as u32;
        state.output.extend_from_slice(&filter_bytes);

        // Index block
        let index = state.index_builder.build();
        let index_bytes = index.serialize();
        let index_block_offset = state.output.len() as u64;
        let index_block_size = index_bytes.len() as u32;
        state.output.extend_from_slice(&index_bytes);

        // Footer
        let min_key = state.min_key.unwrap();
        let max_key = state.max_key.unwrap();
        let footer = SstFooter {
            bloom_filter_offset,
            bloom_filter_size,
            index_block_offset,
            index_block_size,
            entry_count: state.entry_count,
            min_key: SstFooter::truncate_key(&min_key),
            max_key: SstFooter::truncate_key(&max_key),
            compression_type: self.config.compression,
            format_version: FORMAT_VERSION,
            dedup_block_size,
        };
        state.output.extend_from_slice(&footer.encode());

        // Backfill header
        let header = SstHeader {
            compression: self.config.compression,
            entry_count: state.entry_count,
        };
        state.output[..HEADER_SIZE].copy_from_slice(&header.encode());

        let file_size = state.output.len() as u64;
        backend.put(path, Bytes::from(state.output)).await?;

        Ok(SstInfo {
            path: path.to_string(),
            entry_count: state.entry_count,
            file_size,
            min_key,
            max_key,
            bloom_filter_offset,
            bloom_filter_size,
            index_block_offset,
            index_block_size,
            dedup_block_size,
            compression: self.config.compression,
        })
    }
}

pub fn generate_sst_path(namespace: &str, level: u32) -> String {
    let id = ulid::Ulid::new();
    format!("flushdb/{namespace}/sstables/L{level}/{id}.sst")
}

pub fn generate_run_fragment_path(
    namespace: &str,
    level: u32,
    run_id: &str,
    fragment_index: u32,
) -> String {
    format!("flushdb/{namespace}/sstables/L{level}/run-{run_id}/frag-{fragment_index:04}.sst")
}
