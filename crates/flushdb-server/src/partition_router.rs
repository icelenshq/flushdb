use flushdb_types::{FlushError, FlushResult};

use crate::namespace_config::{NamespaceConfig, PartitionKeyStrategy};

pub trait PartitionRouter: Send + Sync {
    fn route(&self, record_id: &str) -> FlushResult<u32>;
}

pub struct LocalPartitionRouter {
    strategy: PartitionKeyStrategy,
    partition_count: u32,
    partition_mask: u32,
}

fn partition_hash(input: &[u8]) -> u32 {
    crc32fast::hash(input)
}

impl LocalPartitionRouter {
    pub fn new(config: &NamespaceConfig) -> Self {
        Self {
            strategy: config.partition_key_strategy.clone(),
            partition_count: config.partition_count,
            partition_mask: config.partition_count - 1,
        }
    }

    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }
}

impl PartitionRouter for LocalPartitionRouter {
    fn route(&self, record_id: &str) -> FlushResult<u32> {
        match &self.strategy {
            PartitionKeyStrategy::Simple => {
                let hash = partition_hash(record_id.as_bytes());
                Ok(hash & self.partition_mask)
            }
            PartitionKeyStrategy::Composite {
                delimiter,
                field_indices,
            } => {
                let fields: Vec<&str> = record_id.split(delimiter.as_str()).collect();
                let max_index = field_indices.iter().max().unwrap_or(&0);
                if fields.len() <= *max_index {
                    return Err(FlushError::InvalidArgument {
                        message: format!(
                            "record_id: expected at least {} fields, got {}",
                            max_index + 1,
                            fields.len()
                        ),
                    });
                }
                let selected: Vec<&str> = field_indices.iter().map(|&i| fields[i]).collect();
                let partition_key = selected.join(delimiter);
                let hash = partition_hash(partition_key.as_bytes());
                Ok(hash & self.partition_mask)
            }
            PartitionKeyStrategy::Prefix { length } => {
                let prefix = &record_id[..std::cmp::min(*length, record_id.len())];
                let hash = partition_hash(prefix.as_bytes());
                Ok(hash & self.partition_mask)
            }
            PartitionKeyStrategy::CustomHash { hash_name } => match hash_name.as_str() {
                "crc32" => {
                    let hash = crc32fast::hash(record_id.as_bytes());
                    Ok(hash & self.partition_mask)
                }
                "fnv" => {
                    use std::hash::{Hash, Hasher};
                    let mut hasher = fnv::FnvHasher::default();
                    record_id.hash(&mut hasher);
                    let hash = hasher.finish() as u32;
                    Ok(hash & self.partition_mask)
                }
                _ => Err(FlushError::InvalidArgument {
                    message: format!("hash_name: unknown hash function: {}", hash_name),
                }),
            },
        }
    }
}
