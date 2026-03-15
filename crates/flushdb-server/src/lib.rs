pub mod namespace_config;

pub use namespace_config::{
    ConsistencyScope, ConsistencyTarget, NamespaceConfig, PartitionKeyStrategy, StorageLayer,
    StorageLayerConfig, StorageLayerType, WriteConsistency,
};
