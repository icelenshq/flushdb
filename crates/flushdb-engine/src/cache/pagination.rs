use std::collections::HashMap;

pub const DEFAULT_ITEM_SIZE: usize = 1024;
pub const MIN_ITEMS_PER_PAGE: usize = 1;

#[derive(Debug, Clone, Default)]
struct RunningAverage {
    total_bytes: u64,
    total_items: u64,
}

#[derive(Default)]
pub struct NamespaceSizeEstimator {
    averages: HashMap<String, RunningAverage>,
}

impl NamespaceSizeEstimator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_items(&mut self, namespace: &str, total_bytes: usize, item_count: usize) {
        let avg = self.averages.entry(namespace.to_string()).or_default();
        avg.total_bytes += total_bytes as u64;
        avg.total_items += item_count as u64;
    }

    pub fn estimate_avg_item_size(&self, namespace: &str) -> Option<usize> {
        let avg = self.averages.get(namespace)?;
        if avg.total_items == 0 {
            return None;
        }
        Some((avg.total_bytes / avg.total_items) as usize)
    }

    pub fn estimate_item_count(&self, namespace: &str, page_size_bytes: usize) -> usize {
        let avg_size = self
            .estimate_avg_item_size(namespace)
            .unwrap_or(DEFAULT_ITEM_SIZE);
        (page_size_bytes / avg_size).max(MIN_ITEMS_PER_PAGE)
    }

    pub fn reset(&mut self, namespace: &str) {
        self.averages.remove(namespace);
    }
}
