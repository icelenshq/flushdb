use std::env;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use flushdb_types::{FlushError, FlushResult, OrderedKey};

pub struct VersionGenerator {
    node_id: u16,
    sequence: AtomicU16,
    last_timestamp: AtomicU64,
}

impl VersionGenerator {
    pub fn new(node_id: u16) -> Self {
        Self {
            node_id,
            sequence: AtomicU16::new(0),
            last_timestamp: AtomicU64::new(0),
        }
    }

    pub fn with_node_id_from_env() -> FlushResult<Self> {
        let raw = env::var("FLUSHDB_NODE_ID").map_err(|e| FlushError::InvalidArgument {
            message: format!("FLUSHDB_NODE_ID env var not set: {e}"),
        })?;

        let node_id: u16 = raw.parse().map_err(|e| FlushError::InvalidArgument {
            message: format!("FLUSHDB_NODE_ID is not a valid u16: {e}"),
        })?;

        Ok(Self::new(node_id))
    }

    pub fn next_version(&self) -> OrderedKey {
        loop {
            let current_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before UNIX epoch")
                .as_millis() as u64;

            let last_ts = self.last_timestamp.load(Ordering::Acquire);

            if current_ms > last_ts {
                match self.last_timestamp.compare_exchange(
                    last_ts,
                    current_ms,
                    Ordering::Release,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // Store 1 because sequence 0 is used by this caller
                        self.sequence.store(1, Ordering::Release);
                        return OrderedKey::new(current_ms, self.node_id, 0);
                    }
                    Err(_) => continue,
                }
            }

            // current_ms <= last_ts: use last_ts (handles both equal and clock-skew cases)
            let ts = self.last_timestamp.load(Ordering::Acquire);
            let seq = self.sequence.fetch_add(1, Ordering::Relaxed);

            if seq == u16::MAX {
                // Sequence overflow: spin until next millisecond
                loop {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("system clock before UNIX epoch")
                        .as_millis() as u64;
                    if now > ts {
                        break;
                    }
                    std::hint::spin_loop();
                }
                // Retry from the top with the new timestamp
                continue;
            }

            return OrderedKey::new(ts, self.node_id, seq);
        }
    }

    pub fn node_id(&self) -> u16 {
        self.node_id
    }
}
