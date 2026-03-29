use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use flushdb_types::{FlushError, FlushResult, OrderedKey};

// Single AtomicU64 packs both timestamp and sequence so that advancing the
// millisecond and resetting the sequence happen in one CAS — no gap for
// another thread to observe the new timestamp with a stale sequence.
//
// Layout: upper 48 bits = timestamp_ms, lower 16 bits = next_sequence.
// 48 bits covers ~8,900 years from UNIX epoch.

const TIMESTAMP_SHIFT: u32 = 16;
const SEQ_MASK: u64 = 0xFFFF;

fn pack(timestamp_ms: u64, next_seq: u16) -> u64 {
    (timestamp_ms << TIMESTAMP_SHIFT) | (next_seq as u64)
}

fn unpack(state: u64) -> (u64, u16) {
    let next_seq = (state & SEQ_MASK) as u16;
    let ts = state >> TIMESTAMP_SHIFT;
    (ts, next_seq)
}

pub struct VersionGenerator {
    node_id: u16,
    state: AtomicU64,
}

impl VersionGenerator {
    pub fn new(node_id: u16) -> Self {
        Self {
            node_id,
            state: AtomicU64::new(pack(0, 0)),
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

            let state = self.state.load(Ordering::Acquire);
            let (last_ts, next_seq) = unpack(state);

            if current_ms > last_ts {
                let new_state = pack(current_ms, 1);
                match self.state.compare_exchange(
                    state,
                    new_state,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return OrderedKey::new(current_ms, self.node_id, 0),
                    Err(_) => continue,
                }
            } else {
                if next_seq == u16::MAX {
                    spin_until_after_ms(last_ts);
                    continue;
                }

                let new_state = state + 1;
                match self.state.compare_exchange(
                    state,
                    new_state,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return OrderedKey::new(last_ts, self.node_id, next_seq),
                    Err(_) => continue,
                }
            }
        }
    }

    pub fn node_id(&self) -> u16 {
        self.node_id
    }
}

fn spin_until_after_ms(target_ts: u64) {
    loop {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as u64;
        if now > target_ts {
            break;
        }
        std::hint::spin_loop();
    }
}
