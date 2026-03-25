use crate::error::{FlushError, FlushResult};

const IDEMPOTENCY_TOKEN_SIZE: usize = 24;
const GENERATION_TIME_SIZE: usize = 8;

/// Fixed-size 24-byte idempotency token for deduplication.
///
/// Wire format (all big-endian):
///   Offset 0:  8 bytes  — generation_time (big-endian u64)
///   Offset 8:  16 bytes — token (raw UUID v7 bytes)
///
/// Internally stored as a contiguous `[u8; 24]` so that `as_bytes()`
/// can return a borrowed slice without copying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IdempotencyToken {
    data: [u8; IDEMPOTENCY_TOKEN_SIZE],
}

impl IdempotencyToken {
    pub fn new(generation_time: u64) -> Self {
        let uuid = uuid::Uuid::now_v7();
        let token: [u8; 16] = *uuid.as_bytes();
        Self::from_parts(generation_time, token)
    }

    pub fn none() -> Self {
        Self {
            data: [0u8; IDEMPOTENCY_TOKEN_SIZE],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> FlushResult<Self> {
        if bytes.len() != IDEMPOTENCY_TOKEN_SIZE {
            return Err(FlushError::InvalidArgument {
                message: format!(
                    "IdempotencyToken requires exactly {IDEMPOTENCY_TOKEN_SIZE} bytes, got {}",
                    bytes.len()
                ),
            });
        }

        let mut data = [0u8; IDEMPOTENCY_TOKEN_SIZE];
        data.copy_from_slice(bytes);
        Ok(Self { data })
    }

    pub fn from_parts(generation_time: u64, token: [u8; 16]) -> Self {
        let mut data = [0u8; IDEMPOTENCY_TOKEN_SIZE];
        data[..GENERATION_TIME_SIZE].copy_from_slice(&generation_time.to_be_bytes());
        data[GENERATION_TIME_SIZE..].copy_from_slice(&token);
        Self { data }
    }

    pub fn generation_time(&self) -> u64 {
        let mut buf = [0u8; GENERATION_TIME_SIZE];
        buf.copy_from_slice(&self.data[..GENERATION_TIME_SIZE]);
        u64::from_be_bytes(buf)
    }

    pub fn token_bytes(&self) -> &[u8; 16] {
        self.data[GENERATION_TIME_SIZE..]
            .first_chunk::<16>()
            .expect("infallible: data is always 24 bytes so sub-slice [8..24] is always 16 bytes")
    }

    pub fn is_none(&self) -> bool {
        self.data == [0u8; IDEMPOTENCY_TOKEN_SIZE]
    }

    pub fn to_bytes(&self) -> [u8; IDEMPOTENCY_TOKEN_SIZE] {
        self.data
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Derives a unique token for a specific item index within a batch request.
    ///
    /// - `index == 0` returns `self` unchanged (single-item backward compat).
    /// - `is_none()` returns `none()` (no-dedup requests are unaffected).
    /// - XORs into the UUID portion (bytes 8..12) so `generation_time` is preserved
    ///   for TTL / drift checks.
    pub fn derive_for_index(&self, index: u32) -> Self {
        if self.is_none() || index == 0 {
            return *self;
        }
        let mut derived = self.data;
        let idx = index.to_le_bytes();
        derived[8] ^= idx[0];
        derived[9] ^= idx[1];
        derived[10] ^= idx[2];
        derived[11] ^= idx[3];
        Self { data: derived }
    }

    /// Returns `true` if this token's drift from `now_ms` is within `max_drift_ms`.
    /// The none sentinel always passes.
    pub fn is_within_drift(&self, now_ms: u64, max_drift_ms: u64) -> bool {
        if self.is_none() {
            return true;
        }

        let gen = self.generation_time();
        let diff = if gen >= now_ms {
            gen.saturating_sub(now_ms)
        } else {
            now_ms.saturating_sub(gen)
        };

        diff <= max_drift_ms
    }
}
