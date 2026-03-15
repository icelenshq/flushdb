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
    /// Creates a new token with the given generation time and a fresh UUID v7 nonce.
    pub fn new(generation_time: u64) -> Self {
        let uuid = uuid::Uuid::now_v7();
        let token: [u8; 16] = *uuid.as_bytes();
        Self::from_parts(generation_time, token)
    }

    /// Returns the all-zeros sentinel indicating no idempotency is requested.
    pub fn none() -> Self {
        Self {
            data: [0u8; IDEMPOTENCY_TOKEN_SIZE],
        }
    }

    /// Parse from exactly 24 bytes. Returns `InvalidArgument` if length != 24.
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

    /// Direct construction from explicit parts.
    pub fn from_parts(generation_time: u64, token: [u8; 16]) -> Self {
        let mut data = [0u8; IDEMPOTENCY_TOKEN_SIZE];
        data[..GENERATION_TIME_SIZE].copy_from_slice(&generation_time.to_be_bytes());
        data[GENERATION_TIME_SIZE..].copy_from_slice(&token);
        Self { data }
    }

    /// Returns the generation timestamp in milliseconds.
    pub fn generation_time(&self) -> u64 {
        let mut buf = [0u8; GENERATION_TIME_SIZE];
        buf.copy_from_slice(&self.data[..GENERATION_TIME_SIZE]);
        u64::from_be_bytes(buf)
    }

    /// Returns the 16-byte UUID v7 nonce portion.
    pub fn token_bytes(&self) -> &[u8; 16] {
        // The data array is always 24 bytes, so [8..24] is always exactly 16 bytes.
        // `first_chunk` on a 16-byte slice always succeeds.
        self.data[GENERATION_TIME_SIZE..].first_chunk::<16>().expect(
            "infallible: data is always 24 bytes so sub-slice [8..24] is always 16 bytes",
        )
    }

    /// Returns `true` if all 24 bytes are zero (the no-idempotency sentinel).
    pub fn is_none(&self) -> bool {
        self.data == [0u8; IDEMPOTENCY_TOKEN_SIZE]
    }

    /// Serialize to a 24-byte big-endian array.
    pub fn to_bytes(&self) -> [u8; IDEMPOTENCY_TOKEN_SIZE] {
        self.data
    }

    /// Borrow the token as a contiguous byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Returns `true` if the token's generation time is within `max_drift_ms`
    /// of `now_ms`, or if the token is the none sentinel.
    ///
    /// Uses checked arithmetic to avoid overflow on extreme values.
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
