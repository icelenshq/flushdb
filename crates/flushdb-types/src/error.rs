use std::io;

use thiserror::Error;

/// Unified error type for all flushdb operations.
#[derive(Debug, Error)]
pub enum FlushError {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{field} too long: {actual} bytes exceeds maximum of {max}")]
    KeyTooLong {
        field: &'static str,
        actual: usize,
        max: usize,
    },

    #[error("invalid key: {reason}")]
    InvalidKey { reason: String },

    #[error("not found: {key}")]
    NotFound { key: String },

    #[error("precondition failed: {message}")]
    PreconditionFailed { message: String },

    #[error("CRC mismatch: expected {expected:#010x}, actual {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("corrupted data: {message}")]
    CorruptedData { message: String },

    #[error("duplicate token: {token}")]
    DuplicateToken { token: String },

    #[error("resource exhausted: {resource} — {message}")]
    ResourceExhausted { resource: String, message: String },

    #[error("epoch fenced: expected {expected}, actual {actual}")]
    EpochFenced { expected: u64, actual: u64 },

    #[error("invalid argument: {message}")]
    InvalidArgument { message: String },
}

pub type FlushResult<T> = Result<T, FlushError>;
