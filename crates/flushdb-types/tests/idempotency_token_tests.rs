use flushdb_types::{FlushError, IdempotencyToken};

#[test]
fn test_new_creates_unique_tokens() {
    let t1 = IdempotencyToken::new(1000);
    let t2 = IdempotencyToken::new(1000);
    // Same generation_time, but UUID v7 nonces must differ.
    assert_eq!(t1.generation_time(), t2.generation_time());
    assert_ne!(t1.token_bytes(), t2.token_bytes());
    assert_ne!(t1, t2);
}

#[test]
fn test_none_is_all_zeros() {
    let token = IdempotencyToken::none();
    assert_eq!(token.to_bytes(), [0u8; 24]);
}

#[test]
fn test_none_is_detected() {
    assert!(IdempotencyToken::none().is_none());
}

#[test]
fn test_non_none_is_not_none() {
    let token = IdempotencyToken::new(1234);
    assert!(!token.is_none());
}

#[test]
fn test_round_trip_bytes() {
    let original = IdempotencyToken::new(1_700_000_000_000);
    let bytes = original.to_bytes();
    let restored = IdempotencyToken::from_bytes(&bytes).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn test_from_bytes_wrong_length() {
    let too_short = [0u8; 23];
    let err = IdempotencyToken::from_bytes(&too_short).unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidArgument { .. }),
        "expected InvalidArgument for 23 bytes, got: {err:?}"
    );

    let too_long = [0u8; 25];
    let err = IdempotencyToken::from_bytes(&too_long).unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidArgument { .. }),
        "expected InvalidArgument for 25 bytes, got: {err:?}"
    );
}

#[test]
fn test_generation_time_accessor() {
    let token = IdempotencyToken::new(42_000);
    assert_eq!(token.generation_time(), 42_000);
}

#[test]
fn test_token_bytes_accessor() {
    let uuid_bytes: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let token = IdempotencyToken::from_parts(999, uuid_bytes);
    assert_eq!(*token.token_bytes(), uuid_bytes);
}

#[test]
fn test_from_parts() {
    let gen_time: u64 = 0xDEAD_BEEF_CAFE_BABE;
    let nonce: [u8; 16] = [
        0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xA0, 0xB0, 0xC0, 0xD0, 0xE0,
        0xF0, 0xFF,
    ];
    let token = IdempotencyToken::from_parts(gen_time, nonce);
    assert_eq!(token.generation_time(), gen_time);
    assert_eq!(*token.token_bytes(), nonce);
}

#[test]
fn test_is_within_drift_valid() {
    let now_ms = 100_000;
    let token = IdempotencyToken::new(now_ms - 3_000); // 3s in the past
    assert!(token.is_within_drift(now_ms, 5_000)); // 5s drift allowed
}

#[test]
fn test_is_within_drift_expired() {
    let now_ms = 100_000;
    let token = IdempotencyToken::new(now_ms - 10_000); // 10s in the past
    assert!(!token.is_within_drift(now_ms, 5_000)); // 5s drift allowed
}

#[test]
fn test_is_within_drift_future() {
    let now_ms = 100_000;
    let token = IdempotencyToken::new(now_ms + 10_000); // 10s in the future
    assert!(!token.is_within_drift(now_ms, 5_000)); // 5s drift allowed
}

#[test]
fn test_is_within_drift_none_always_valid() {
    let none_token = IdempotencyToken::none();
    assert!(none_token.is_within_drift(0, 0));
    assert!(none_token.is_within_drift(u64::MAX, 0));
    assert!(none_token.is_within_drift(1_000_000, 5_000));
}

#[test]
fn test_copy_semantics() {
    let a = IdempotencyToken::new(42);
    let b = a; // Copy
    // Both should be usable after the assignment.
    assert_eq!(a.generation_time(), 42);
    assert_eq!(b.generation_time(), 42);
    assert_eq!(a, b);
}

#[test]
fn test_wire_format_big_endian() {
    let gen_time: u64 = 0x0102030405060708;
    let nonce: [u8; 16] = [
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
        0xFF, 0x00,
    ];
    let token = IdempotencyToken::from_parts(gen_time, nonce);
    let bytes = token.to_bytes();

    // First 8 bytes: generation_time in big-endian
    assert_eq!(
        &bytes[..8],
        &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
    );
    // Next 16 bytes: raw UUID nonce
    assert_eq!(
        &bytes[8..],
        &[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
            0xFF, 0x00]
    );
}

#[test]
fn test_as_bytes_length_and_content() {
    let token = IdempotencyToken::new(5000);
    let slice = token.as_bytes();
    let array = token.to_bytes();
    assert_eq!(slice.len(), 24);
    assert_eq!(slice, &array);
}

#[test]
fn test_is_within_drift_exact_boundary() {
    let now_ms = 100_000;
    // Token is exactly max_drift_ms in the past — should be within drift (<=).
    let token = IdempotencyToken::new(now_ms - 5_000);
    assert!(
        token.is_within_drift(now_ms, 5_000),
        "exact boundary should be within drift"
    );
}

#[test]
fn test_is_within_drift_future_exact_boundary() {
    let now_ms = 100_000;
    let max_drift_ms = 5_000;
    let token = IdempotencyToken::new(now_ms + max_drift_ms);
    assert!(
        token.is_within_drift(now_ms, max_drift_ms),
        "token at exactly now + max_drift should be within drift"
    );
}

#[test]
fn test_is_within_drift_future_just_beyond_boundary() {
    let now_ms = 100_000;
    let max_drift_ms = 5_000;
    let token = IdempotencyToken::new(now_ms + max_drift_ms + 1);
    assert!(
        !token.is_within_drift(now_ms, max_drift_ms),
        "token at now + max_drift + 1 should be outside drift"
    );
}

#[test]
fn test_as_bytes_none_is_all_zeros() {
    let none = IdempotencyToken::none();
    assert!(none.as_bytes().iter().all(|&b| b == 0));
}
