use std::io;

use flushdb_types::{FlushError, FlushResult};

#[test]
fn test_io_error_from_std() {
    fn fallible() -> FlushResult<()> {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file missing");
        Err(io_err)?
    }

    let err = fallible().unwrap_err();
    assert!(matches!(err, FlushError::Io(_)));
    assert!(err.to_string().contains("file missing"));
}

#[test]
fn test_key_too_long_display() {
    let err = FlushError::KeyTooLong {
        field: "record_id",
        actual: 300,
        max: 255,
    };
    let display = err.to_string();
    assert!(display.contains("record_id"), "should contain field name");
    assert!(display.contains("300"), "should contain actual size");
    assert!(display.contains("255"), "should contain max size");
}

#[test]
fn test_invalid_key_display() {
    let err = FlushError::InvalidKey {
        reason: "contains null byte".to_string(),
    };
    let display = err.to_string();
    assert!(
        display.contains("contains null byte"),
        "should contain reason"
    );
}

#[test]
fn test_not_found_display() {
    let err = FlushError::NotFound {
        key: "user:42".to_string(),
    };
    let display = err.to_string();
    assert!(display.contains("user:42"), "should contain key");
}

#[test]
fn test_precondition_failed_display() {
    let err = FlushError::PreconditionFailed {
        message: "version mismatch".to_string(),
    };
    let display = err.to_string();
    assert!(
        display.contains("version mismatch"),
        "should contain message"
    );
}

#[test]
fn test_crc_mismatch_display() {
    let err = FlushError::CrcMismatch {
        expected: 0xDEADBEEF,
        actual: 0xCAFEBABE,
    };
    let display = err.to_string();
    assert!(
        display.contains("0xdeadbeef"),
        "should contain expected CRC, got: {display}"
    );
    assert!(
        display.contains("0xcafebabe"),
        "should contain actual CRC, got: {display}"
    );
}

#[test]
fn test_corrupted_data_display() {
    let err = FlushError::CorruptedData {
        message: "unexpected EOF in block header".to_string(),
    };
    let display = err.to_string();
    assert!(
        display.contains("unexpected EOF in block header"),
        "should contain description"
    );
}

#[test]
fn test_epoch_fenced_display() {
    let err = FlushError::EpochFenced {
        expected: 5,
        actual: 3,
    };
    let display = err.to_string();
    assert!(display.contains('5'), "should contain expected epoch");
    assert!(display.contains('3'), "should contain actual epoch");
}

#[test]
fn test_resource_exhausted_display() {
    let err = FlushError::ResourceExhausted {
        resource: "memtable".to_string(),
        message: "write buffer full".to_string(),
    };
    let display = err.to_string();
    assert!(display.contains("memtable"), "should contain resource name");
    assert!(
        display.contains("write buffer full"),
        "should contain message"
    );
}

#[test]
fn test_duplicate_token_display() {
    let err = FlushError::DuplicateToken {
        token: "abc-123".to_string(),
    };
    let display = err.to_string();
    assert!(display.contains("abc-123"), "should contain token");
}

#[test]
fn test_invalid_argument_display() {
    let err = FlushError::InvalidArgument {
        message: "batch size must be positive".to_string(),
    };
    let display = err.to_string();
    assert!(
        display.contains("batch size must be positive"),
        "should contain message"
    );
}
