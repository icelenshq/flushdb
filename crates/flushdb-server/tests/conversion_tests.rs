use bytes::Bytes;
use flushdb_engine::{GetResult, MergeEntry, PageToken, RangeReadResult};
use flushdb_proto::flushdb::v1 as proto;
use flushdb_server::conversions::*;
use flushdb_server::NamespaceConfig;
use flushdb_types::{CompositeKey, EntryType, FlushError, IdempotencyToken, OrderedKey};

// === IdempotencyToken Tests ===

#[test]
fn test_proto_to_token_valid() {
    let token_bytes = vec![1u8; 16];
    let proto_token = proto::IdempotencyToken {
        generation_time: 12345,
        token: token_bytes.clone(),
    };
    let result = proto_to_idempotency_token(Some(proto_token)).expect("should succeed");
    assert_eq!(result.generation_time(), 12345);
    assert_eq!(result.token_bytes(), &[1u8; 16]);
}

#[test]
fn test_proto_to_token_none() {
    let result = proto_to_idempotency_token(None).expect("should succeed");
    assert!(result.is_none());
}

#[test]
fn test_proto_to_token_all_zeros() {
    let proto_token = proto::IdempotencyToken {
        generation_time: 0,
        token: vec![],
    };
    let result = proto_to_idempotency_token(Some(proto_token)).expect("should succeed");
    assert!(result.is_none());
}

#[test]
fn test_proto_to_token_wrong_length() {
    let proto_token = proto::IdempotencyToken {
        generation_time: 100,
        token: vec![1u8; 10],
    };
    let result = proto_to_idempotency_token(Some(proto_token));
    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("exactly 16 bytes"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_token_round_trip() {
    let original = IdempotencyToken::from_parts(99999, [42u8; 16]);
    let proto_form = idempotency_token_to_proto(&original);
    let recovered =
        proto_to_idempotency_token(Some(proto_form)).expect("round trip should succeed");
    assert_eq!(original, recovered);
}

// === OrderedKey Tests ===

#[test]
fn test_ordered_key_to_proto() {
    let key = OrderedKey::new(1000, 42, 7);
    let proto_key = ordered_key_to_proto(&key);
    assert_eq!(proto_key.timestamp_ms, 1000);
    assert_eq!(proto_key.node_id, 42);
    assert_eq!(proto_key.sequence, 7);
}

#[test]
fn test_proto_to_ordered_key_valid() {
    let proto_key = proto::OrderedKey {
        timestamp_ms: 5000,
        node_id: 100,
        sequence: 200,
    };
    let key = proto_to_ordered_key(&proto_key).expect("should succeed");
    assert_eq!(key.timestamp_ms(), 5000);
    assert_eq!(key.node_id(), 100);
    assert_eq!(key.sequence(), 200);
}

#[test]
fn test_proto_to_ordered_key_overflow_node_id() {
    let proto_key = proto::OrderedKey {
        timestamp_ms: 1000,
        node_id: 70000,
        sequence: 0,
    };
    let result = proto_to_ordered_key(&proto_key);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("node_id"));
            assert!(message.contains("65535"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_proto_to_ordered_key_overflow_sequence() {
    let proto_key = proto::OrderedKey {
        timestamp_ms: 1000,
        node_id: 0,
        sequence: 100_000,
    };
    let result = proto_to_ordered_key(&proto_key);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("sequence"));
            assert!(message.contains("65535"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_ordered_key_round_trip() {
    let original = OrderedKey::new(999, 255, 1024);
    let proto_key = ordered_key_to_proto(&original);
    let recovered = proto_to_ordered_key(&proto_key).expect("round trip should succeed");
    assert_eq!(original, recovered);
}

// === Predicate Tests ===

#[test]
fn test_parse_match_keys() {
    let pred = proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchKeys(proto::MatchKeys {
            keys: vec![b"key1".to_vec(), b"key2".to_vec()],
        })),
    };
    let parsed = parse_predicate(Some(pred)).expect("should succeed");
    match parsed {
        ParsedPredicate::MatchKeys { keys } => {
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], Bytes::from_static(b"key1"));
            assert_eq!(keys[1], Bytes::from_static(b"key2"));
        }
        other => panic!("expected MatchKeys, got: {other:?}"),
    }
}

#[test]
fn test_parse_match_range() {
    let pred = proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchRange(
            proto::MatchRange {
                start_key: b"aaa".to_vec(),
                end_key: b"zzz".to_vec(),
                start_inclusive: true,
                end_inclusive: false,
            },
        )),
    };
    let parsed = parse_predicate(Some(pred)).expect("should succeed");
    match parsed {
        ParsedPredicate::MatchRange {
            start_key,
            end_key,
            start_inclusive,
            end_inclusive,
        } => {
            assert_eq!(start_key, Some(Bytes::from_static(b"aaa")));
            assert_eq!(end_key, Some(Bytes::from_static(b"zzz")));
            assert!(start_inclusive);
            assert!(!end_inclusive);
        }
        other => panic!("expected MatchRange, got: {other:?}"),
    }
}

#[test]
fn test_parse_match_all() {
    let pred = proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchAll(true)),
    };
    let parsed = parse_predicate(Some(pred)).expect("should succeed");
    assert!(matches!(parsed, ParsedPredicate::MatchAll));
}

#[test]
fn test_parse_no_predicate() {
    let result = parse_predicate(None);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("must specify a predicate"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_parse_predicate_inner_none() {
    let result = parse_predicate(Some(proto::Predicate { predicate: None }));
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("must specify a predicate"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_parse_match_keys_empty() {
    let pred = proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchKeys(proto::MatchKeys {
            keys: vec![],
        })),
    };
    let result = parse_predicate(Some(pred));
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("must specify at least one key"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_parse_match_range_inverted() {
    let pred = proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchRange(
            proto::MatchRange {
                start_key: b"zzz".to_vec(),
                end_key: b"aaa".to_vec(),
                start_inclusive: true,
                end_inclusive: true,
            },
        )),
    };
    let result = parse_predicate(Some(pred));
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("start_key must be <= end_key"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

// === Selection Tests ===

fn test_namespace_config() -> NamespaceConfig {
    NamespaceConfig::new("test-ns".into(), 4).expect("valid config")
}

#[test]
fn test_parse_selection_defaults() {
    let config = test_namespace_config();
    let sel = proto::Selection {
        page_size_bytes: 0,
        item_limit: 0,
        exclude_values: false,
        page_token: vec![],
    };
    let (opts, exclude) = parse_selection(Some(sel), &config).expect("should succeed");
    assert_eq!(opts.page_size_bytes, config.default_page_size_bytes as usize);
    assert!(opts.item_limit.is_none());
    assert!(opts.resume_from.is_none());
    assert!(!exclude);
}

#[test]
fn test_parse_selection_capped() {
    let config = test_namespace_config();
    let sel = proto::Selection {
        page_size_bytes: u32::MAX,
        item_limit: 50,
        exclude_values: true,
        page_token: vec![],
    };
    let (opts, exclude) = parse_selection(Some(sel), &config).expect("should succeed");
    assert_eq!(opts.page_size_bytes, config.max_page_size_bytes as usize);
    assert_eq!(opts.item_limit, Some(50));
    assert!(exclude);
}

#[test]
fn test_parse_selection_none() {
    let config = test_namespace_config();
    let (opts, exclude) = parse_selection(None, &config).expect("should succeed");
    assert_eq!(opts.page_size_bytes, config.default_page_size_bytes as usize);
    assert!(opts.item_limit.is_none());
    assert!(opts.resume_from.is_none());
    assert!(!exclude);
}

// === Result Formatting Tests ===

fn make_get_result(item_key: &[u8], value: &[u8], metadata: &[u8]) -> GetResult {
    GetResult {
        key: CompositeKey::new(b"record1", item_key).expect("valid key"),
        value: Bytes::copy_from_slice(value),
        metadata: Bytes::copy_from_slice(metadata),
        sequence_number: 1,
    }
}

fn make_merge_entry(
    item_key: &[u8],
    value: &[u8],
    entry_type: EntryType,
) -> MergeEntry {
    MergeEntry {
        composite_key: CompositeKey::new(b"record1", item_key).expect("valid key"),
        value: Bytes::copy_from_slice(value),
        metadata: Bytes::new(),
        entry_type,
        sequence_number: 1,
    }
}

#[test]
fn test_get_result_to_item() {
    let result = make_get_result(b"mykey", b"myvalue", b"mymeta");
    let item = get_result_to_proto_item(&result);
    assert_eq!(item.key, b"mykey");
    assert_eq!(item.value, b"myvalue");
    assert_eq!(item.metadata, b"mymeta");
    assert_eq!(item.chunk, 0);
}

#[test]
fn test_format_excludes_values() {
    let results = vec![
        Some(make_get_result(b"k1", b"v1", b"m1")),
        None,
        Some(make_get_result(b"k2", b"v2", b"m2")),
    ];
    let items = format_get_response(results, true);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].key, b"k1");
    assert!(items[0].value.is_empty());
    assert_eq!(items[0].metadata, b"m1");
    assert_eq!(items[1].key, b"k2");
    assert!(items[1].value.is_empty());
}

#[test]
fn test_format_get_response_includes_values() {
    let results = vec![Some(make_get_result(b"k1", b"v1", b"m1"))];
    let items = format_get_response(results, false);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].value, b"v1");
}

#[test]
fn test_format_scan_final_page_empty_token() {
    let result = RangeReadResult {
        entries: vec![make_merge_entry(b"k1", b"v1", EntryType::Put)],
        next_page_token: None,
        total_bytes: 10,
        is_partial: false,
    };
    let (items, token_bytes) = format_scan_response(&result, false);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].key, b"k1");
    assert!(token_bytes.is_empty());
}

#[test]
fn test_format_scan_skips_tombstones() {
    let result = RangeReadResult {
        entries: vec![
            make_merge_entry(b"k1", b"v1", EntryType::Put),
            make_merge_entry(b"k2", b"", EntryType::Delete),
            make_merge_entry(b"k3", b"v3", EntryType::Put),
        ],
        next_page_token: None,
        total_bytes: 20,
        is_partial: false,
    };
    let (items, _) = format_scan_response(&result, false);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].key, b"k1");
    assert_eq!(items[1].key, b"k3");
}

#[test]
fn test_format_scan_with_page_token() {
    let token = PageToken {
        last_composite_key: CompositeKey::new(b"record1", b"k1").expect("valid"),
        last_sequence_number: 5,
        avg_item_size_bytes: Some(100),
    };
    let result = RangeReadResult {
        entries: vec![make_merge_entry(b"k1", b"v1", EntryType::Put)],
        next_page_token: Some(token),
        total_bytes: 10,
        is_partial: true,
    };
    let (items, token_bytes) = format_scan_response(&result, false);
    assert_eq!(items.len(), 1);
    assert!(!token_bytes.is_empty());

    let decoded = PageToken::decode(&token_bytes).expect("should decode");
    assert_eq!(decoded.last_sequence_number, 5);
}

// === Error Mapping Tests ===

#[test]
fn test_not_found_maps_correctly() {
    let err = FlushError::NotFound {
        key: "missing-key".into(),
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::NotFound);
    assert!(status.message().contains("missing-key"));
}

#[test]
fn test_invalid_key_maps_correctly() {
    let err = FlushError::InvalidKey {
        reason: "bad key".into(),
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn test_duplicate_token_maps_correctly() {
    let err = FlushError::DuplicateToken {
        token: "tok123".into(),
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::AlreadyExists);
    assert!(status.message().contains("tok123"));
}

#[test]
fn test_resource_exhausted_maps_correctly() {
    let err = FlushError::ResourceExhausted {
        resource: "memtable".into(),
        message: "full".into(),
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
}

#[test]
fn test_corrupted_data_maps_internal() {
    let err = FlushError::CorruptedData {
        message: "bad block".into(),
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn test_epoch_fenced_maps_aborted() {
    let err = FlushError::EpochFenced {
        expected: 5,
        actual: 3,
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::Aborted);
}

#[test]
fn test_crc_mismatch_maps_internal() {
    let err = FlushError::CrcMismatch {
        expected: 0xDEAD,
        actual: 0xBEEF,
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn test_precondition_failed_maps_correctly() {
    let err = FlushError::PreconditionFailed {
        message: "version conflict".into(),
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn test_key_too_long_maps_invalid_argument() {
    let err = FlushError::KeyTooLong {
        field: "item_key",
        actual: 5000,
        max: 4096,
    };
    let status = flush_error_to_status(err);
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

// === Validation Tests ===

#[test]
fn test_validate_empty_namespace() {
    let result = validate_namespace("");
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("must not be empty"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_validate_valid_namespace() {
    validate_namespace("my-namespace").expect("should be valid");
}

#[test]
fn test_validate_empty_record_id() {
    let result = validate_record_id("");
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("must not be empty"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_validate_record_id_null_bytes() {
    let result = validate_record_id("hello\0world");
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("null bytes"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_validate_record_id_too_long() {
    let long_id: String = "a".repeat(257);
    let result = validate_record_id(&long_id);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("exceeds maximum"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_validate_valid_record_id() {
    validate_record_id("user-123").expect("should be valid");
}

#[test]
fn test_validate_items_empty_list() {
    let result = validate_items(&[]);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("must not be empty"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_validate_item_key_too_long() {
    let items = vec![proto::Item {
        key: vec![0u8; 4097],
        value: vec![],
        metadata: vec![],
        chunk: 0,
    }];
    let result = validate_items(&items);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(message.contains("exceeds maximum"));
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[test]
fn test_validate_valid_items() {
    let items = vec![
        proto::Item {
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
            metadata: vec![],
            chunk: 0,
        },
        proto::Item {
            key: b"key2".to_vec(),
            value: b"val2".to_vec(),
            metadata: vec![],
            chunk: 0,
        },
    ];
    validate_items(&items).expect("should be valid");
}

// === MergeEntry conversion test ===

#[test]
fn test_merge_entry_to_proto_item() {
    let entry = make_merge_entry(b"mk", b"mv", EntryType::Put);
    let item = merge_entry_to_proto_item(&entry);
    assert_eq!(item.key, b"mk");
    assert_eq!(item.value, b"mv");
    assert_eq!(item.chunk, 0);
}
