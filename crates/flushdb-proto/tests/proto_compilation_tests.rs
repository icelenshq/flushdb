use flushdb_proto::flushdb::v1::*;

#[test]
fn test_put_items_request_construction() {
    let req = PutItemsRequest {
        idempotency_token: Some(IdempotencyToken {
            generation_time: 1234567890,
            token: vec![1; 16],
        }),
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        items: vec![Item {
            key: b"item-key".to_vec(),
            value: b"item-value".to_vec(),
            metadata: vec![],
            chunk: 0,
        }],
    };
    assert_eq!(req.namespace, "test-ns");
    assert_eq!(req.id, "record-1");
    assert_eq!(req.items.len(), 1);
    assert!(req.idempotency_token.is_some());
}

#[test]
fn test_get_items_request_construction() {
    let req = GetItemsRequest {
        namespace: "ns".to_string(),
        id: "rec".to_string(),
        predicate: Some(Predicate {
            predicate: Some(predicate::Predicate::MatchAll(true)),
        }),
        selection: Some(Selection {
            page_size_bytes: 2_000_000,
            item_limit: 100,
            exclude_values: false,
            page_token: vec![],
        }),
        signals: std::collections::HashMap::new(),
    };
    assert_eq!(req.namespace, "ns");
    assert!(req.predicate.is_some());
    assert!(req.selection.is_some());
}

#[test]
fn test_delete_items_request_construction() {
    let req = DeleteItemsRequest {
        idempotency_token: Some(IdempotencyToken {
            generation_time: 1000,
            token: vec![2; 16],
        }),
        namespace: "ns".to_string(),
        id: "rec".to_string(),
        predicate: Some(Predicate {
            predicate: Some(predicate::Predicate::MatchAll(true)),
        }),
    };
    assert_eq!(req.namespace, "ns");
    assert_eq!(req.id, "rec");
    assert!(req.idempotency_token.is_some());
    assert!(req.predicate.is_some());
}

#[test]
fn test_scan_items_request_construction() {
    let req = ScanItemsRequest {
        namespace: "ns".to_string(),
        id: "rec".to_string(),
        predicate: Some(Predicate {
            predicate: Some(predicate::Predicate::MatchAll(true)),
        }),
        signals: std::collections::HashMap::new(),
    };
    assert_eq!(req.namespace, "ns");
    assert_eq!(req.id, "rec");
}

#[test]
fn test_predicate_match_keys() {
    let pred = Predicate {
        predicate: Some(predicate::Predicate::MatchKeys(MatchKeys {
            keys: vec![b"key1".to_vec(), b"key2".to_vec()],
        })),
    };
    match pred.predicate {
        Some(predicate::Predicate::MatchKeys(mk)) => {
            assert_eq!(mk.keys.len(), 2);
            assert_eq!(mk.keys[0], b"key1");
            assert_eq!(mk.keys[1], b"key2");
        }
        _ => panic!("expected MatchKeys"),
    }
}

#[test]
fn test_predicate_match_range() {
    let pred = Predicate {
        predicate: Some(predicate::Predicate::MatchRange(MatchRange {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            start_inclusive: true,
            end_inclusive: false,
        })),
    };
    match pred.predicate {
        Some(predicate::Predicate::MatchRange(mr)) => {
            assert_eq!(mr.start_key, b"a");
            assert_eq!(mr.end_key, b"z");
            assert!(mr.start_inclusive);
            assert!(!mr.end_inclusive);
        }
        _ => panic!("expected MatchRange"),
    }
}

#[test]
fn test_predicate_match_all() {
    let pred = Predicate {
        predicate: Some(predicate::Predicate::MatchAll(true)),
    };
    match pred.predicate {
        Some(predicate::Predicate::MatchAll(val)) => assert!(val),
        _ => panic!("expected MatchAll"),
    }
}

#[test]
fn test_selection_construction() {
    let sel = Selection {
        page_size_bytes: 2_000_000,
        item_limit: 500,
        exclude_values: true,
        page_token: vec![1, 2, 3],
    };
    assert_eq!(sel.page_size_bytes, 2_000_000);
    assert_eq!(sel.item_limit, 500);
    assert!(sel.exclude_values);
    assert_eq!(sel.page_token, vec![1, 2, 3]);
}

#[test]
fn test_item_construction() {
    let item = Item {
        key: b"sort-key".to_vec(),
        value: b"payload".to_vec(),
        metadata: b"content-type:json".to_vec(),
        chunk: 3,
    };
    assert_eq!(item.key, b"sort-key");
    assert_eq!(item.value, b"payload");
    assert_eq!(item.metadata, b"content-type:json");
    assert_eq!(item.chunk, 3);
}

#[test]
fn test_ordered_key_construction() {
    let key = OrderedKey {
        timestamp_ms: 1234567890000,
        node_id: 42,
        sequence: 100,
    };
    assert_eq!(key.timestamp_ms, 1234567890000);
    assert_eq!(key.node_id, 42);
    assert_eq!(key.sequence, 100);
}

#[test]
fn test_idempotency_token_construction() {
    let token = IdempotencyToken {
        generation_time: 9999,
        token: vec![0xAB; 16],
    };
    assert_eq!(token.generation_time, 9999);
    assert_eq!(token.token.len(), 16);
    assert!(token.token.iter().all(|&b| b == 0xAB));
}
