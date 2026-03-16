use flushdb_proto::flushdb::v1::*;
use prost::Message;

// Encode/decode round-trip tests — these actually exercise the proto schema
// and catch field number conflicts, type mismatches, and reserved-field collisions.

#[test]
fn test_put_items_request_round_trip() {
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

    let encoded = req.encode_to_vec();
    let decoded = PutItemsRequest::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.namespace, "test-ns");
    assert_eq!(decoded.id, "record-1");
    assert_eq!(decoded.items.len(), 1);
    assert_eq!(decoded.items[0].key, b"item-key");
    assert_eq!(decoded.items[0].value, b"item-value");
    let token = decoded.idempotency_token.expect("token missing");
    assert_eq!(token.generation_time, 1234567890);
    assert_eq!(token.token, vec![1; 16]);
}

#[test]
fn test_put_items_response_round_trip() {
    let resp = PutItemsResponse {
        version: Some(OrderedKey {
            timestamp_ms: 1700000000000,
            node_id: 42,
            sequence: 100,
        }),
    };

    let encoded = resp.encode_to_vec();
    let decoded = PutItemsResponse::decode(encoded.as_slice()).expect("decode failed");
    let version = decoded.version.expect("version missing");
    assert_eq!(version.timestamp_ms, 1700000000000);
    assert_eq!(version.node_id, 42);
    assert_eq!(version.sequence, 100);
}

#[test]
fn test_get_items_request_round_trip() {
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

    let encoded = req.encode_to_vec();
    let decoded = GetItemsRequest::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.namespace, "ns");
    assert_eq!(decoded.id, "rec");
    let sel = decoded.selection.expect("selection missing");
    assert_eq!(sel.page_size_bytes, 2_000_000);
    assert_eq!(sel.item_limit, 100);
    assert!(!sel.exclude_values);
    match decoded.predicate.expect("predicate missing").predicate {
        Some(predicate::Predicate::MatchAll(val)) => assert!(val),
        other => panic!("expected MatchAll(true), got: {other:?}"),
    }
}

#[test]
fn test_get_items_response_round_trip() {
    let resp = GetItemsResponse {
        items: vec![
            Item {
                key: b"key-1".to_vec(),
                value: b"val-1".to_vec(),
                metadata: b"meta".to_vec(),
                chunk: 0,
            },
            Item {
                key: b"key-2".to_vec(),
                value: b"val-2".to_vec(),
                metadata: vec![],
                chunk: 1,
            },
        ],
        next_page_token: vec![0xDE, 0xAD],
    };

    let encoded = resp.encode_to_vec();
    let decoded = GetItemsResponse::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.items.len(), 2);
    assert_eq!(decoded.items[0].key, b"key-1");
    assert_eq!(decoded.items[0].value, b"val-1");
    assert_eq!(decoded.items[0].metadata, b"meta");
    assert_eq!(decoded.items[1].key, b"key-2");
    assert_eq!(decoded.items[1].chunk, 1);
    assert_eq!(decoded.next_page_token, vec![0xDE, 0xAD]);
}

#[test]
fn test_get_items_response_empty() {
    let resp = GetItemsResponse {
        items: vec![],
        next_page_token: vec![],
    };

    let encoded = resp.encode_to_vec();
    let decoded = GetItemsResponse::decode(encoded.as_slice()).expect("decode failed");
    assert!(decoded.items.is_empty());
    assert!(decoded.next_page_token.is_empty());
}

#[test]
fn test_delete_items_request_round_trip() {
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

    let encoded = req.encode_to_vec();
    let decoded = DeleteItemsRequest::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.namespace, "ns");
    assert_eq!(decoded.id, "rec");
    let token = decoded.idempotency_token.expect("token missing");
    assert_eq!(token.generation_time, 1000);
    assert_eq!(token.token, vec![2; 16]);
    assert!(decoded.predicate.is_some());
}

#[test]
fn test_delete_items_response_round_trip() {
    let resp = DeleteItemsResponse {
        version: Some(OrderedKey {
            timestamp_ms: 9999,
            node_id: 7,
            sequence: 42,
        }),
    };

    let encoded = resp.encode_to_vec();
    let decoded = DeleteItemsResponse::decode(encoded.as_slice()).expect("decode failed");
    let version = decoded.version.expect("version missing");
    assert_eq!(version.timestamp_ms, 9999);
    assert_eq!(version.node_id, 7);
    assert_eq!(version.sequence, 42);
}

#[test]
fn test_scan_items_request_round_trip() {
    let req = ScanItemsRequest {
        namespace: "ns".to_string(),
        id: "rec".to_string(),
        predicate: Some(Predicate {
            predicate: Some(predicate::Predicate::MatchAll(true)),
        }),
        signals: std::collections::HashMap::new(),
    };

    let encoded = req.encode_to_vec();
    let decoded = ScanItemsRequest::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.namespace, "ns");
    assert_eq!(decoded.id, "rec");
    match decoded.predicate.expect("predicate missing").predicate {
        Some(predicate::Predicate::MatchAll(val)) => assert!(val),
        other => panic!("expected MatchAll(true), got: {other:?}"),
    }
}

#[test]
fn test_scan_items_response_round_trip() {
    let resp = ScanItemsResponse {
        items: vec![Item {
            key: b"scan-key".to_vec(),
            value: b"scan-val".to_vec(),
            metadata: vec![],
            chunk: 0,
        }],
    };

    let encoded = resp.encode_to_vec();
    let decoded = ScanItemsResponse::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.items.len(), 1);
    assert_eq!(decoded.items[0].key, b"scan-key");
    assert_eq!(decoded.items[0].value, b"scan-val");
}

#[test]
fn test_predicate_match_keys_round_trip() {
    let pred = Predicate {
        predicate: Some(predicate::Predicate::MatchKeys(MatchKeys {
            keys: vec![b"key1".to_vec(), b"key2".to_vec()],
        })),
    };

    let encoded = pred.encode_to_vec();
    let decoded = Predicate::decode(encoded.as_slice()).expect("decode failed");
    match decoded.predicate {
        Some(predicate::Predicate::MatchKeys(mk)) => {
            assert_eq!(mk.keys.len(), 2);
            assert_eq!(mk.keys[0], b"key1");
            assert_eq!(mk.keys[1], b"key2");
        }
        _ => panic!("expected MatchKeys"),
    }
}

#[test]
fn test_predicate_match_range_round_trip() {
    let pred = Predicate {
        predicate: Some(predicate::Predicate::MatchRange(MatchRange {
            start_key: b"a".to_vec(),
            end_key: b"z".to_vec(),
            start_inclusive: true,
            end_inclusive: false,
        })),
    };

    let encoded = pred.encode_to_vec();
    let decoded = Predicate::decode(encoded.as_slice()).expect("decode failed");
    match decoded.predicate {
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
fn test_predicate_match_all_round_trip() {
    for val in [true, false] {
        let pred = Predicate {
            predicate: Some(predicate::Predicate::MatchAll(val)),
        };

        let encoded = pred.encode_to_vec();
        let decoded = Predicate::decode(encoded.as_slice()).expect("decode failed");
        match decoded.predicate {
            Some(predicate::Predicate::MatchAll(v)) => assert_eq!(v, val),
            _ => panic!("expected MatchAll({val})"),
        }
    }
}

#[test]
fn test_predicate_none_round_trip() {
    let pred = Predicate { predicate: None };

    let encoded = pred.encode_to_vec();
    let decoded = Predicate::decode(encoded.as_slice()).expect("decode failed");
    assert!(decoded.predicate.is_none());
}

#[test]
fn test_selection_round_trip() {
    let sel = Selection {
        page_size_bytes: 2_000_000,
        item_limit: 500,
        exclude_values: true,
        page_token: vec![1, 2, 3],
    };

    let encoded = sel.encode_to_vec();
    let decoded = Selection::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.page_size_bytes, 2_000_000);
    assert_eq!(decoded.item_limit, 500);
    assert!(decoded.exclude_values);
    assert_eq!(decoded.page_token, vec![1, 2, 3]);
}

#[test]
fn test_item_round_trip() {
    let item = Item {
        key: b"sort-key".to_vec(),
        value: b"payload".to_vec(),
        metadata: b"content-type:json".to_vec(),
        chunk: 3,
    };

    let encoded = item.encode_to_vec();
    let decoded = Item::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.key, b"sort-key");
    assert_eq!(decoded.value, b"payload");
    assert_eq!(decoded.metadata, b"content-type:json");
    assert_eq!(decoded.chunk, 3);
}

#[test]
fn test_ordered_key_round_trip() {
    let key = OrderedKey {
        timestamp_ms: 1234567890000,
        node_id: 42,
        sequence: 100,
    };

    let encoded = key.encode_to_vec();
    let decoded = OrderedKey::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.timestamp_ms, 1234567890000);
    assert_eq!(decoded.node_id, 42);
    assert_eq!(decoded.sequence, 100);
}

#[test]
fn test_idempotency_token_round_trip() {
    let token = IdempotencyToken {
        generation_time: 9999,
        token: vec![0xAB; 16],
    };

    let encoded = token.encode_to_vec();
    let decoded = IdempotencyToken::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.generation_time, 9999);
    assert_eq!(decoded.token.len(), 16);
    assert!(decoded.token.iter().all(|&b| b == 0xAB));
}

#[test]
fn test_get_items_request_with_signals() {
    let mut signals = std::collections::HashMap::new();
    signals.insert("hint".to_string(), b"cache-warm".to_vec());

    let req = GetItemsRequest {
        namespace: "ns".to_string(),
        id: "rec".to_string(),
        predicate: None,
        selection: None,
        signals,
    };

    let encoded = req.encode_to_vec();
    let decoded = GetItemsRequest::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.signals.get("hint").unwrap(), b"cache-warm");
    assert!(decoded.predicate.is_none());
    assert!(decoded.selection.is_none());
}

#[test]
fn test_put_items_request_multiple_items() {
    let items: Vec<Item> = (0..100)
        .map(|i| Item {
            key: format!("key-{i}").into_bytes(),
            value: format!("val-{i}").into_bytes(),
            metadata: vec![],
            chunk: i as u32,
        })
        .collect();

    let req = PutItemsRequest {
        idempotency_token: None,
        namespace: "ns".to_string(),
        id: "rec".to_string(),
        items,
    };

    let encoded = req.encode_to_vec();
    let decoded = PutItemsRequest::decode(encoded.as_slice()).expect("decode failed");
    assert_eq!(decoded.items.len(), 100);
    assert_eq!(decoded.items[99].key, b"key-99");
    assert_eq!(decoded.items[99].chunk, 99);
    assert!(decoded.idempotency_token.is_none());
}
