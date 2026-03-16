use bytes::Bytes;
use flushdb_test::s3_test_utils::{cleanup_test_prefix, create_test_s3_backend, test_prefix};
use flushdb_types::{FlushError, StorageBackend};

// ===========================================================================
// Category 1: Basic CRUD (7 tests)
// ===========================================================================

#[tokio::test]
async fn test_s3_put_get_round_trip() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("put_get");

    let key = format!("{}mykey", prefix);
    let value = Bytes::from("hello world");

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_put_empty_bytes() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("put_empty");

    let key = format!("{}empty", prefix);
    let value = Bytes::new();

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);
    assert!(result.is_empty());

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_put_large_payload() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("put_large");

    let key = format!("{}large", prefix);
    let data = vec![0xABu8; 10 * 1024 * 1024]; // 10 MB
    let value = Bytes::from(data);

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result.len(), 10 * 1024 * 1024);
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_nonexistent_key() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("get_nonexistent");

    let key = format!("{}does-not-exist", prefix);
    let result = backend.get(&key).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::NotFound { .. } => {}
        other => panic!("expected NotFound, got: {:?}", other),
    }

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_delete_existing_key() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("delete_existing");

    let key = format!("{}to-delete", prefix);
    backend
        .put(&key, Bytes::from("temporary"))
        .await
        .unwrap();

    backend.delete(&key).await.unwrap();

    let result = backend.get(&key).await;
    assert!(matches!(result, Err(FlushError::NotFound { .. })));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_delete_nonexistent_idempotent() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("delete_idempotent");

    let key = format!("{}never-existed", prefix);
    // Deleting a non-existent key should succeed silently
    backend.delete(&key).await.unwrap();
    // Doing it again should also succeed
    backend.delete(&key).await.unwrap();

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_put_overwrite() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("put_overwrite");

    let key = format!("{}overwrite-me", prefix);
    backend.put(&key, Bytes::from("first")).await.unwrap();
    backend.put(&key, Bytes::from("second")).await.unwrap();

    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, Bytes::from("second"));

    cleanup_test_prefix(&backend, &prefix).await;
}

// ===========================================================================
// Category 2: Byte-Range Reads (9 tests)
// ===========================================================================

#[tokio::test]
async fn test_s3_get_range_first_n_bytes() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_first_n");

    let key = format!("{}range-data", prefix);
    let value = Bytes::from("abcdefghij"); // 10 bytes
    backend.put(&key, value).await.unwrap();

    let result = backend.get_range(&key, 0, 5).await.unwrap();
    assert_eq!(result, Bytes::from("abcde"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_last_n_bytes() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_last_n");

    let key = format!("{}range-data", prefix);
    let value = Bytes::from("abcdefghij"); // 10 bytes
    backend.put(&key, value).await.unwrap();

    let result = backend.get_range(&key, 7, 3).await.unwrap();
    assert_eq!(result, Bytes::from("hij"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_middle_slice() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_middle");

    let key = format!("{}range-data", prefix);
    let value = Bytes::from("abcdefghij");
    backend.put(&key, value).await.unwrap();

    let result = backend.get_range(&key, 3, 4).await.unwrap();
    assert_eq!(result, Bytes::from("defg"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_entire_object() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_entire");

    let key = format!("{}range-data", prefix);
    let value = Bytes::from("abcdefghij");
    backend.put(&key, value.clone()).await.unwrap();

    let result = backend.get_range(&key, 0, 10).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_extends_beyond() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_beyond");

    let key = format!("{}range-data", prefix);
    let value = Bytes::from("abcdefghij"); // 10 bytes
    backend.put(&key, value).await.unwrap();

    // Request 100 bytes starting at offset 5; S3 returns what's available
    let result = backend.get_range(&key, 5, 100).await.unwrap();
    assert_eq!(result, Bytes::from("fghij"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_nonexistent_key() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_nonexistent");

    let key = format!("{}no-such-key", prefix);
    let result = backend.get_range(&key, 0, 10).await;

    assert!(matches!(result, Err(FlushError::NotFound { .. })));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_sequential_reads() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_sequential");

    let key = format!("{}sequential", prefix);
    let value = Bytes::from("0123456789ABCDEF");
    backend.put(&key, value).await.unwrap();

    let r1 = backend.get_range(&key, 0, 4).await.unwrap();
    let r2 = backend.get_range(&key, 4, 4).await.unwrap();
    let r3 = backend.get_range(&key, 8, 4).await.unwrap();
    let r4 = backend.get_range(&key, 12, 4).await.unwrap();

    assert_eq!(r1, Bytes::from("0123"));
    assert_eq!(r2, Bytes::from("4567"));
    assert_eq!(r3, Bytes::from("89AB"));
    assert_eq!(r4, Bytes::from("CDEF"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_zero_length() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_zero_len");

    let key = format!("{}some-data", prefix);
    backend.put(&key, Bytes::from("data")).await.unwrap();

    // Zero-length range returns empty bytes without hitting S3
    let result = backend.get_range(&key, 0, 0).await.unwrap();
    assert!(result.is_empty());

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_get_range_offset_at_end() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_offset_end");

    let key = format!("{}offset-end", prefix);
    let value = Bytes::from("abcde"); // 5 bytes
    backend.put(&key, value).await.unwrap();

    // Offset at exact end of object with length > 0 should fail
    let result = backend.get_range(&key, 5, 1).await;
    assert!(result.is_err());

    cleanup_test_prefix(&backend, &prefix).await;
}

// ===========================================================================
// Category 3: Conditional Put / CAS (7 tests)
// ===========================================================================

#[tokio::test]
async fn test_s3_conditional_put_new_key() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("cond_new");

    let key = format!("{}fresh-key", prefix);
    backend
        .conditional_put(&key, Bytes::from("value1"))
        .await
        .unwrap();

    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, Bytes::from("value1"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_conditional_put_existing_key() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("cond_existing");

    let key = format!("{}exists", prefix);
    backend.put(&key, Bytes::from("original")).await.unwrap();

    let result = backend
        .conditional_put(&key, Bytes::from("conflict"))
        .await;
    assert!(matches!(result, Err(FlushError::PreconditionFailed { .. })));

    // Original value unchanged
    let val = backend.get(&key).await.unwrap();
    assert_eq!(val, Bytes::from("original"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_conditional_put_concurrent_two() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("cond_concurrent2");

    let key = format!("{}race", prefix);
    let b1 = backend.clone();
    let b2 = backend.clone();
    let k1 = key.clone();
    let k2 = key.clone();

    let (r1, r2) = tokio::join!(
        b1.conditional_put(&k1, Bytes::from("writer-1")),
        b2.conditional_put(&k2, Bytes::from("writer-2")),
    );

    // Exactly one should succeed, the other should fail
    let successes = [&r1, &r2].iter().filter(|r| r.is_ok()).count();
    let failures = [&r1, &r2]
        .iter()
        .filter(|r| matches!(r, Err(FlushError::PreconditionFailed { .. })))
        .count();

    assert_eq!(successes, 1, "exactly one writer should succeed");
    assert_eq!(failures, 1, "exactly one writer should fail with PreconditionFailed");

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_conditional_put_then_get() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("cond_then_get");

    let key = format!("{}cond-val", prefix);
    backend
        .conditional_put(&key, Bytes::from("cas-value"))
        .await
        .unwrap();

    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, Bytes::from("cas-value"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_conditional_put_delete_retry() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("cond_delete_retry");

    let key = format!("{}retry", prefix);

    // First conditional put succeeds
    backend
        .conditional_put(&key, Bytes::from("first"))
        .await
        .unwrap();

    // Second conditional put fails
    let result = backend.conditional_put(&key, Bytes::from("second")).await;
    assert!(matches!(result, Err(FlushError::PreconditionFailed { .. })));

    // Delete the key
    backend.delete(&key).await.unwrap();

    // Now conditional put should succeed again
    backend
        .conditional_put(&key, Bytes::from("third"))
        .await
        .unwrap();

    let val = backend.get(&key).await.unwrap();
    assert_eq!(val, Bytes::from("third"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_unconditional_then_conditional() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("uncond_then_cond");

    let key = format!("{}mixed", prefix);

    // Unconditional put
    backend.put(&key, Bytes::from("unconditional")).await.unwrap();

    // Conditional put on same key should fail
    let result = backend
        .conditional_put(&key, Bytes::from("conditional"))
        .await;
    assert!(matches!(result, Err(FlushError::PreconditionFailed { .. })));

    // Value should be unchanged
    let val = backend.get(&key).await.unwrap();
    assert_eq!(val, Bytes::from("unconditional"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_conditional_put_stress() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("cond_stress");

    let key = format!("{}stress-key", prefix);

    let mut handles = Vec::new();
    for i in 0..10 {
        let b = backend.clone();
        let k = key.clone();
        handles.push(tokio::spawn(async move {
            b.conditional_put(&k, Bytes::from(format!("writer-{}", i)))
                .await
        }));
    }

    let results: Vec<_> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let successes = results.iter().filter(|r| r.is_ok()).count();
    let precondition_failures = results
        .iter()
        .filter(|r| matches!(r, Err(FlushError::PreconditionFailed { .. })))
        .count();

    assert_eq!(successes, 1, "exactly 1 out of 10 should win");
    assert_eq!(precondition_failures, 9, "exactly 9 should fail with PreconditionFailed");

    cleanup_test_prefix(&backend, &prefix).await;
}

// ===========================================================================
// Category 4: List Prefix (6 tests)
// ===========================================================================

#[tokio::test]
async fn test_s3_list_prefix_specific() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("list_specific");

    let k1 = format!("{}aaa", prefix);
    let k2 = format!("{}bbb", prefix);
    let k3 = format!("{}ccc", prefix);

    backend.put(&k1, Bytes::from("1")).await.unwrap();
    backend.put(&k2, Bytes::from("2")).await.unwrap();
    backend.put(&k3, Bytes::from("3")).await.unwrap();

    let keys = backend.list_prefix(&prefix).await.unwrap();
    assert_eq!(keys.len(), 3);
    assert!(keys.contains(&k1));
    assert!(keys.contains(&k2));
    assert!(keys.contains(&k3));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_list_prefix_sorted() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("list_sorted");

    // Insert in reverse order
    let k3 = format!("{}zzz", prefix);
    let k2 = format!("{}mmm", prefix);
    let k1 = format!("{}aaa", prefix);

    backend.put(&k3, Bytes::from("3")).await.unwrap();
    backend.put(&k1, Bytes::from("1")).await.unwrap();
    backend.put(&k2, Bytes::from("2")).await.unwrap();

    let keys = backend.list_prefix(&prefix).await.unwrap();
    assert_eq!(keys.len(), 3);

    // Verify sorted order
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_list_prefix_nonexistent() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("list_nonexistent");

    // This specific prefix has no keys
    let keys = backend.list_prefix(&prefix).await.unwrap();
    assert!(keys.is_empty());

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_list_prefix_boundary() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("list_boundary");

    // Only keys with our exact prefix should be returned
    let k1 = format!("{}item1", prefix);
    let k2 = format!("{}item2", prefix);

    // A key with a similar but different prefix should NOT match
    let other_prefix = format!("{}x", prefix.trim_end_matches('/'));
    let k_other = format!("{}other", other_prefix);

    backend.put(&k1, Bytes::from("1")).await.unwrap();
    backend.put(&k2, Bytes::from("2")).await.unwrap();
    backend.put(&k_other, Bytes::from("x")).await.unwrap();

    let keys = backend.list_prefix(&prefix).await.unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&k1));
    assert!(keys.contains(&k2));
    assert!(!keys.contains(&k_other));

    cleanup_test_prefix(&backend, &prefix).await;
    // Also clean the other prefix
    let _ = backend.delete(&k_other).await;
}

#[tokio::test]
async fn test_s3_list_prefix_concurrent_writes() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("list_concurrent");

    let mut handles = Vec::new();
    for i in 0..20 {
        let b = backend.clone();
        let key = format!("{}key-{:03}", prefix, i);
        handles.push(tokio::spawn(async move {
            b.put(&key, Bytes::from(format!("val-{}", i))).await.unwrap();
        }));
    }
    futures::future::join_all(handles).await;

    let keys = backend.list_prefix(&prefix).await.unwrap();
    assert_eq!(keys.len(), 20);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_list_prefix_multiple_keys() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("list_multi");

    for i in 0..50 {
        let key = format!("{}entry-{:04}", prefix, i);
        backend
            .put(&key, Bytes::from(format!("data-{}", i)))
            .await
            .unwrap();
    }

    let keys = backend.list_prefix(&prefix).await.unwrap();
    assert_eq!(keys.len(), 50);

    // Verify all expected keys are present
    for i in 0..50 {
        let expected_key = format!("{}entry-{:04}", prefix, i);
        assert!(keys.contains(&expected_key), "missing key: {}", expected_key);
    }

    cleanup_test_prefix(&backend, &prefix).await;
}

// ===========================================================================
// Category 5: S3 Path Conventions (7 tests)
// ===========================================================================

#[tokio::test]
async fn test_s3_manifest_path() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("manifest_path");

    let key = format!("{}ns/default/manifest/MANIFEST-000001", prefix);
    let value = Bytes::from(r#"{"version":1}"#);

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_sstable_l0_path() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("sst_l0");

    let key = format!("{}ns/default/L0/sst-00000001.sst", prefix);
    let value = Bytes::from(vec![0u8; 256]);

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_sstable_l1_path() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("sst_l1");

    let key = format!("{}ns/default/L1/sst-00000042.sst", prefix);
    let value = Bytes::from(vec![0xFFu8; 128]);

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_slash_in_keys() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("slash_keys");

    let key = format!("{}a/b/c/d/e/f/deep-nested", prefix);
    let value = Bytes::from("nested-value");

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_manifest_list_sorted() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("manifest_list");

    let manifest_prefix = format!("{}ns/default/manifest/", prefix);
    let keys = vec![
        format!("{}MANIFEST-000003", manifest_prefix),
        format!("{}MANIFEST-000001", manifest_prefix),
        format!("{}MANIFEST-000002", manifest_prefix),
    ];

    for key in &keys {
        backend.put(key, Bytes::from("{}")).await.unwrap();
    }

    let listed = backend.list_prefix(&manifest_prefix).await.unwrap();
    assert_eq!(listed.len(), 3);

    // Should be lexicographically sorted
    assert_eq!(
        listed[0],
        format!("{}MANIFEST-000001", manifest_prefix)
    );
    assert_eq!(
        listed[1],
        format!("{}MANIFEST-000002", manifest_prefix)
    );
    assert_eq!(
        listed[2],
        format!("{}MANIFEST-000003", manifest_prefix)
    );

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_out_of_order_insertion_sorted_list() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("ooo_sorted");

    let keys = vec![
        format!("{}zebra", prefix),
        format!("{}apple", prefix),
        format!("{}mango", prefix),
        format!("{}banana", prefix),
    ];

    for key in &keys {
        backend.put(key, Bytes::from("x")).await.unwrap();
    }

    let listed = backend.list_prefix(&prefix).await.unwrap();
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(listed, expected);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_manifest_discovery_protocol() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("manifest_discovery");

    let manifest_prefix = format!("{}ns/myns/manifest/", prefix);

    // Write multiple manifest versions
    for i in 1..=5 {
        let key = format!("{}MANIFEST-{:06}", manifest_prefix, i);
        backend
            .put(&key, Bytes::from(format!(r#"{{"version":{}}}"#, i)))
            .await
            .unwrap();
    }

    // List and pick the latest (last in sorted order)
    let manifests = backend.list_prefix(&manifest_prefix).await.unwrap();
    assert_eq!(manifests.len(), 5);

    let latest = manifests.last().unwrap();
    assert!(latest.ends_with("MANIFEST-000005"));

    let data = backend.get(latest).await.unwrap();
    assert_eq!(data, Bytes::from(r#"{"version":5}"#));

    cleanup_test_prefix(&backend, &prefix).await;
}

// ===========================================================================
// Category 6: Parity Tests — S3 vs LocalFs (17 tests)
// ===========================================================================

mod parity {
    use bytes::Bytes;
    use flushdb_test::s3_test_utils::{cleanup_test_prefix, create_test_s3_backend, test_prefix};
    use flushdb_types::{FlushError, LocalFsBackend, StorageBackend};

    #[tokio::test]
    async fn test_parity_put_get_round_trip() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_put_get");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}key1", prefix);
        let local_key = "key1";
        let value = Bytes::from("parity-check");

        s3.put(&s3_key, value.clone()).await.unwrap();
        local.put(local_key, value.clone()).await.unwrap();

        let s3_result = s3.get(&s3_key).await.unwrap();
        let local_result = local.get(local_key).await.unwrap();
        assert_eq!(s3_result, local_result);

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_get_nonexistent() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_nonexist");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}nope", prefix);
        let s3_result = s3.get(&s3_key).await;
        let local_result = local.get("nope").await;

        assert!(matches!(s3_result, Err(FlushError::NotFound { .. })));
        assert!(matches!(local_result, Err(FlushError::NotFound { .. })));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_put_overwrite() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_overwrite");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}ow", prefix);
        let local_key = "ow";

        s3.put(&s3_key, Bytes::from("v1")).await.unwrap();
        local.put(local_key, Bytes::from("v1")).await.unwrap();

        s3.put(&s3_key, Bytes::from("v2")).await.unwrap();
        local.put(local_key, Bytes::from("v2")).await.unwrap();

        let s3_val = s3.get(&s3_key).await.unwrap();
        let local_val = local.get(local_key).await.unwrap();
        assert_eq!(s3_val, local_val);
        assert_eq!(s3_val, Bytes::from("v2"));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_get_range_partial() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_range_partial");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let data = Bytes::from("0123456789");
        let s3_key = format!("{}data", prefix);
        let local_key = "data";

        s3.put(&s3_key, data.clone()).await.unwrap();
        local.put(local_key, data).await.unwrap();

        let s3_range = s3.get_range(&s3_key, 2, 5).await.unwrap();
        let local_range = local.get_range(local_key, 2, 5).await.unwrap();
        assert_eq!(s3_range, local_range);
        assert_eq!(s3_range, Bytes::from("23456"));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_get_range_first_byte() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_range_first");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let data = Bytes::from("ABCDEF");
        let s3_key = format!("{}fb", prefix);
        let local_key = "fb";

        s3.put(&s3_key, data.clone()).await.unwrap();
        local.put(local_key, data).await.unwrap();

        let s3_r = s3.get_range(&s3_key, 0, 1).await.unwrap();
        let local_r = local.get_range(local_key, 0, 1).await.unwrap();
        assert_eq!(s3_r, local_r);
        assert_eq!(s3_r, Bytes::from("A"));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_get_range_full_object() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_range_full");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let data = Bytes::from("fulldata");
        let s3_key = format!("{}full", prefix);
        let local_key = "full";

        s3.put(&s3_key, data.clone()).await.unwrap();
        local.put(local_key, data.clone()).await.unwrap();

        let s3_r = s3.get_range(&s3_key, 0, 8).await.unwrap();
        let local_r = local.get_range(local_key, 0, 8).await.unwrap();
        assert_eq!(s3_r, local_r);
        assert_eq!(s3_r, data);

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_get_range_nonexistent() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_range_ne");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}missing", prefix);
        let s3_r = s3.get_range(&s3_key, 0, 5).await;
        let local_r = local.get_range("missing", 0, 5).await;

        assert!(matches!(s3_r, Err(FlushError::NotFound { .. })));
        assert!(matches!(local_r, Err(FlushError::NotFound { .. })));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_get_range_zero_length() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_range_zero");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let data = Bytes::from("payload");
        let s3_key = format!("{}zl", prefix);
        let local_key = "zl";

        s3.put(&s3_key, data.clone()).await.unwrap();
        local.put(local_key, data).await.unwrap();

        // Both should return empty bytes for zero-length on existing key
        let s3_r = s3.get_range(&s3_key, 0, 0).await.unwrap();
        let local_r = local.get_range(local_key, 0, 0).await.unwrap();
        assert_eq!(s3_r, local_r);
        assert!(s3_r.is_empty());

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_conditional_put_new() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_cond_new");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let val = Bytes::from("conditional");
        let s3_key = format!("{}cn", prefix);
        let local_key = "cn";

        s3.conditional_put(&s3_key, val.clone()).await.unwrap();
        local.conditional_put(local_key, val.clone()).await.unwrap();

        let s3_v = s3.get(&s3_key).await.unwrap();
        let local_v = local.get(local_key).await.unwrap();
        assert_eq!(s3_v, local_v);
        assert_eq!(s3_v, val);

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_conditional_put_existing() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_cond_exist");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}ce", prefix);
        let local_key = "ce";

        s3.put(&s3_key, Bytes::from("first")).await.unwrap();
        local.put(local_key, Bytes::from("first")).await.unwrap();

        let s3_r = s3.conditional_put(&s3_key, Bytes::from("second")).await;
        let local_r = local.conditional_put(local_key, Bytes::from("second")).await;

        assert!(matches!(s3_r, Err(FlushError::PreconditionFailed { .. })));
        assert!(matches!(local_r, Err(FlushError::PreconditionFailed { .. })));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_conditional_put_after_delete() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_cond_del");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}cd", prefix);
        let local_key = "cd";

        // Create then delete
        s3.put(&s3_key, Bytes::from("orig")).await.unwrap();
        local.put(local_key, Bytes::from("orig")).await.unwrap();
        s3.delete(&s3_key).await.unwrap();
        local.delete(local_key).await.unwrap();

        // Conditional put should now succeed on both
        s3.conditional_put(&s3_key, Bytes::from("after-delete"))
            .await
            .unwrap();
        local
            .conditional_put(local_key, Bytes::from("after-delete"))
            .await
            .unwrap();

        let s3_v = s3.get(&s3_key).await.unwrap();
        let local_v = local.get(local_key).await.unwrap();
        assert_eq!(s3_v, local_v);
        assert_eq!(s3_v, Bytes::from("after-delete"));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_delete_idempotent() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_del_idemp");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}ghost", prefix);
        let local_key = "ghost";

        // Delete non-existent: both should succeed
        s3.delete(&s3_key).await.unwrap();
        local.delete(local_key).await.unwrap();

        // Delete again: still fine
        s3.delete(&s3_key).await.unwrap();
        local.delete(local_key).await.unwrap();

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_delete_then_get() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_del_get");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_key = format!("{}dg", prefix);
        let local_key = "dg";

        s3.put(&s3_key, Bytes::from("ephemeral")).await.unwrap();
        local.put(local_key, Bytes::from("ephemeral")).await.unwrap();

        s3.delete(&s3_key).await.unwrap();
        local.delete(local_key).await.unwrap();

        let s3_r = s3.get(&s3_key).await;
        let local_r = local.get(local_key).await;

        assert!(matches!(s3_r, Err(FlushError::NotFound { .. })));
        assert!(matches!(local_r, Err(FlushError::NotFound { .. })));

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_list_prefix_sorted() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_list_sort");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        // Use prefix-relative keys for local, full prefix keys for S3
        let items = ["charlie", "alpha", "bravo"];

        for item in &items {
            let s3_key = format!("{}{}", prefix, item);
            s3.put(&s3_key, Bytes::from(*item)).await.unwrap();
            let local_key = format!("pfx/{}", item);
            local.put(&local_key, Bytes::from(*item)).await.unwrap();
        }

        let s3_keys = s3.list_prefix(&prefix).await.unwrap();
        let local_keys = local.list_prefix("pfx/").await.unwrap();

        assert_eq!(s3_keys.len(), local_keys.len());
        // Both should be sorted
        let s3_sorted: Vec<_> = s3_keys.iter().map(|k| k.strip_prefix(&prefix).unwrap()).collect();
        let local_sorted: Vec<_> = local_keys.iter().map(|k| k.strip_prefix("pfx/").unwrap()).collect();
        assert_eq!(s3_sorted, local_sorted);
        assert_eq!(s3_sorted, vec!["alpha", "bravo", "charlie"]);

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_list_prefix_empty() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_list_empty");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let s3_keys = s3.list_prefix(&prefix).await.unwrap();
        let local_keys = local.list_prefix("nonexistent/").await.unwrap();

        assert_eq!(s3_keys.len(), 0);
        assert_eq!(local_keys.len(), 0);

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_binary_data_round_trip() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_binary");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        // All 256 byte values
        let data: Vec<u8> = (0..=255).collect();
        let value = Bytes::from(data);

        let s3_key = format!("{}bin", prefix);
        let local_key = "bin";

        s3.put(&s3_key, value.clone()).await.unwrap();
        local.put(local_key, value.clone()).await.unwrap();

        let s3_v = s3.get(&s3_key).await.unwrap();
        let local_v = local.get(local_key).await.unwrap();
        assert_eq!(s3_v, local_v);
        assert_eq!(s3_v, value);

        cleanup_test_prefix(&s3, &prefix).await;
    }

    #[tokio::test]
    async fn test_parity_nested_path_keys() {
        let s3 = create_test_s3_backend().await;
        let prefix = test_prefix("par_nested");
        let dir = tempfile::TempDir::new().unwrap();
        let local = LocalFsBackend::new(dir.path().to_path_buf());

        let nested_suffix = "a/b/c/d/file.dat";
        let s3_key = format!("{}{}", prefix, nested_suffix);
        let local_key = nested_suffix;
        let value = Bytes::from("deep");

        s3.put(&s3_key, value.clone()).await.unwrap();
        local.put(local_key, value.clone()).await.unwrap();

        let s3_v = s3.get(&s3_key).await.unwrap();
        let local_v = local.get(local_key).await.unwrap();
        assert_eq!(s3_v, local_v);
        assert_eq!(s3_v, value);

        cleanup_test_prefix(&s3, &prefix).await;
    }
}

// ===========================================================================
// Category 7: Error Handling and Edge Cases (9 tests)
// ===========================================================================

#[tokio::test]
async fn test_s3_long_key_names() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("long_key");

    // S3 supports keys up to 1024 bytes. Account for the shard prefix (4 chars)
    // and the test prefix (~60 chars) so that the total sharded key stays under 1024.
    let long_suffix = "x".repeat(200);
    let key = format!("{}{}", prefix, long_suffix);
    let value = Bytes::from("long-key-value");

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_binary_data_values() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("binary_data");

    let key = format!("{}binary", prefix);
    let data: Vec<u8> = (0..=255).cycle().take(1024).collect();
    let value = Bytes::from(data);

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_empty_prefix_listing_empty_backend() {
    let backend = create_test_s3_backend().await;
    // Use a unique prefix guaranteed to have no data
    let prefix = test_prefix("empty_listing_unique");

    let keys = backend.list_prefix(&prefix).await.unwrap();
    assert!(keys.is_empty());

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_concurrent_read_write_same_key() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("concurrent_rw");

    let key = format!("{}shared", prefix);
    // Seed the key with initial value
    backend.put(&key, Bytes::from("initial")).await.unwrap();

    let mut handles = Vec::new();

    // Spawn writers
    for i in 0..5 {
        let b = backend.clone();
        let k = key.clone();
        handles.push(tokio::spawn(async move {
            b.put(&k, Bytes::from(format!("writer-{}", i)))
                .await
                .unwrap();
        }));
    }

    // Spawn readers
    for _ in 0..5 {
        let b = backend.clone();
        let k = key.clone();
        handles.push(tokio::spawn(async move {
            // Reads should not error — they get some version of the value
            let result = b.get(&k).await;
            assert!(result.is_ok());
        }));
    }

    futures::future::join_all(handles).await;

    // Final read should succeed
    let final_val = backend.get(&key).await.unwrap();
    assert!(!final_val.is_empty());

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_zero_byte_value() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("zero_byte");

    let key = format!("{}zero", prefix);
    let value = Bytes::new();

    backend.put(&key, value.clone()).await.unwrap();
    let result = backend.get(&key).await.unwrap();
    assert_eq!(result, value);
    assert!(result.is_empty());

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_special_url_chars_in_keys() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("special_chars");

    let special_keys = vec![
        format!("{}key with spaces", prefix),
        format!("{}key+plus", prefix),
        format!("{}key=equals", prefix),
        format!("{}key&ampersand", prefix),
    ];

    for key in &special_keys {
        backend.put(key, Bytes::from("special")).await.unwrap();
        let result = backend.get(key).await.unwrap();
        assert_eq!(result, Bytes::from("special"), "failed for key: {}", key);
    }

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_overwrite_then_range_read() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("overwrite_range");

    let key = format!("{}owrange", prefix);

    // Write first version
    backend.put(&key, Bytes::from("AAAAAAAAAA")).await.unwrap();

    // Overwrite with different data
    backend.put(&key, Bytes::from("BBBBBBBBBB")).await.unwrap();

    // Range read should see the new data
    let result = backend.get_range(&key, 0, 5).await.unwrap();
    assert_eq!(result, Bytes::from("BBBBB"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_range_read_at_end_boundary() {
    let backend = create_test_s3_backend().await;
    let prefix = test_prefix("range_end_boundary");

    let key = format!("{}boundary", prefix);
    let value = Bytes::from("abcde"); // 5 bytes
    backend.put(&key, value).await.unwrap();

    // Read last byte only
    let result = backend.get_range(&key, 4, 1).await.unwrap();
    assert_eq!(result, Bytes::from("e"));

    // Read last 2 bytes
    let result = backend.get_range(&key, 3, 2).await.unwrap();
    assert_eq!(result, Bytes::from("de"));

    // Read extending past end — should return only available bytes
    let result = backend.get_range(&key, 3, 100).await.unwrap();
    assert_eq!(result, Bytes::from("de"));

    cleanup_test_prefix(&backend, &prefix).await;
}

#[tokio::test]
async fn test_s3_hash_prefix_distribution() {
    let backend = create_test_s3_backend().await;

    // Verify that shard_key produces the expected {NNN}/{key} format
    let sharded = backend.shard_key("test-key");
    assert!(
        sharded.contains('/'),
        "shard_key should contain a slash separator"
    );

    let parts: Vec<&str> = sharded.splitn(2, '/').collect();
    assert_eq!(parts.len(), 2);

    // The shard number should be 3 digits zero-padded
    let shard_num = parts[0];
    assert_eq!(shard_num.len(), 3, "shard prefix should be 3 digits");
    let parsed: u32 = shard_num.parse().expect("shard prefix should be numeric");
    assert!(parsed < 128, "shard number should be < 128");

    // The logical key should be preserved
    assert_eq!(parts[1], "test-key");

    // Verify multiple keys distribute across shards
    let mut shards_seen = std::collections::HashSet::new();
    for i in 0..1000 {
        let key = format!("key-{}", i);
        let sharded = backend.shard_key(&key);
        let shard: u32 = sharded.splitn(2, '/').next().unwrap().parse().unwrap();
        shards_seen.insert(shard);
    }

    // With 1000 keys and 128 shards, we should see a reasonable distribution
    assert!(
        shards_seen.len() > 50,
        "expected at least 50 distinct shards, got {}",
        shards_seen.len()
    );
}
