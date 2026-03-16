use bytes::Bytes;
use flushdb_types::{FlushError, LocalFsBackend, StorageBackend};
use tempfile::tempdir;

// Basic CRUD

#[tokio::test]
async fn test_put_get_round_trip() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let data = Bytes::from("hello world");
    backend.put("key1", data.clone()).await.expect("put failed");

    let result = backend.get("key1").await.expect("get failed");
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_put_empty_value() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let data = Bytes::new();
    backend
        .put("empty", data.clone())
        .await
        .expect("put failed");

    let result = backend.get("empty").await.expect("get failed");
    assert_eq!(result, data);
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_put_large_value() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    // 10 MB — typical SSTable size per TESTING.md.
    let data = Bytes::from(vec![0xABu8; 10_000_000]);
    backend
        .put("large", data.clone())
        .await
        .expect("put failed");

    let result = backend.get("large").await.expect("get failed");
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_get_not_found() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let err = backend.get("nonexistent").await.unwrap_err();
    match err {
        FlushError::NotFound { key } => assert_eq!(key, "nonexistent"),
        other => panic!("expected NotFound, got {:?}", other),
    }
}

#[tokio::test]
async fn test_put_overwrite() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("key", Bytes::from("first"))
        .await
        .expect("put failed");
    backend
        .put("key", Bytes::from("second"))
        .await
        .expect("put failed");

    let result = backend.get("key").await.expect("get failed");
    assert_eq!(result, Bytes::from("second"));
}

#[tokio::test]
async fn test_delete_existing() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("key", Bytes::from("data"))
        .await
        .expect("put failed");
    backend.delete("key").await.expect("delete failed");

    let err = backend.get("key").await.unwrap_err();
    match err {
        FlushError::NotFound { key } => assert_eq!(key, "key"),
        other => panic!("expected NotFound, got {:?}", other),
    }
}

#[tokio::test]
async fn test_delete_idempotent() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .delete("nonexistent")
        .await
        .expect("delete should be idempotent");
}

#[tokio::test]
async fn test_put_with_slashes() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let data = Bytes::from("nested data");
    backend
        .put("a/b/c", data.clone())
        .await
        .expect("put failed");

    let result = backend.get("a/b/c").await.expect("get failed");
    assert_eq!(result, data);

    assert!(dir.path().join("a").join("b").is_dir());
}

// Byte-range reads

#[tokio::test]
async fn test_get_range_first_bytes() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("0123456789"))
        .await
        .expect("put failed");

    let result = backend.get_range("file", 0, 5).await.expect("get_range failed");
    assert_eq!(result, Bytes::from("01234"));
}

#[tokio::test]
async fn test_get_range_last_bytes() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("0123456789"))
        .await
        .expect("put failed");

    // Read the last 4 bytes (footer simulation).
    let result = backend.get_range("file", 6, 4).await.expect("get_range failed");
    assert_eq!(result, Bytes::from("6789"));
}

#[tokio::test]
async fn test_get_range_middle() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("0123456789"))
        .await
        .expect("put failed");

    let result = backend.get_range("file", 3, 4).await.expect("get_range failed");
    assert_eq!(result, Bytes::from("3456"));
}

#[tokio::test]
async fn test_get_range_full() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let data = Bytes::from("hello world");
    backend
        .put("file", data.clone())
        .await
        .expect("put failed");

    let result = backend
        .get_range("file", 0, data.len() as u64)
        .await
        .expect("get_range failed");
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_get_range_beyond_end() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("short"))
        .await
        .expect("put failed");

    // Request more bytes than available — returns partial content.
    let result = backend
        .get_range("file", 2, 100)
        .await
        .expect("get_range failed");
    assert_eq!(result, Bytes::from("ort"));
}

#[tokio::test]
async fn test_get_range_not_found() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let err = backend.get_range("missing", 0, 10).await.unwrap_err();
    assert!(matches!(err, FlushError::NotFound { .. }));
}

#[tokio::test]
async fn test_get_range_zero_length() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("data"))
        .await
        .expect("put failed");

    let result = backend
        .get_range("file", 0, 0)
        .await
        .expect("get_range failed");
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_get_range_zero_length_not_found() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let err = backend.get_range("missing", 0, 0).await.unwrap_err();
    assert!(matches!(err, FlushError::NotFound { .. }));
}

#[tokio::test]
async fn test_get_range_offset_at_end() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("data"))
        .await
        .expect("put failed");

    let err = backend.get_range("file", 4, 1).await.unwrap_err();
    assert!(matches!(err, FlushError::Io(_)));
}

#[tokio::test]
async fn test_get_range_offset_beyond_end() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("data"))
        .await
        .expect("put failed");

    let err = backend.get_range("file", 100, 5).await.unwrap_err();
    assert!(matches!(err, FlushError::Io(_)));
}

#[tokio::test]
async fn test_sequential_ranges() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    // Simulate SSTable: header | data blocks | index | footer
    let mut sstable = Vec::new();
    let header = b"HDR_";
    let data = b"DATA_BLOCK_CONTENTS_HERE";
    let index = b"INDEX_BLOCK";
    let footer = b"FOOT";

    sstable.extend_from_slice(header);
    sstable.extend_from_slice(data);
    sstable.extend_from_slice(index);
    sstable.extend_from_slice(footer);

    backend
        .put("table.sst", Bytes::from(sstable.clone()))
        .await
        .expect("put failed");

    // Read header.
    let r = backend
        .get_range("table.sst", 0, header.len() as u64)
        .await
        .expect("get_range failed");
    assert_eq!(r.as_ref(), header);

    // Read data block.
    let offset = header.len() as u64;
    let r = backend
        .get_range("table.sst", offset, data.len() as u64)
        .await
        .expect("get_range failed");
    assert_eq!(r.as_ref(), data);

    // Read index.
    let offset = (header.len() + data.len()) as u64;
    let r = backend
        .get_range("table.sst", offset, index.len() as u64)
        .await
        .expect("get_range failed");
    assert_eq!(r.as_ref(), index);

    // Read footer.
    let offset = (header.len() + data.len() + index.len()) as u64;
    let r = backend
        .get_range("table.sst", offset, footer.len() as u64)
        .await
        .expect("get_range failed");
    assert_eq!(r.as_ref(), footer);
}

// Conditional put

#[tokio::test]
async fn test_conditional_put_new_key() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .conditional_put("new_key", Bytes::from("value"))
        .await
        .expect("conditional_put should succeed for new key");
}

#[tokio::test]
async fn test_conditional_put_existing_key() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .conditional_put("key", Bytes::from("first"))
        .await
        .expect("conditional_put failed");

    let err = backend
        .conditional_put("key", Bytes::from("second"))
        .await
        .unwrap_err();
    assert!(matches!(err, FlushError::PreconditionFailed { .. }));
}

#[tokio::test]
async fn test_conditional_put_then_get() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let data = Bytes::from("value");
    backend
        .conditional_put("key", data.clone())
        .await
        .expect("conditional_put failed");

    let result = backend.get("key").await.expect("get failed");
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_conditional_put_after_delete() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .conditional_put("key", Bytes::from("first"))
        .await
        .expect("conditional_put failed");
    backend.delete("key").await.expect("delete failed");

    backend
        .conditional_put("key", Bytes::from("second"))
        .await
        .expect("conditional_put after delete should succeed");

    let result = backend.get("key").await.expect("get failed");
    assert_eq!(result, Bytes::from("second"));
}

#[tokio::test]
async fn test_put_then_conditional_put() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("key", Bytes::from("via put"))
        .await
        .expect("put failed");

    let err = backend
        .conditional_put("key", Bytes::from("via cond put"))
        .await
        .unwrap_err();
    assert!(matches!(err, FlushError::PreconditionFailed { .. }));
}

#[tokio::test]
async fn test_concurrent_conditional_put() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = std::sync::Arc::new(LocalFsBackend::new(dir.path()));

    let mut handles = Vec::new();
    for i in 0..10 {
        let b = backend.clone();
        handles.push(tokio::spawn(async move {
            b.conditional_put("race_key", Bytes::from(format!("writer-{}", i)))
                .await
        }));
    }

    let mut successes = 0;
    let mut failures = 0;
    for h in handles {
        match h.await.expect("task panicked") {
            Ok(()) => successes += 1,
            Err(FlushError::PreconditionFailed { .. }) => failures += 1,
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    assert_eq!(successes, 1, "exactly one writer should succeed");
    assert_eq!(failures, 9, "nine writers should fail");
}

// List prefix

#[tokio::test]
async fn test_list_prefix_empty() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let keys = backend.list_prefix("").await.expect("list_prefix failed");
    assert!(keys.is_empty());
}

#[tokio::test]
async fn test_list_prefix_all() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend.put("a", Bytes::from("1")).await.expect("put failed");
    backend.put("b", Bytes::from("2")).await.expect("put failed");
    backend.put("c", Bytes::from("3")).await.expect("put failed");

    let keys = backend.list_prefix("").await.expect("list_prefix failed");
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn test_list_prefix_filter() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("data/1", Bytes::from("a"))
        .await
        .expect("put failed");
    backend
        .put("data/2", Bytes::from("b"))
        .await
        .expect("put failed");
    backend
        .put("meta/1", Bytes::from("c"))
        .await
        .expect("put failed");

    let keys = backend
        .list_prefix("data/")
        .await
        .expect("list_prefix failed");
    assert_eq!(keys, vec!["data/1", "data/2"]);
}

#[tokio::test]
async fn test_list_prefix_sorted() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    // Insert in non-sorted order.
    backend
        .put("cherry", Bytes::from("3"))
        .await
        .expect("put failed");
    backend
        .put("apple", Bytes::from("1"))
        .await
        .expect("put failed");
    backend
        .put("banana", Bytes::from("2"))
        .await
        .expect("put failed");

    let keys = backend.list_prefix("").await.expect("list_prefix failed");
    assert_eq!(keys, vec!["apple", "banana", "cherry"]);
}

#[tokio::test]
async fn test_list_prefix_no_match() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend.put("a", Bytes::from("1")).await.expect("put failed");
    backend.put("b", Bytes::from("2")).await.expect("put failed");

    let keys = backend
        .list_prefix("z")
        .await
        .expect("list_prefix failed");
    assert!(keys.is_empty());
}

#[tokio::test]
async fn test_list_prefix_boundary() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("ab", Bytes::from("1"))
        .await
        .expect("put failed");
    backend
        .put("a/x", Bytes::from("2"))
        .await
        .expect("put failed");
    backend
        .put("abc", Bytes::from("3"))
        .await
        .expect("put failed");
    backend
        .put("b", Bytes::from("4"))
        .await
        .expect("put failed");

    let keys = backend.list_prefix("a").await.expect("list_prefix failed");
    assert_eq!(keys, vec!["a/x", "ab", "abc"]);
}

#[tokio::test]
async fn test_list_prefix_nested() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("ns/sst/001.sst", Bytes::from("1"))
        .await
        .expect("put failed");
    backend
        .put("ns/sst/002.sst", Bytes::from("2"))
        .await
        .expect("put failed");
    backend
        .put("ns/manifest/current", Bytes::from("3"))
        .await
        .expect("put failed");

    let keys = backend
        .list_prefix("ns/sst/")
        .await
        .expect("list_prefix failed");
    assert_eq!(keys, vec!["ns/sst/001.sst", "ns/sst/002.sst"]);
}

#[tokio::test]
async fn test_list_prefix_after_delete() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend.put("a", Bytes::from("1")).await.expect("put failed");
    backend.put("b", Bytes::from("2")).await.expect("put failed");
    backend.put("c", Bytes::from("3")).await.expect("put failed");

    backend.delete("b").await.expect("delete failed");

    let keys = backend.list_prefix("").await.expect("list_prefix failed");
    assert_eq!(keys, vec!["a", "c"]);
}

// S3 path conventions

#[tokio::test]
async fn test_s3_manifest_path() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let manifest = Bytes::from(r#"{"version":1}"#);
    backend
        .put("tenant-42/manifests/manifest-00001", manifest.clone())
        .await
        .expect("put failed");

    let result = backend
        .get("tenant-42/manifests/manifest-00001")
        .await
        .expect("get failed");
    assert_eq!(result, manifest);
}

#[tokio::test]
async fn test_s3_sstable_path() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let data = Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    backend
        .put("tenant-42/sstables/L0/sst-00001.sst", data.clone())
        .await
        .expect("put failed");

    let result = backend
        .get("tenant-42/sstables/L0/sst-00001.sst")
        .await
        .expect("get failed");
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_s3_nested_sstable_path() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("ns/L0/001.sst", Bytes::from("l0"))
        .await
        .expect("put failed");
    backend
        .put("ns/L1/001.sst", Bytes::from("l1"))
        .await
        .expect("put failed");

    let keys = backend
        .list_prefix("ns/")
        .await
        .expect("list_prefix failed");
    assert_eq!(keys, vec!["ns/L0/001.sst", "ns/L1/001.sst"]);
}

#[tokio::test]
async fn test_manifest_discovery_protocol() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    for i in 1..=5u32 {
        let key = format!("tenant/manifests/manifest-{:05}", i);
        let body = format!(r#"{{"epoch":{}}}"#, i);
        backend
            .put(&key, Bytes::from(body))
            .await
            .expect("put failed");
    }

    let manifests = backend
        .list_prefix("tenant/manifests/manifest-")
        .await
        .expect("list_prefix failed");
    assert_eq!(manifests.len(), 5);

    let highest = manifests.last().expect("no manifests found");
    assert_eq!(highest, "tenant/manifests/manifest-00005");

    let next_key = "tenant/manifests/manifest-00006";
    backend
        .conditional_put(next_key, Bytes::from(r#"{"epoch":6}"#))
        .await
        .expect("conditional_put should succeed for new manifest");

    let manifests = backend
        .list_prefix("tenant/manifests/manifest-")
        .await
        .expect("list_prefix failed");
    assert_eq!(manifests.len(), 6);
    assert_eq!(
        manifests.last().expect("no manifests found"),
        "tenant/manifests/manifest-00006"
    );
}

// Edge cases

#[tokio::test]
async fn test_binary_data_round_trip() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let data: Vec<u8> = (0..=255).collect();
    let bytes = Bytes::from(data.clone());

    backend
        .put("binary", bytes.clone())
        .await
        .expect("put failed");
    let result = backend.get("binary").await.expect("get failed");
    assert_eq!(result, bytes);
    assert_eq!(result.len(), 256);
}

#[tokio::test]
async fn test_overwrite_then_range_read() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("file", Bytes::from("old_data"))
        .await
        .expect("put failed");
    backend
        .put("file", Bytes::from("new_data"))
        .await
        .expect("put failed");

    let result = backend
        .get_range("file", 0, 3)
        .await
        .expect("get_range failed");
    assert_eq!(result, Bytes::from("new"));
}

#[tokio::test]
async fn test_deeply_nested_path() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    let key = "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z";
    let data = Bytes::from("deep");

    backend.put(key, data.clone()).await.expect("put failed");
    let result = backend.get(key).await.expect("get failed");
    assert_eq!(result, data);

    let keys = backend.list_prefix("a/b/c/").await.expect("list_prefix failed");
    assert_eq!(keys, vec![key]);
}

// Error handling

#[tokio::test]
async fn test_special_characters_in_keys() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    // Test keys with special URL characters (except `/` which creates dirs,
    // and characters that are invalid in file names on macOS/Linux).
    let special_keys = ["key+plus", "key%percent", "key@at", "key=equals"];

    for (i, key) in special_keys.iter().enumerate() {
        let data = Bytes::from(format!("value-{}", i));
        backend
            .put(key, data.clone())
            .await
            .unwrap_or_else(|e| panic!("put failed for key '{}': {:?}", key, e));

        let result = backend
            .get(key)
            .await
            .unwrap_or_else(|e| panic!("get failed for key '{}': {:?}", key, e));
        assert_eq!(result, data, "round-trip failed for key '{}'", key);
    }
}

#[tokio::test]
async fn test_concurrent_read_write() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = std::sync::Arc::new(LocalFsBackend::new(dir.path()));

    backend
        .put("shared", Bytes::from("initial"))
        .await
        .expect("put failed");

    let mut handles = Vec::new();

    // 5 writers overwriting the same key concurrently.
    for i in 0..5 {
        let b = backend.clone();
        handles.push(tokio::spawn(async move {
            b.put("shared", Bytes::from(format!("writer-{}", i)))
                .await
                .expect("concurrent put failed");
        }));
    }

    // 5 readers reading concurrently.
    for _ in 0..5 {
        let b = backend.clone();
        handles.push(tokio::spawn(async move {
            // Should never error — key always exists.
            let result = b.get("shared").await.expect("concurrent get failed");
            assert!(!result.is_empty(), "concurrent read returned empty data");
        }));
    }

    for h in handles {
        h.await.expect("task panicked");
    }

    let final_val = backend.get("shared").await.expect("final get failed");
    let final_str = String::from_utf8(final_val.to_vec()).expect("not utf8");
    assert!(
        final_str.starts_with("writer-"),
        "unexpected final value: {}",
        final_str
    );
}

#[tokio::test]
async fn test_long_key_name() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    // Use path segments to stay under filesystem limits while testing long keys.
    // Total length ~200 chars with segments.
    let segments: Vec<String> = (0..20).map(|i| format!("seg{:05}", i)).collect();
    let key = segments.join("/");
    let data = Bytes::from("long key data");

    backend.put(&key, data.clone()).await.expect("put failed");
    let result = backend.get(&key).await.expect("get failed");
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_s3_path_out_of_order_insertion_sorted() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    // Insert manifest files in non-sorted order.
    for i in [5, 2, 4, 1, 3] {
        let key = format!("flushdb/ns/manifests/{:020}", i);
        backend
            .put(&key, Bytes::from(format!("epoch-{}", i)))
            .await
            .expect("put failed");
    }

    let keys = backend
        .list_prefix("flushdb/ns/manifests/")
        .await
        .expect("list_prefix failed");
    assert_eq!(keys.len(), 5);

    // Must be lexicographically sorted regardless of insertion order.
    for i in 0..keys.len() - 1 {
        assert!(
            keys[i] < keys[i + 1],
            "keys not sorted: {:?} >= {:?}",
            keys[i],
            keys[i + 1]
        );
    }
}

#[tokio::test]
async fn test_list_prefix_excludes_tmp_files() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .put("real_key", Bytes::from("data"))
        .await
        .expect("put failed");

    // Plant a .tmp file to simulate a crash during atomic put.
    tokio::fs::write(dir.path().join(".tmp.abcdef"), b"partial")
        .await
        .expect("failed to write tmp file");

    let keys = backend
        .list_prefix("")
        .await
        .expect("list_prefix failed");
    assert_eq!(
        keys,
        vec!["real_key"],
        ".tmp.* files must not appear in list_prefix"
    );
}

#[tokio::test]
async fn test_put_leaves_no_tmp_residue() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    for i in 0..10 {
        backend
            .put(&format!("key-{i}"), Bytes::from(format!("value-{i}")))
            .await
            .expect("put failed");
    }

    let keys = backend
        .list_prefix("")
        .await
        .expect("list_prefix failed");
    assert_eq!(keys.len(), 10);
    for key in &keys {
        assert!(
            !key.contains(".tmp."),
            "tmp file leaked into listing: {key}"
        );
    }
}

#[tokio::test]
async fn test_double_delete_then_conditional_put() {
    let dir = tempdir().expect("failed to create tempdir");
    let backend = LocalFsBackend::new(dir.path());

    backend
        .conditional_put("key", Bytes::from("first"))
        .await
        .expect("conditional_put failed");
    backend.delete("key").await.expect("delete failed");
    backend
        .delete("key")
        .await
        .expect("second delete should be idempotent");

    backend
        .conditional_put("key", Bytes::from("second"))
        .await
        .expect("conditional_put after double delete should succeed");

    let result = backend.get("key").await.expect("get failed");
    assert_eq!(result, Bytes::from("second"));
}
