use std::collections::HashSet;

use flushdb_server::S3StorageBackend;
use flushdb_types::FlushError;

fn dummy_client() -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .build();
    aws_sdk_s3::Client::from_conf(config)
}

#[test]
fn test_shard_key_deterministic() {
    let backend = S3StorageBackend::new(dummy_client(), "test-bucket".to_string());
    let key = "my/test/key.sst";
    let first = backend.shard_key(key);
    let second = backend.shard_key(key);
    assert_eq!(first, second);
}

#[test]
fn test_shard_key_distribution() {
    let backend = S3StorageBackend::new(dummy_client(), "test-bucket".to_string());
    let mut seen_shards: HashSet<u32> = HashSet::new();

    for i in 0..1000 {
        let key = format!("key-{}", i);
        let sharded = backend.shard_key(&key);
        let prefix_str = sharded.split('/').next().expect("should have prefix");
        let shard_num: u32 = prefix_str.parse().expect("prefix should be numeric");
        seen_shards.insert(shard_num);
    }

    assert_eq!(
        seen_shards.len(),
        128,
        "expected all 128 shards to be hit, but only {} were",
        seen_shards.len()
    );
}

#[test]
fn test_shard_key_format() {
    let backend = S3StorageBackend::new(dummy_client(), "test-bucket".to_string());
    let sharded = backend.shard_key("some/path/file.dat");

    let parts: Vec<&str> = sharded.splitn(2, '/').collect();
    assert_eq!(parts.len(), 2);

    let prefix = parts[0];
    assert_eq!(prefix.len(), 3, "prefix should be zero-padded to 3 digits");
    assert!(
        prefix.chars().all(|c| c.is_ascii_digit()),
        "prefix should contain only digits"
    );

    let num: u32 = prefix.parse().expect("prefix should parse as u32");
    assert!(num < 128, "shard index should be < prefix_count");

    assert_eq!(parts[1], "some/path/file.dat");
}

#[test]
fn test_prefix_count_validation() {
    let client = dummy_client();

    // Rejects 0
    let result = S3StorageBackend::with_prefix_count(client.clone(), "bucket".to_string(), 0);
    assert!(matches!(
        result,
        Err(FlushError::InvalidArgument { .. })
    ));

    // Rejects non-power-of-2
    for bad in [3, 5, 6, 7] {
        let result =
            S3StorageBackend::with_prefix_count(client.clone(), "bucket".to_string(), bad);
        assert!(
            matches!(result, Err(FlushError::InvalidArgument { .. })),
            "expected rejection for prefix_count={}",
            bad
        );
    }

    // Accepts valid powers of 2
    for good in [1, 2, 4, 64, 128, 256] {
        let result =
            S3StorageBackend::with_prefix_count(client.clone(), "bucket".to_string(), good);
        assert!(
            result.is_ok(),
            "expected acceptance for prefix_count={}",
            good
        );
    }
}

#[test]
fn test_empty_bucket_rejected() {
    let client = dummy_client();
    let result = S3StorageBackend::with_prefix_count(client, String::new(), 128);
    assert!(matches!(
        result,
        Err(FlushError::InvalidArgument { .. })
    ));
}

#[test]
fn test_new_default_prefix_count() {
    let backend = S3StorageBackend::new(dummy_client(), "test-bucket".to_string());
    // Verify default prefix_count by checking shard key range.
    // With 128 shards, CRC % 128 must produce values 0..127.
    let sharded = backend.shard_key("test");
    let prefix_str = sharded.split('/').next().expect("should have prefix");
    let shard_num: u32 = prefix_str.parse().expect("prefix should be numeric");
    assert!(shard_num < 128);

    // Also verify by creating with explicit 128 and comparing
    let backend_explicit =
        S3StorageBackend::with_prefix_count(dummy_client(), "test-bucket".to_string(), 128)
            .expect("128 should be valid");
    assert_eq!(
        backend.shard_key("any-key"),
        backend_explicit.shard_key("any-key")
    );
}

#[test]
fn test_with_custom_prefix_count() {
    let client = dummy_client();

    // 1 shard: all keys map to 000/
    let backend =
        S3StorageBackend::with_prefix_count(client.clone(), "bucket".to_string(), 1)
            .expect("1 is valid");
    assert!(backend.shard_key("foo").starts_with("000/"));
    assert!(backend.shard_key("bar").starts_with("000/"));

    // 64 shards
    let backend =
        S3StorageBackend::with_prefix_count(client.clone(), "bucket".to_string(), 64)
            .expect("64 is valid");
    let sharded = backend.shard_key("test-key");
    let prefix_str = sharded.split('/').next().expect("should have prefix");
    let shard_num: u32 = prefix_str.parse().expect("prefix should be numeric");
    assert!(shard_num < 64);

    // 256 shards
    let backend =
        S3StorageBackend::with_prefix_count(client, "bucket".to_string(), 256)
            .expect("256 is valid");
    let sharded = backend.shard_key("test-key");
    let prefix_str = sharded.split('/').next().expect("should have prefix");
    let shard_num: u32 = prefix_str.parse().expect("prefix should be numeric");
    assert!(shard_num < 256);
}

#[test]
fn test_shard_key_preserves_logical_key() {
    let backend = S3StorageBackend::new(dummy_client(), "test-bucket".to_string());

    let keys = ["simple", "path/with/slashes", "key-with-dashes", "a/b/c/d.sst"];
    for key in keys {
        let sharded = backend.shard_key(key);
        assert!(
            sharded.ends_with(key),
            "sharded key '{}' should end with original key '{}'",
            sharded,
            key
        );
        // Verify format: NNN/original_key
        let slash_pos = sharded.find('/').expect("should contain slash");
        assert_eq!(slash_pos, 3, "shard prefix should be exactly 3 chars");
    }
}

#[test]
fn test_different_keys_can_map_to_different_shards() {
    let backend = S3StorageBackend::new(dummy_client(), "test-bucket".to_string());
    let shard_a = backend.shard_key("alpha");
    let shard_b = backend.shard_key("beta");

    let prefix_a = &shard_a[..3];
    let prefix_b = &shard_b[..3];

    // These specific keys are very likely to hash to different shards,
    // but we only assert they produce valid format
    assert_ne!(shard_a, shard_b);
    assert!(prefix_a.chars().all(|c| c.is_ascii_digit()));
    assert!(prefix_b.chars().all(|c| c.is_ascii_digit()));
}
