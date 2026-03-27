use std::collections::HashMap;

use crate::client::DemoClient;
use crate::data_gen::ProductGenerator;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub struct VerifyConfig {
    pub server_addr: String,
    pub namespace: String,
    pub sample_size: u32,
    pub product_range: u32,
}

struct CheckResult {
    name: String,
    passed: bool,
    detail: Option<String>,
}

impl CheckResult {
    fn pass(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: true,
            detail: None,
        }
    }

    fn fail(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            passed: false,
            detail: Some(detail.into()),
        }
    }
}

pub async fn run_verify(config: VerifyConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DemoClient::connect(&config.server_addr)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })?;

    let mut all_results: Vec<CheckResult> = Vec::new();

    // Phase 1: Seeded data checks — validates products written by `seed` command
    println!(
        "=== Phase 1: Seeded data checks ({} samples from [0, {})) ===",
        config.sample_size, config.product_range,
    );
    let seeded = check_seeded_data(
        &mut client,
        &config.namespace,
        config.sample_size,
        config.product_range,
    )
    .await;
    all_results.extend(seeded);

    // Phase 2: Round-trip correctness checks — writes its own isolated data
    println!();
    println!("=== Phase 2: Round-trip correctness checks ===");
    let roundtrip = check_roundtrip(&mut client, &config.namespace).await;
    all_results.extend(roundtrip);

    // Summary
    let passed = all_results.iter().filter(|r| r.passed).count();
    let failed = all_results.iter().filter(|r| !r.passed).count();

    println!();
    println!("=== Results ===");
    for r in &all_results {
        let status = if r.passed { "PASS" } else { "FAIL" };
        match &r.detail {
            Some(detail) => println!("  [{}] {} — {}", status, r.name, detail),
            None => println!("  [{}] {}", status, r.name),
        }
    }
    println!();
    println!(
        "Total: {} passed, {} failed out of {}",
        passed,
        failed,
        all_results.len(),
    );

    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 1: Seeded data checks
// ---------------------------------------------------------------------------

async fn check_seeded_data(
    client: &mut DemoClient,
    namespace: &str,
    sample_size: u32,
    product_range: u32,
) -> Vec<CheckResult> {
    let gen = ProductGenerator::new(42);
    let mut rng = StdRng::seed_from_u64(9999);
    let mut results = Vec::new();

    let mut presence_ok = 0u32;
    let mut presence_fail = 0u32;
    let mut integrity_ok = 0u32;
    let mut integrity_fail = 0u32;
    let mut count_ok = 0u32;
    let mut count_fail = 0u32;
    let mut schema_ok = 0u32;
    let mut schema_fail = 0u32;
    let mut first_failures: Vec<String> = Vec::new();

    for i in 0..sample_size {
        let product_id = rng.random_range(0..product_range);
        let record_id = ProductGenerator::record_id(product_id);
        let raw_items = gen.generate_product(product_id);
        // Apply last-write-wins deduplication: the engine keeps only the latest
        // value for each key within a PutItemsRequest, so our expectations must
        // match.
        let expected_items = dedup_last_write_wins(raw_items);

        let resp = match client.get_product_all(namespace, &record_id).await {
            Ok(r) => r,
            Err(e) => {
                presence_fail += 1;
                integrity_fail += 1;
                count_fail += 1;
                schema_fail += 1;
                if first_failures.len() < 5 {
                    first_failures.push(format!(
                        "product {} ({}): get failed: {}",
                        product_id, record_id, e
                    ));
                }
                continue;
            }
        };

        let actual: HashMap<Vec<u8>, Vec<u8>> = resp
            .items
            .iter()
            .map(|item| (item.key.clone(), item.value.clone()))
            .collect();

        // 1. Presence check
        let keys: Vec<String> = actual
            .keys()
            .filter_map(|k| String::from_utf8(k.clone()).ok())
            .collect();
        let has_info = keys.iter().any(|k| k == "info");
        let has_price = keys.iter().any(|k| k == "price");
        let has_inventory = keys.iter().any(|k| k.starts_with("inventory:"));
        let has_variant = keys.iter().any(|k| k.starts_with("variant:"));
        if has_info && has_price && has_inventory && has_variant {
            presence_ok += 1;
        } else {
            presence_fail += 1;
            if first_failures.len() < 5 {
                first_failures.push(format!(
                    "product {} presence: missing keys. got: {:?}",
                    product_id, keys
                ));
            }
        }

        // 2. Item count check
        if actual.len() == expected_items.len() {
            count_ok += 1;
        } else {
            count_fail += 1;
            if first_failures.len() < 5 {
                first_failures.push(format!(
                    "product {} count: expected {} items, got {}",
                    product_id,
                    expected_items.len(),
                    actual.len()
                ));
            }
        }

        // 3. Value integrity check — byte-for-byte comparison
        let mut this_integrity_ok = true;
        for expected in &expected_items {
            match actual.get(&expected.key) {
                Some(actual_value) => {
                    if actual_value != &expected.value {
                        this_integrity_ok = false;
                        if first_failures.len() < 5 {
                            let key_str = String::from_utf8(expected.key.clone())
                                .unwrap_or_else(|_| format!("<{} bytes>", expected.key.len()));
                            first_failures.push(format!(
                                "product {} key '{}': value mismatch (expected {} bytes, got {} bytes)",
                                product_id,
                                key_str,
                                expected.value.len(),
                                actual_value.len()
                            ));
                        }
                        break;
                    }
                }
                None => {
                    this_integrity_ok = false;
                    if first_failures.len() < 5 {
                        let key_str = String::from_utf8(expected.key.clone())
                            .unwrap_or_else(|_| format!("<{} bytes>", expected.key.len()));
                        first_failures.push(format!(
                            "product {} key '{}': missing from response",
                            product_id, key_str
                        ));
                    }
                    break;
                }
            }
        }
        if this_integrity_ok {
            integrity_ok += 1;
        } else {
            integrity_fail += 1;
        }

        // 4. JSON schema validation
        let schema_valid = validate_json_schema(&actual);
        if schema_valid {
            schema_ok += 1;
        } else {
            schema_fail += 1;
            if first_failures.len() < 5 {
                first_failures.push(format!(
                    "product {} schema: invalid JSON fields",
                    product_id
                ));
            }
        }

        if (i + 1) % 25 == 0 {
            println!("  Checked {}/{} products...", i + 1, sample_size);
        }
    }

    results.push(if presence_fail == 0 {
        CheckResult::pass(format!(
            "presence: {}/{} products have required keys",
            presence_ok, sample_size
        ))
    } else {
        CheckResult::fail(
            format!("presence: {}/{} passed", presence_ok, sample_size),
            format!("{} failed", presence_fail),
        )
    });

    results.push(if count_fail == 0 {
        CheckResult::pass(format!(
            "item-count: {}/{} products match expected count",
            count_ok, sample_size
        ))
    } else {
        CheckResult::fail(
            format!("item-count: {}/{} passed", count_ok, sample_size),
            format!("{} failed", count_fail),
        )
    });

    results.push(if integrity_fail == 0 {
        CheckResult::pass(format!(
            "value-integrity: {}/{} products match byte-for-byte",
            integrity_ok, sample_size
        ))
    } else {
        CheckResult::fail(
            format!("value-integrity: {}/{} passed", integrity_ok, sample_size),
            format!("{} failed", integrity_fail),
        )
    });

    results.push(if schema_fail == 0 {
        CheckResult::pass(format!(
            "json-schema: {}/{} products have valid JSON fields",
            schema_ok, sample_size
        ))
    } else {
        CheckResult::fail(
            format!("json-schema: {}/{} passed", schema_ok, sample_size),
            format!("{} failed", schema_fail),
        )
    });

    if !first_failures.is_empty() {
        println!("  First failures:");
        for f in &first_failures {
            println!("    - {}", f);
        }
    }

    results
}

/// Deduplicate items by key, keeping the last occurrence (last-write-wins).
fn dedup_last_write_wins(
    items: Vec<flushdb_proto::flushdb::v1::Item>,
) -> Vec<flushdb_proto::flushdb::v1::Item> {
    let mut seen = HashMap::new();
    for (i, item) in items.into_iter().enumerate() {
        seen.insert(item.key.clone(), (i, item));
    }
    let mut deduped: Vec<_> = seen.into_values().collect();
    deduped.sort_by_key(|(i, _)| *i);
    deduped.into_iter().map(|(_, item)| item).collect()
}

fn validate_json_schema(items: &HashMap<Vec<u8>, Vec<u8>>) -> bool {
    // Validate info key
    if let Some(val) = items.get(b"info".as_slice()) {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(val) {
            if !v.get("name").is_some_and(|n| n.is_string())
                || !v.get("description").is_some_and(|d| d.is_string())
                || !v.get("category").is_some_and(|c| c.is_string())
                || !v.get("brand").is_some_and(|b| b.is_string())
            {
                return false;
            }
        } else {
            return false;
        }
    }

    // Validate price key
    if let Some(val) = items.get(b"price".as_slice()) {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(val) {
            if !v.get("current_cents").is_some_and(|c| c.is_u64())
                || !v.get("original_cents").is_some_and(|o| o.is_u64())
                || !v.get("currency").is_some_and(|c| c.is_string())
            {
                return false;
            }
            // original_cents >= current_cents
            if let (Some(curr), Some(orig)) =
                (v["current_cents"].as_u64(), v["original_cents"].as_u64())
            {
                if orig < curr {
                    return false;
                }
            }
        } else {
            return false;
        }
    }

    // Validate inventory keys
    for (key, val) in items {
        let key_str = match std::str::from_utf8(key) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if key_str.starts_with("inventory:") {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(val) {
                if !v.get("count").is_some_and(|c| c.is_u64())
                    || !v.get("reserved").is_some_and(|r| r.is_u64())
                {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Validate variant keys
        if key_str.starts_with("variant:") {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(val) {
                if !v.get("sku_suffix").is_some_and(|s| s.is_string())
                    || !v.get("extra_cents").is_some_and(|e| e.is_u64())
                    || !v.get("in_stock").is_some_and(|i| i.is_boolean())
                {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Validate review keys
        if key_str.starts_with("review:") {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(val) {
                if !v.get("rating").is_some_and(|r| r.is_u64())
                    || !v.get("author").is_some_and(|a| a.is_string())
                    || !v.get("title").is_some_and(|t| t.is_string())
                    || !v.get("body").is_some_and(|b| b.is_string())
                {
                    return false;
                }
                if let Some(rating) = v["rating"].as_u64() {
                    if !(1..=5).contains(&rating) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }
    }

    true
}

// ---------------------------------------------------------------------------
// Phase 2: Round-trip correctness checks
// ---------------------------------------------------------------------------

async fn check_roundtrip(client: &mut DemoClient, namespace: &str) -> Vec<CheckResult> {
    let mut results = Vec::new();

    results.push(check_put_get_roundtrip(client, namespace).await);
    results.push(check_delete_correctness(client, namespace).await);
    results.push(check_scan_correctness(client, namespace).await);
    results.push(check_selective_get(client, namespace).await);
    results.push(check_overwrite(client, namespace).await);

    results
}

async fn check_put_get_roundtrip(client: &mut DemoClient, namespace: &str) -> CheckResult {
    let record_id = "__verify:roundtrip";
    let gen = ProductGenerator::new(42);
    let expected_items = gen.generate_product(7777);

    if let Err(e) = client
        .put_product(namespace, record_id, expected_items.clone())
        .await
    {
        return CheckResult::fail("put-get-roundtrip", format!("put failed: {}", e));
    }

    let resp = match client.get_product_all(namespace, record_id).await {
        Ok(r) => r,
        Err(e) => {
            return CheckResult::fail("put-get-roundtrip", format!("get failed: {}", e));
        }
    };

    let actual: HashMap<Vec<u8>, Vec<u8>> = resp
        .items
        .iter()
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect();

    if actual.len() != expected_items.len() {
        return CheckResult::fail(
            "put-get-roundtrip",
            format!(
                "item count mismatch: expected {}, got {}",
                expected_items.len(),
                actual.len()
            ),
        );
    }

    for expected in &expected_items {
        match actual.get(&expected.key) {
            Some(actual_value) if actual_value == &expected.value => {}
            Some(actual_value) => {
                let key_str = String::from_utf8_lossy(&expected.key);
                return CheckResult::fail(
                    "put-get-roundtrip",
                    format!(
                        "key '{}': value mismatch ({} vs {} bytes)",
                        key_str,
                        expected.value.len(),
                        actual_value.len()
                    ),
                );
            }
            None => {
                let key_str = String::from_utf8_lossy(&expected.key);
                return CheckResult::fail(
                    "put-get-roundtrip",
                    format!("key '{}' missing from response", key_str),
                );
            }
        }
    }

    // cleanup
    let _ = client.delete_product(namespace, record_id).await;

    CheckResult::pass("put-get-roundtrip: write then read returns exact data")
}

async fn check_delete_correctness(client: &mut DemoClient, namespace: &str) -> CheckResult {
    let record_id = "__verify:delete";
    let gen = ProductGenerator::new(42);
    let items = gen.generate_product(8888);

    if let Err(e) = client.put_product(namespace, record_id, items).await {
        return CheckResult::fail("delete-correctness", format!("put failed: {}", e));
    }

    // Verify it's there
    match client.get_product_all(namespace, record_id).await {
        Ok(resp) if resp.items.is_empty() => {
            return CheckResult::fail(
                "delete-correctness",
                "put succeeded but get returned empty before delete",
            );
        }
        Err(e) => {
            return CheckResult::fail(
                "delete-correctness",
                format!("get before delete failed: {}", e),
            );
        }
        _ => {}
    }

    if let Err(e) = client.delete_product(namespace, record_id).await {
        return CheckResult::fail("delete-correctness", format!("delete failed: {}", e));
    }

    match client.get_product_all(namespace, record_id).await {
        Ok(resp) => {
            if resp.items.is_empty() {
                CheckResult::pass("delete-correctness: delete removes all items")
            } else {
                CheckResult::fail(
                    "delete-correctness",
                    format!("{} items still present after delete", resp.items.len()),
                )
            }
        }
        Err(e) => CheckResult::fail(
            "delete-correctness",
            format!("get after delete failed: {}", e),
        ),
    }
}

async fn check_scan_correctness(client: &mut DemoClient, namespace: &str) -> CheckResult {
    let record_id = "__verify:scan";
    let gen = ProductGenerator::new(42);
    let items = gen.generate_product(9999);

    let expected_variants: Vec<(Vec<u8>, Vec<u8>)> = items
        .iter()
        .filter(|item| {
            std::str::from_utf8(&item.key)
                .map(|k| k.starts_with("variant:"))
                .unwrap_or(false)
        })
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect();

    if let Err(e) = client.put_product(namespace, record_id, items).await {
        return CheckResult::fail("scan-correctness", format!("put failed: {}", e));
    }

    let scan_results = match client
        .scan_product_range(
            namespace,
            record_id,
            b"variant:".to_vec(),
            b"variant;\x00".to_vec(), // ';' is one past ':'
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let _ = client.delete_product(namespace, record_id).await;
            return CheckResult::fail("scan-correctness", format!("scan failed: {}", e));
        }
    };

    let scanned: HashMap<Vec<u8>, Vec<u8>> = scan_results
        .iter()
        .flat_map(|resp| resp.items.iter())
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect();

    let _ = client.delete_product(namespace, record_id).await;

    if scanned.len() != expected_variants.len() {
        return CheckResult::fail(
            "scan-correctness",
            format!(
                "expected {} variants, scan returned {}",
                expected_variants.len(),
                scanned.len()
            ),
        );
    }

    for (key, value) in &expected_variants {
        match scanned.get(key) {
            Some(actual_value) if actual_value == value => {}
            Some(_) => {
                let key_str = String::from_utf8_lossy(key);
                return CheckResult::fail(
                    "scan-correctness",
                    format!("variant '{}': value mismatch", key_str),
                );
            }
            None => {
                let key_str = String::from_utf8_lossy(key);
                return CheckResult::fail(
                    "scan-correctness",
                    format!("variant '{}' missing from scan", key_str),
                );
            }
        }
    }

    CheckResult::pass(format!(
        "scan-correctness: variant:* scan returns all {} variants",
        expected_variants.len()
    ))
}

async fn check_selective_get(client: &mut DemoClient, namespace: &str) -> CheckResult {
    let record_id = "__verify:selective";
    let gen = ProductGenerator::new(42);
    let items = gen.generate_product(6666);

    let expected_info_value = items
        .iter()
        .find(|i| i.key == b"info")
        .expect("generator always produces info")
        .value
        .clone();
    let expected_price_value = items
        .iter()
        .find(|i| i.key == b"price")
        .expect("generator always produces price")
        .value
        .clone();

    if let Err(e) = client.put_product(namespace, record_id, items).await {
        return CheckResult::fail("selective-get", format!("put failed: {}", e));
    }

    let resp = match client
        .get_product_keys(
            namespace,
            record_id,
            vec![b"info".to_vec(), b"price".to_vec()],
        )
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let _ = client.delete_product(namespace, record_id).await;
            return CheckResult::fail("selective-get", format!("get_product_keys failed: {}", e));
        }
    };

    let _ = client.delete_product(namespace, record_id).await;

    let actual: HashMap<Vec<u8>, Vec<u8>> = resp
        .items
        .iter()
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect();

    if actual.len() != 2 {
        return CheckResult::fail(
            "selective-get",
            format!("requested 2 keys (info, price), got {} items", actual.len()),
        );
    }

    if actual.get(b"info".as_slice()) != Some(&expected_info_value) {
        return CheckResult::fail("selective-get", "info value mismatch");
    }
    if actual.get(b"price".as_slice()) != Some(&expected_price_value) {
        return CheckResult::fail("selective-get", "price value mismatch");
    }

    CheckResult::pass("selective-get: MatchKeys returns only requested keys with correct values")
}

async fn check_overwrite(client: &mut DemoClient, namespace: &str) -> CheckResult {
    let record_id = "__verify:overwrite";
    let gen = ProductGenerator::new(42);

    // Write product 1111
    let items_v1 = gen.generate_product(1111);
    if let Err(e) = client.put_product(namespace, record_id, items_v1).await {
        return CheckResult::fail("overwrite", format!("first put failed: {}", e));
    }

    // Overwrite with product 2222 (different data, same record_id)
    let items_v2 = gen.generate_product(2222);
    if let Err(e) = client
        .put_product(namespace, record_id, items_v2.clone())
        .await
    {
        return CheckResult::fail("overwrite", format!("second put failed: {}", e));
    }

    let resp = match client.get_product_all(namespace, record_id).await {
        Ok(r) => r,
        Err(e) => {
            let _ = client.delete_product(namespace, record_id).await;
            return CheckResult::fail("overwrite", format!("get after overwrite failed: {}", e));
        }
    };

    let _ = client.delete_product(namespace, record_id).await;

    let actual: HashMap<Vec<u8>, Vec<u8>> = resp
        .items
        .iter()
        .map(|item| (item.key.clone(), item.value.clone()))
        .collect();

    // Check that v2's info and price are present with correct values
    let v2_info = items_v2
        .iter()
        .find(|i| i.key == b"info")
        .expect("generator always produces info");
    let v2_price = items_v2
        .iter()
        .find(|i| i.key == b"price")
        .expect("generator always produces price");

    match actual.get(b"info".as_slice()) {
        Some(val) if val == &v2_info.value => {}
        Some(_) => {
            return CheckResult::fail("overwrite", "info not updated to v2 value");
        }
        None => {
            return CheckResult::fail("overwrite", "info key missing after overwrite");
        }
    }
    match actual.get(b"price".as_slice()) {
        Some(val) if val == &v2_price.value => {}
        Some(_) => {
            return CheckResult::fail("overwrite", "price not updated to v2 value");
        }
        None => {
            return CheckResult::fail("overwrite", "price key missing after overwrite");
        }
    }

    CheckResult::pass("overwrite: second put overwrites first put's values")
}
