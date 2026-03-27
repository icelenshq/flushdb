use flushdb_demo::data_gen::ProductGenerator;
use rand::rngs::StdRng;
use rand::SeedableRng;

#[test]
fn test_generate_product_deterministic() {
    let gen = ProductGenerator::new(42);
    let items_a = gen.generate_product(0);
    let items_b = gen.generate_product(0);
    assert_eq!(items_a.len(), items_b.len());
    for (a, b) in items_a.iter().zip(items_b.iter()) {
        assert_eq!(a.key, b.key);
        assert_eq!(a.value, b.value);
    }
}

#[test]
fn test_generate_product_different_ids_differ() {
    let gen = ProductGenerator::new(42);
    let items_a = gen.generate_product(0);
    let items_b = gen.generate_product(1);
    let val_a = &items_a[0].value;
    let val_b = &items_b[0].value;
    assert_ne!(val_a, val_b);
}

#[test]
fn test_generate_product_has_required_keys() {
    let gen = ProductGenerator::new(42);
    for product_id in [0, 1, 100, 999_999] {
        let items = gen.generate_product(product_id);
        let keys: Vec<String> = items
            .iter()
            .filter_map(|i| String::from_utf8(i.key.clone()).ok())
            .collect();

        assert!(
            keys.contains(&"info".to_string()),
            "product {} missing info key",
            product_id
        );
        assert!(
            keys.contains(&"price".to_string()),
            "product {} missing price key",
            product_id
        );
        assert!(
            keys.iter().any(|k| k.starts_with("inventory:")),
            "product {} missing inventory key",
            product_id
        );
        assert!(
            keys.iter().any(|k| k.starts_with("variant:")),
            "product {} missing variant key",
            product_id
        );
    }
}

#[test]
fn test_generate_product_item_count_range() {
    let gen = ProductGenerator::new(42);
    for product_id in 0..100 {
        let items = gen.generate_product(product_id);
        assert!(
            items.len() >= 6,
            "product {} has too few items: {}",
            product_id,
            items.len()
        );
        assert!(
            items.len() <= 20,
            "product {} has too many items: {}",
            product_id,
            items.len()
        );
    }
}

#[test]
fn test_record_id_format() {
    assert_eq!(ProductGenerator::record_id(0), "product:000000");
    assert_eq!(ProductGenerator::record_id(1), "product:000001");
    assert_eq!(ProductGenerator::record_id(999999), "product:999999");
}

#[test]
fn test_info_value_is_valid_json() {
    let gen = ProductGenerator::new(42);
    let items = gen.generate_product(42);
    let info_item = items.iter().find(|i| i.key == b"info").expect("has info");
    let parsed: serde_json::Value = serde_json::from_slice(&info_item.value).expect("valid json");
    assert!(parsed.get("name").is_some());
    assert!(parsed.get("description").is_some());
    assert!(parsed.get("category").is_some());
    assert!(parsed.get("brand").is_some());
}

#[test]
fn test_price_value_is_valid_json() {
    let gen = ProductGenerator::new(42);
    let items = gen.generate_product(42);
    let price_item = items.iter().find(|i| i.key == b"price").expect("has price");
    let parsed: serde_json::Value = serde_json::from_slice(&price_item.value).expect("valid json");
    assert!(parsed.get("current_cents").is_some());
    assert!(parsed.get("original_cents").is_some());
    assert!(parsed.get("currency").is_some());
    let current = parsed["current_cents"].as_u64().expect("u64");
    let original = parsed["original_cents"].as_u64().expect("u64");
    assert!(original >= current);
}

#[test]
fn test_generate_update_items_has_price_and_inventory() {
    let mut rng = StdRng::seed_from_u64(99);
    let items = ProductGenerator::generate_update_items(&mut rng);
    let keys: Vec<String> = items
        .iter()
        .filter_map(|i| String::from_utf8(i.key.clone()).ok())
        .collect();
    assert!(keys.contains(&"price".to_string()), "missing price key");
    assert!(
        keys.iter().any(|k| k.starts_with("inventory:")),
        "missing inventory key"
    );
    assert!(items.len() >= 2 && items.len() <= 4);
}

#[test]
fn test_generate_update_items_valid_json() {
    let mut rng = StdRng::seed_from_u64(123);
    let items = ProductGenerator::generate_update_items(&mut rng);
    let price_item = items.iter().find(|i| i.key == b"price").expect("has price");
    let parsed: serde_json::Value = serde_json::from_slice(&price_item.value).expect("valid json");
    assert!(parsed.get("current_cents").is_some());
    assert!(parsed.get("original_cents").is_some());
    let current = parsed["current_cents"].as_u64().expect("u64");
    let original = parsed["original_cents"].as_u64().expect("u64");
    assert!(original >= current);

    for item in items.iter().filter(|i| i.key.starts_with(b"inventory:")) {
        let inv: serde_json::Value = serde_json::from_slice(&item.value).expect("valid json");
        assert!(inv.get("count").is_some());
        assert!(inv.get("reserved").is_some());
    }
}
