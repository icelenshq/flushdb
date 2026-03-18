use flushdb_proto::flushdb::v1::Item;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const CATEGORIES: &[&str] = &[
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Toys",
    "Food & Drink",
    "Automotive",
];

const ADJECTIVES: &[&str] = &[
    "Premium",
    "Classic",
    "Ultra",
    "Essential",
    "Pro",
    "Deluxe",
    "Eco",
    "Smart",
    "Compact",
    "Advanced",
    "Vintage",
    "Modern",
    "Elite",
    "Basic",
    "Super",
    "Mega",
];

const NOUNS: &[&str] = &[
    "Widget",
    "Gadget",
    "Gizmo",
    "Device",
    "Tool",
    "Kit",
    "Set",
    "Pack",
    "Bundle",
    "System",
    "Station",
    "Hub",
    "Gear",
    "Unit",
    "Module",
    "Rig",
];

const BRANDS: &[&str] = &[
    "AcmeCo",
    "NovaTech",
    "PeakGear",
    "ZenWorks",
    "BoltCraft",
    "CoreLine",
    "SwiftMade",
    "TrueForm",
];

const COLORS: &[&str] = &[
    "red", "blue", "green", "black", "white", "silver", "gold", "navy",
];

const CLOTHING_SIZES: &[&str] = &["XS", "S", "M", "L", "XL", "XXL"];

const ELECTRONICS_STORAGE: &[&str] = &["64GB", "128GB", "256GB", "512GB", "1TB"];

const WAREHOUSES: &[&str] = &["warehouse-east", "warehouse-west", "warehouse-central"];

pub struct ProductGenerator {
    base_seed: u64,
}

impl ProductGenerator {
    pub fn new(seed: u64) -> Self {
        Self { base_seed: seed }
    }

    pub fn generate_product(&self, product_id: u32) -> Vec<Item> {
        let mut rng = StdRng::seed_from_u64(self.base_seed.wrapping_add(product_id as u64));
        let mut items = Vec::with_capacity(12);

        let category_idx = rng.random_range(0..CATEGORIES.len());
        let category = CATEGORIES[category_idx];
        let adjective = ADJECTIVES[rng.random_range(0..ADJECTIVES.len())];
        let noun = NOUNS[rng.random_range(0..NOUNS.len())];
        let brand = BRANDS[rng.random_range(0..BRANDS.len())];

        let name = format!("{} {} {}", adjective, noun, category_idx);
        let description = format!(
            "A {} {} by {} in the {} category. Product ID: {}.",
            adjective.to_lowercase(),
            noun.to_lowercase(),
            brand,
            category,
            product_id,
        );

        // info item
        let info_json = serde_json::json!({
            "name": name,
            "description": description,
            "category": category,
            "brand": brand,
        });
        items.push(Item {
            key: b"info".to_vec(),
            value: serde_json::to_vec(&info_json).expect("valid json"),
            metadata: Vec::new(),
            chunk: 0,
        });

        // price item
        let current_cents: u32 = rng.random_range(99..99999);
        let markup: u32 = rng.random_range(0..5000);
        let original_cents = current_cents + markup;
        let price_json = serde_json::json!({
            "current_cents": current_cents,
            "original_cents": original_cents,
            "currency": "USD",
        });
        items.push(Item {
            key: b"price".to_vec(),
            value: serde_json::to_vec(&price_json).expect("valid json"),
            metadata: Vec::new(),
            chunk: 0,
        });

        // inventory items (1-3 warehouses)
        let warehouse_count = rng.random_range(1..=WAREHOUSES.len());
        for wh in &WAREHOUSES[..warehouse_count] {
            let inv_json = serde_json::json!({
                "count": rng.random_range(0u32..500),
                "reserved": rng.random_range(0u32..50),
            });
            let key = format!("inventory:{}", wh);
            items.push(Item {
                key: key.into_bytes(),
                value: serde_json::to_vec(&inv_json).expect("valid json"),
                metadata: Vec::new(),
                chunk: 0,
            });
        }

        // variant items (category-aware, may produce duplicate keys)
        let variant_count = rng.random_range(2..=6);
        for _ in 0..variant_count {
            let color = COLORS[rng.random_range(0..COLORS.len())];
            let size_part = match category {
                "Clothing" | "Sports" => {
                    CLOTHING_SIZES[rng.random_range(0..CLOTHING_SIZES.len())].to_string()
                }
                "Electronics" => {
                    ELECTRONICS_STORAGE[rng.random_range(0..ELECTRONICS_STORAGE.len())].to_string()
                }
                _ => format!("v{}", rng.random_range(1u32..10)),
            };
            let key = format!("variant:{}:{}", color, size_part);
            let extra_cents: u32 = rng.random_range(0..2000);
            let in_stock: bool = rng.random_bool(0.7);
            let variant_json = serde_json::json!({
                "sku_suffix": format!("{}-{}", &color[..2].to_uppercase(), size_part),
                "extra_cents": extra_cents,
                "in_stock": in_stock,
            });
            items.push(Item {
                key: key.into_bytes(),
                value: serde_json::to_vec(&variant_json).expect("valid json"),
                metadata: Vec::new(),
                chunk: 0,
            });
        }

        // review items (1-5 reviews, skewed toward higher ratings)
        let review_count = rng.random_range(1..=5);
        for r in 0..review_count {
            let ts = 1_710_000_000_000u64 + (product_id as u64) * 1000 + r as u64;
            let rating: u32 = {
                let roll: f64 = rng.random();
                if roll < 0.05 {
                    1
                } else if roll < 0.10 {
                    2
                } else if roll < 0.25 {
                    3
                } else if roll < 0.55 {
                    4
                } else {
                    5
                }
            };
            let user_id = rng.random_range(1u32..10000);
            let review_json = serde_json::json!({
                "rating": rating,
                "author": format!("user:{}", user_id),
                "title": format!("Review {} for product {}", r + 1, product_id),
                "body": format!("This is review #{} with rating {}.", r + 1, rating),
            });
            let key = format!("review:{}", ts);
            items.push(Item {
                key: key.into_bytes(),
                value: serde_json::to_vec(&review_json).expect("valid json"),
                metadata: Vec::new(),
                chunk: 0,
            });
        }

        items
    }

    pub fn generate_update_items(rng: &mut impl Rng) -> Vec<Item> {
        let mut items = Vec::with_capacity(4);

        let current_cents: u32 = rng.random_range(99..99999);
        let markup: u32 = rng.random_range(0..5000);
        let price_json = serde_json::json!({
            "current_cents": current_cents,
            "original_cents": current_cents + markup,
            "currency": "USD",
        });
        items.push(Item {
            key: b"price".to_vec(),
            value: serde_json::to_vec(&price_json).expect("valid json"),
            metadata: Vec::new(),
            chunk: 0,
        });

        let warehouse_count = rng.random_range(1..=WAREHOUSES.len());
        for wh in &WAREHOUSES[..warehouse_count] {
            let inv_json = serde_json::json!({
                "count": rng.random_range(0u32..500),
                "reserved": rng.random_range(0u32..50),
            });
            items.push(Item {
                key: format!("inventory:{}", wh).into_bytes(),
                value: serde_json::to_vec(&inv_json).expect("valid json"),
                metadata: Vec::new(),
                chunk: 0,
            });
        }

        items
    }

    pub fn record_id(product_id: u32) -> String {
        format!("product:{:06}", product_id)
    }

    #[allow(dead_code)]
    pub fn expected_item_prefixes() -> &'static [&'static str] {
        &["info", "price", "inventory:", "variant:"]
    }
}
