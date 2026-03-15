use std::collections::BTreeMap;

use flushdb_engine::{
    CompactionConfig, CompactionScheduler, CompactionType, Level, Manifest, SSTableMeta,
    WriteStallStatus,
};

fn make_sst_meta(
    id: &str,
    min_key: &[u8],
    max_key: &[u8],
    size_bytes: u64,
    created_at_ms: u64,
) -> SSTableMeta {
    SSTableMeta {
        id: id.to_string(),
        size_bytes,
        entry_count: 100,
        min_key: min_key.to_vec(),
        max_key: max_key.to_vec(),
        bloom_filter_offset: 0,
        bloom_filter_size: 0,
        index_offset: 0,
        index_size: 0,
        created_at_ms,
        sequence_range: (1, 100),
        record_id_count: 50,
        run_id: None,
        fragment_index: None,
        dedup_block_size: 0,
    }
}

fn make_manifest_with_levels(
    l0: Vec<SSTableMeta>,
    l1: Vec<SSTableMeta>,
    l2: Vec<SSTableMeta>,
    l3: Vec<SSTableMeta>,
) -> Manifest {
    let mut levels = BTreeMap::new();
    levels.insert(Level::L0, l0);
    levels.insert(Level::L1, l1);
    levels.insert(Level::L2, l2);
    levels.insert(Level::L3, l3);

    Manifest {
        format_version: 1,
        manifest_id: flushdb_engine::ManifestId::new(1),
        writer_epoch: 1,
        compactor_epoch: 0,
        namespace: "test".to_string(),
        created_at_ms: 1000,
        last_flushed_sequence: 0,
        levels,
        blob_files: Vec::new(),
        tombstone_compaction_watermarks: BTreeMap::new(),
        previous_manifest_id: flushdb_engine::ManifestId::ZERO,
        is_snapshot: false,
    }
}

// ---- L0 Trigger Tests ----

#[test]
fn test_l0_trigger_fires_at_threshold() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l0 = (0..5)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                format!("a{i:02}").as_bytes(),
                format!("z{i:02}").as_bytes(),
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let task = scheduler.check_l0_trigger(&manifest);
    assert!(task.is_some(), "L0 trigger should fire with 5 SSTables (threshold=4)");
    let task = task.unwrap();
    assert_eq!(task.task_type, CompactionType::L0ToL1);
    assert_eq!(task.source_level, Level::L0);
    assert_eq!(task.target_level, Level::L1);
}

#[test]
fn test_l0_trigger_not_fired_below_threshold() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l0 = (0..3)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                format!("a{i:02}").as_bytes(),
                format!("z{i:02}").as_bytes(),
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let task = scheduler.check_l0_trigger(&manifest);
    assert!(task.is_none(), "L0 trigger should not fire with 3 SSTables");
}

#[test]
fn test_l0_trigger_not_fired_at_exactly_threshold() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l0 = (0..4)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                format!("a{i:02}").as_bytes(),
                format!("z{i:02}").as_bytes(),
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let task = scheduler.check_l0_trigger(&manifest);
    assert!(
        task.is_none(),
        "L0 trigger should not fire at exactly threshold (4), only above"
    );
}

#[test]
fn test_l0_trigger_includes_all_l0() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l0: Vec<SSTableMeta> = (0..6)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                format!("a{i:02}").as_bytes(),
                format!("z{i:02}").as_bytes(),
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0.clone(), vec![], vec![], vec![]);
    let task = scheduler.check_l0_trigger(&manifest).unwrap();

    assert_eq!(
        task.input_sstables.len(),
        6,
        "all L0 SSTables should be included in the compaction input"
    );
    let input_ids: Vec<&str> = task.input_sstables.iter().map(|m| m.id.as_str()).collect();
    for i in 0..6 {
        let expected_id = format!("l0_{i}");
        assert!(
            input_ids.contains(&expected_id.as_str()),
            "input should contain {expected_id}"
        );
    }
}

#[test]
fn test_l0_trigger_finds_overlapping_l1() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());

    // L0 SSTables covering key range [b00, d00]
    let l0 = (0..5)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"b00",
                b"d00",
                1024,
                1000 + i,
            )
        })
        .collect();

    // L1 SSTables with non-overlapping ranges sorted by min_key
    let l1 = vec![
        make_sst_meta("l1_0", b"a00", b"a99", 1024, 500), // no overlap
        make_sst_meta("l1_1", b"b50", b"c50", 1024, 501), // overlaps
        make_sst_meta("l1_2", b"c80", b"d50", 1024, 502), // overlaps
        make_sst_meta("l1_3", b"e00", b"f00", 1024, 503), // no overlap
        make_sst_meta("l1_4", b"g00", b"h00", 1024, 504), // no overlap
    ];

    let manifest = make_manifest_with_levels(l0, l1, vec![], vec![]);
    let task = scheduler.check_l0_trigger(&manifest).unwrap();

    let target_ids: Vec<&str> = task.target_sstables.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(target_ids.len(), 2, "should find exactly 2 overlapping L1 SSTables");
    assert!(target_ids.contains(&"l1_1"));
    assert!(target_ids.contains(&"l1_2"));
}

#[test]
fn test_l0_trigger_no_l1_overlap() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());

    let l0 = (0..5)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"b00",
                1024,
                1000 + i,
            )
        })
        .collect();

    // L1 SSTables with ranges entirely outside L0's [a00, b00]
    let l1 = vec![
        make_sst_meta("l1_0", b"c00", b"d00", 1024, 500),
        make_sst_meta("l1_1", b"e00", b"f00", 1024, 501),
    ];

    let manifest = make_manifest_with_levels(l0, l1, vec![], vec![]);
    let task = scheduler.check_l0_trigger(&manifest).unwrap();
    assert!(
        task.target_sstables.is_empty(),
        "no L1 SSTables should overlap"
    );
}

// ---- Level Trigger Tests ----

#[test]
fn test_l1_trigger_fires_when_oversize() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l1_max = 256 * 1024 * 1024;

    // Each SSTable is l1_max / 2 + 1 byte, so two of them exceed the limit
    let half_plus = l1_max / 2 + 1;
    let l1 = vec![
        make_sst_meta("l1_0", b"a00", b"m00", half_plus, 100),
        make_sst_meta("l1_1", b"n00", b"z00", half_plus, 200),
    ];

    let manifest = make_manifest_with_levels(vec![], l1, vec![], vec![]);
    let task = scheduler.check_level_trigger(&manifest, Level::L1);
    assert!(task.is_some(), "L1 trigger should fire when total size > max");
    let task = task.unwrap();
    assert_eq!(task.task_type, CompactionType::LevelToLevel);
    assert_eq!(task.source_level, Level::L1);
    assert_eq!(task.target_level, Level::L2);
}

#[test]
fn test_l1_trigger_not_fired_below_max() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l1_max = 256 * 1024 * 1024;

    let l1 = vec![make_sst_meta("l1_0", b"a00", b"z00", l1_max - 1, 100)];

    let manifest = make_manifest_with_levels(vec![], l1, vec![], vec![]);
    let task = scheduler.check_level_trigger(&manifest, Level::L1);
    assert!(task.is_none(), "L1 trigger should not fire when under max");
}

#[test]
fn test_l1_trigger_not_fired_at_exactly_max() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l1_max = 256 * 1024 * 1024;

    let l1 = vec![make_sst_meta("l1_0", b"a00", b"z00", l1_max, 100)];

    let manifest = make_manifest_with_levels(vec![], l1, vec![], vec![]);
    let task = scheduler.check_level_trigger(&manifest, Level::L1);
    assert!(
        task.is_none(),
        "L1 trigger should not fire at exactly max (only above)"
    );
}

#[test]
fn test_l2_trigger_fires_when_oversize() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l2_max: u64 = 2_560 * 1024 * 1024;

    let half_plus = l2_max / 2 + 1;
    let l2 = vec![
        make_sst_meta("l2_0", b"a00", b"m00", half_plus, 100),
        make_sst_meta("l2_1", b"n00", b"z00", half_plus, 200),
    ];

    let manifest = make_manifest_with_levels(vec![], vec![], l2, vec![]);
    let task = scheduler.check_level_trigger(&manifest, Level::L2);
    assert!(task.is_some(), "L2 trigger should fire when total size > max");
    let task = task.unwrap();
    assert_eq!(task.task_type, CompactionType::LevelToLevel);
    assert_eq!(task.source_level, Level::L2);
    assert_eq!(task.target_level, Level::L3);
}

#[test]
fn test_level_trigger_picks_oldest_input() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l1_max = 256 * 1024 * 1024;

    let l1 = vec![
        make_sst_meta("l1_newer", b"a00", b"m00", l1_max / 2 + 1, 300),
        make_sst_meta("l1_oldest", b"n00", b"z00", l1_max / 2 + 1, 100),
        make_sst_meta("l1_middle", b"m01", b"n00", l1_max / 2, 200),
    ];

    let manifest = make_manifest_with_levels(vec![], l1, vec![], vec![]);
    let task = scheduler.check_level_trigger(&manifest, Level::L1).unwrap();

    assert_eq!(
        task.input_sstables.len(),
        1,
        "level trigger should pick exactly one SSTable"
    );
    assert_eq!(
        task.input_sstables[0].id, "l1_oldest",
        "level trigger should pick the SSTable with the earliest created_at_ms"
    );
}

#[test]
fn test_level_trigger_finds_overlapping_targets() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l1_max = 256 * 1024 * 1024;

    // The oldest L1 SSTable covers [a00, m00]
    let l1 = vec![
        make_sst_meta("l1_0", b"a00", b"m00", l1_max / 2 + 1, 100),
        make_sst_meta("l1_1", b"n00", b"z00", l1_max / 2 + 1, 200),
    ];

    let l2 = vec![
        make_sst_meta("l2_0", b"a00", b"d00", 1024, 50),  // overlaps with l1_0
        make_sst_meta("l2_1", b"e00", b"g00", 1024, 51),  // overlaps with l1_0
        make_sst_meta("l2_2", b"p00", b"r00", 1024, 52),  // no overlap with l1_0
    ];

    let manifest = make_manifest_with_levels(vec![], l1, l2, vec![]);
    let task = scheduler.check_level_trigger(&manifest, Level::L1).unwrap();

    let target_ids: Vec<&str> = task.target_sstables.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(target_ids.len(), 2);
    assert!(target_ids.contains(&"l2_0"));
    assert!(target_ids.contains(&"l2_1"));
    assert!(!target_ids.contains(&"l2_2"));
}

#[test]
fn test_level_trigger_l0_returns_none() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let manifest = make_manifest_with_levels(vec![], vec![], vec![], vec![]);
    let task = scheduler.check_level_trigger(&manifest, Level::L0);
    assert!(task.is_none(), "L0 uses count trigger, not level size trigger");
}

#[test]
fn test_level_trigger_l3_returns_none() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l3 = vec![make_sst_meta("l3_0", b"a00", b"z00", u64::MAX, 100)];
    let manifest = make_manifest_with_levels(vec![], vec![], vec![], l3);
    let task = scheduler.check_level_trigger(&manifest, Level::L3);
    assert!(task.is_none(), "L3 is the bottom level, no overflow target");
}

// ---- Write Stall Tests ----

#[test]
fn test_write_stall_normal() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l0 = (0..4)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest);
    assert_eq!(status, WriteStallStatus::Normal);
    assert!(status.is_normal());
    assert!(!status.is_stopped());
    assert_eq!(status.delay_ms(), 0);
}

#[test]
fn test_write_stall_normal_below_slowdown() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    // 8 L0 SSTables: equal to slowdown trigger, not above it
    let l0 = (0..8)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest);
    assert_eq!(status, WriteStallStatus::Normal);
}

#[test]
fn test_write_stall_slowdown() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    // 9 L0 SSTables: slowdown_trigger=8, so 9 > 8, delay_ms = 9 - 8 = 1
    let l0 = (0..9)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest);
    assert_eq!(
        status,
        WriteStallStatus::Slowdown {
            l0_count: 9,
            delay_ms: 1,
        }
    );
    assert!(!status.is_normal());
    assert!(!status.is_stopped());
    assert_eq!(status.delay_ms(), 1);
    assert_eq!(status.l0_count(), 9);
}

#[test]
fn test_write_stall_slowdown_progressive() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());

    // 10 L0 SSTables: delay_ms = 10 - 8 = 2
    let l0_10: Vec<SSTableMeta> = (0..10)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();
    let manifest_10 = make_manifest_with_levels(l0_10, vec![], vec![], vec![]);
    let status_10 = scheduler.write_stall_status(&manifest_10);
    assert_eq!(
        status_10,
        WriteStallStatus::Slowdown {
            l0_count: 10,
            delay_ms: 2,
        }
    );

    // 11 L0 SSTables: delay_ms = 11 - 8 = 3
    let l0_11: Vec<SSTableMeta> = (0..11)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();
    let manifest_11 = make_manifest_with_levels(l0_11, vec![], vec![], vec![]);
    let status_11 = scheduler.write_stall_status(&manifest_11);
    assert_eq!(
        status_11,
        WriteStallStatus::Slowdown {
            l0_count: 11,
            delay_ms: 3,
        }
    );
}

#[test]
fn test_write_stall_stopped() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    // 12 L0 SSTables: stop_trigger=12, so >= 12 => Stopped
    let l0 = (0..12)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest);
    assert_eq!(status, WriteStallStatus::Stopped { l0_count: 12 });
    assert!(status.is_stopped());
    assert!(!status.is_normal());
    assert_eq!(status.l0_count(), 12);
}

#[test]
fn test_write_stall_stopped_above_threshold() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l0 = (0..15)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest);
    assert_eq!(status, WriteStallStatus::Stopped { l0_count: 15 });
}

#[test]
fn test_write_stall_l0_count() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());

    // Normal: l0_count returns 0
    let manifest_0 = make_manifest_with_levels(vec![], vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest_0);
    assert_eq!(status.l0_count(), 0);

    // Slowdown: l0_count returns actual count
    let l0_9: Vec<SSTableMeta> = (0..9)
        .map(|i| make_sst_meta(&format!("l0_{i}"), b"a", b"z", 1024, i))
        .collect();
    let manifest_9 = make_manifest_with_levels(l0_9, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest_9);
    assert_eq!(status.l0_count(), 9);

    // Stopped: l0_count returns actual count
    let l0_13: Vec<SSTableMeta> = (0..13)
        .map(|i| make_sst_meta(&format!("l0_{i}"), b"a", b"z", 1024, i))
        .collect();
    let manifest_13 = make_manifest_with_levels(l0_13, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest_13);
    assert_eq!(status.l0_count(), 13);
}

// ---- Priority Tests ----

#[test]
fn test_check_triggers_priority_order() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l1_max = 256 * 1024 * 1024;

    // 5 L0 SSTables (triggers L0->L1)
    let l0 = (0..5)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    // L1 oversize (triggers L1->L2)
    let l1 = vec![
        make_sst_meta("l1_0", b"a00", b"m00", l1_max / 2 + 1, 100),
        make_sst_meta("l1_1", b"n00", b"z00", l1_max / 2 + 1, 200),
    ];

    let manifest = make_manifest_with_levels(l0, l1, vec![], vec![]);
    let tasks = scheduler.check_triggers(&manifest);

    assert!(tasks.len() >= 2, "should have at least 2 tasks");
    assert_eq!(
        tasks[0].task_type,
        CompactionType::L0ToL1,
        "first task should be L0->L1 (highest priority)"
    );
    assert_eq!(
        tasks[1].task_type,
        CompactionType::LevelToLevel,
        "second task should be level->level"
    );
    assert_eq!(tasks[1].source_level, Level::L1);
}

#[test]
fn test_check_triggers_multiple_levels() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());
    let l1_max = 256 * 1024 * 1024;
    let l2_max: u64 = 2_560 * 1024 * 1024;

    let l0 = (0..5)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    let l1 = vec![
        make_sst_meta("l1_0", b"a00", b"m00", l1_max / 2 + 1, 100),
        make_sst_meta("l1_1", b"n00", b"z00", l1_max / 2 + 1, 200),
    ];

    let l2 = vec![
        make_sst_meta("l2_0", b"a00", b"m00", l2_max / 2 + 1, 50),
        make_sst_meta("l2_1", b"n00", b"z00", l2_max / 2 + 1, 60),
    ];

    let manifest = make_manifest_with_levels(l0, l1, l2, vec![]);
    let tasks = scheduler.check_triggers(&manifest);

    assert_eq!(tasks.len(), 3, "should have L0->L1, L1->L2, and L2->L3 tasks");
    assert_eq!(tasks[0].task_type, CompactionType::L0ToL1);
    assert_eq!(tasks[1].source_level, Level::L1);
    assert_eq!(tasks[1].target_level, Level::L2);
    assert_eq!(tasks[2].source_level, Level::L2);
    assert_eq!(tasks[2].target_level, Level::L3);
}

#[test]
fn test_check_triggers_no_triggers() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());

    // 2 L0 SSTables (below threshold), small L1
    let l0 = vec![
        make_sst_meta("l0_0", b"a00", b"m00", 1024, 1000),
        make_sst_meta("l0_1", b"n00", b"z00", 1024, 1001),
    ];

    let l1 = vec![make_sst_meta("l1_0", b"a00", b"z00", 1024, 500)];

    let manifest = make_manifest_with_levels(l0, l1, vec![], vec![]);
    let tasks = scheduler.check_triggers(&manifest);
    assert!(tasks.is_empty(), "no triggers should fire");
}

#[test]
fn test_check_triggers_only_l0() {
    let scheduler = CompactionScheduler::new(CompactionConfig::default());

    let l0 = (0..5)
        .map(|i| {
            make_sst_meta(
                &format!("l0_{i}"),
                b"a00",
                b"z00",
                1024,
                1000 + i,
            )
        })
        .collect();

    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    let tasks = scheduler.check_triggers(&manifest);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_type, CompactionType::L0ToL1);
}

// ---- Custom config tests ----

#[test]
fn test_custom_config_l0_trigger() {
    let config = CompactionConfig {
        l0_compaction_trigger: 2,
        l0_slowdown_trigger: 4,
        l0_stop_trigger: 6,
        ..Default::default()
    };
    let scheduler = CompactionScheduler::new(config);

    // 3 L0 SSTables with trigger=2 should fire
    let l0 = (0..3)
        .map(|i| make_sst_meta(&format!("l0_{i}"), b"a", b"z", 1024, i))
        .collect();
    let manifest = make_manifest_with_levels(l0, vec![], vec![], vec![]);
    assert!(scheduler.check_l0_trigger(&manifest).is_some());

    // Stall checks with custom thresholds
    let l0_5: Vec<SSTableMeta> = (0..5)
        .map(|i| make_sst_meta(&format!("l0_{i}"), b"a", b"z", 1024, i))
        .collect();
    let manifest_5 = make_manifest_with_levels(l0_5, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest_5);
    assert_eq!(
        status,
        WriteStallStatus::Slowdown {
            l0_count: 5,
            delay_ms: 1,
        }
    );

    let l0_6: Vec<SSTableMeta> = (0..6)
        .map(|i| make_sst_meta(&format!("l0_{i}"), b"a", b"z", 1024, i))
        .collect();
    let manifest_6 = make_manifest_with_levels(l0_6, vec![], vec![], vec![]);
    let status = scheduler.write_stall_status(&manifest_6);
    assert_eq!(status, WriteStallStatus::Stopped { l0_count: 6 });
}

#[test]
fn test_scheduler_config_accessor() {
    let config = CompactionConfig {
        l0_compaction_trigger: 10,
        ..Default::default()
    };
    let scheduler = CompactionScheduler::new(config);
    assert_eq!(scheduler.config().l0_compaction_trigger, 10);
}
