# Compaction

Compaction merges SSTables to reduce read amplification and reclaim tombstone space.

## Level Structure

```
                  Read amplification
                  (max SSTables checked per point read)
                        │
  L0   [A] [B] [C] [D] │ 4   ← may overlap, all checked
       ────────────────►│     ← flush lands here
           4 files max  │
                        │
  L1   [═══F═══][═══G═══] 1   ← non-overlapping, binary search
           256 MB max   │
                        │
  L2   [════H════][════I════][════J════]  1  ← non-overlapping
            2.56 GB max │
                        │
  L3   [═══════K═══════][═══════L═══════] 1  ← non-overlapping, tombstone GC
            25.6 GB max │
                        │
       ─────────────────┘
       Total: at most 4 + 1 + 1 + 1 = 7 SSTables per point read
       (bloom filters eliminate most of these)
```

## Leveled Strategy (10x size ratio)

| Level | Max | Trigger | Action |
|-------|-----|---------|--------|
| L0 | 4 files | Overflow | All L0 → overlapping L1 |
| L1 | 256 MB | Overflow | One L1 file → overlapping L2 |
| L2 | 2.56 GB | Overflow | One L2 file → overlapping L3 |
| L3 | 25.6 GB | Timer (1h) | Drop expired tombstones |

## Write Stalling

| L0 Count | Effect |
|----------|--------|
| ≤ 4 | Normal. Compaction triggered. |
| 5–8 | Throttled. Each write sleeps `(count - 4) × 1ms`. |
| 9–12 | Stalled. `RESOURCE_EXHAUSTED` with `retry-after`. |

## Merge Process

```
  L0 → L1 compaction example:

  L0:  [A: keys a-m] [B: keys d-z] [C: keys a-f] [D: keys k-p]
                  │         │              │             │
                  └─────────┴──────────────┴─────────────┘
                                    │
                            merge-sort by key
                            dedup by sequence
                            drop dead tombstones
                                    │
                                    ▼
  L1:  [═══frag-0000═══][═══frag-0001═══][═══frag-0002═══]
       keys a-h          keys i-p          keys q-z
       (each ~1 GB, own bloom + index + footer)
                                    │
                                    ▼
                          CAS manifest:
                            remove [A,B,C,D] from L0
                            add [frag-0000..0002] to L1
                                    │
                                    ▼
                          Deferred DELETE of A,B,C,D from S3
                          (after no active readers hold old manifest)
```

## SSTable Run Fragments

Compaction output is a **run** of smaller fragments, not a monolith:

```
sstables/L1/run-{id}/frag-0000.sst
sstables/L1/run-{id}/frag-0001.sst
sstables/L1/run-{id}/frag-0002.sst
```

Max temporary space = `2 × fragment_size` instead of `2 × total_run_size`.

**Trivial move:** If a fragment's key range doesn't overlap the next level, it's moved without re-merge. The executor physically copies the SSTable file from the source-level path to the target-level path, updates the manifest, then deletes the source. This is necessary because SSTable paths are level-encoded — a manifest-only update would leave the file at the old path, causing not-found errors on subsequent reads. For time-ordered keys, most compactions are trivial moves.

## Tombstone Lifecycle

```
Write: DeleteItems → WAL → Memtable → L0
  │
  ▼
L1 compaction: Covered PUTs dropped. Tombstone survives — older PUTs may exist below.
  │
  ▼
L2 compaction: Same.
  │
  ▼
L3 (bottom level): Tombstone TTL checked. Expired (7d default + random 0-24h jitter) → dropped.
```

Range tombstones use a watermark protocol: only eligible for deletion when all levels below have been compacted past the tombstone's creation timestamp.
