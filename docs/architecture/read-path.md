# Read Path

This page describes how flushdb resolves a read request. Reads merge
results across multiple storage layers, from newest to oldest, applying
tombstone filtering and sequence number ordering to produce a consistent
view. The primary challenge is minimizing S3 round-trips, since each GET
adds 50-200 ms of latency.

---

## Merge-Read Pattern

Every read merges data across six layers, checked from newest to oldest:

```
Layer 1:  Active memtable           (in DRAM, newest writes)
Layer 2:  Frozen memtable(s)        (in DRAM, pending flush)
Layer 3:  L0 SSTables               (on S3, most recent flushes, may overlap)
Layer 4:  L1 SSTables               (on S3, non-overlapping within level)
Layer 5:  L2 SSTables               (on S3, non-overlapping within level)
Layer 6:  L3 SSTables               (on S3, non-overlapping within level)
```

**Key property of this ordering:** a result found in a higher layer
(lower number) with a higher sequence number always takes precedence.
If the winning entry is a tombstone, the key is treated as deleted.

L0 is special: SSTables in L0 can have overlapping key ranges because
they come from independent memtable flushes. All L0 SSTables must be
checked. L1 through L3 are non-overlapping within each level, so at most
one SSTable per level can contain a given key.

---

## Point Read

A point read fetches a single item identified by (record_id, item_key).

```
Step 1: Search memtables
        +--------------------+
        | Active memtable    |---> found? return immediately
        +--------------------+
        | Frozen memtable(s) |---> found? return immediately
        +--------------------+
                |
                | not found
                v
Step 2: Bloom/ribbon filter check
        Check all candidate SSTables concurrently.
        Filters reside in DRAM -- takes microseconds.
        Eliminate SSTables that definitely do not contain
        the target record_id.
                |
                | candidates identified
                v
Step 3: Fetch index blocks
        For each candidate SSTable, issue a concurrent
        S3 byte-range GET for its index block (if not cached).
        Binary search the index to locate the target data block.
                |
                v
Step 4: Fetch data blocks
        Issue concurrent S3 byte-range GETs for the target
        data blocks across all candidate SSTables.
                |
                v
Step 5: Merge by sequence number
        Among all results found, the entry with the highest
        sequence number wins.
        If the winner is a tombstone --> return "not found."
        If the winner is a value    --> return it.
```

The memtable check in step 1 avoids any S3 I/O for recently written data.
Steps 3 and 4 typically involve cached index blocks (see S3 GET Reduction
Strategies below), reducing the common case to a single S3 GET for the
data block.

---

## Range Read

A range read returns all items within a record that match a key range
(or all items if the predicate is `match_all`).

```
Step 1: Open iterators on all layers
        Bloom filter eliminates SSTables that do not
        contain the target record_id at all.
        One iterator per qualifying source (memtable,
        each L0 SSTable, one SSTable per L1-L3 level).
                |
                v
Step 2: Merge-sort by item key
        A merge cursor advances all iterators in item key
        order. Tombstone filtering is applied during the
        merge: if a tombstone has a higher sequence number
        than a data entry for the same key, the data entry
        is suppressed.
                |
                v
Step 3: Dual-buffer prefetch
        +-------------------+    +-------------------+
        | Buffer A          |    | Buffer B          |
        | (being consumed)  |    | (filling from S3) |
        +-------------------+    +-------------------+
        The read path consumes results from buffer A while
        buffer B is asynchronously filled with the next batch
        of data blocks from S3. When A is exhausted, the
        buffers swap roles.
                |
                v
Step 4: Accumulate until budget exhausted
        Results accumulate until either:
        - The byte budget is met (default 2 MB), or
        - The end of the requested key range is reached, or
        - An optional item limit is hit.
                |
                v
Step 5: Return results + page token
        The page token encodes the last item key emitted.
        The next request resumes by seeking to the key
        immediately after the token.
```

A full record read (`match_all`) is the same as a range read from the
minimum possible item key to the maximum, covering all items in the record.

---

## S3 GET Reduction Strategies

S3 latency dominates read performance. These strategies reduce the number
of GETs per read operation:

### Persistent index and filter cache

Index blocks and bloom/ribbon filter blocks are pinned in DRAM for the
lifetime of their SSTable. These are typically less than 1% of the
SSTable's total size. Once cached, checking whether an SSTable contains
a record and locating the right data block require zero S3 I/O.

### Coalesced block fetches

When a read needs multiple data blocks from the same SSTable and those
blocks are adjacent on disk, the requests are merged into a single
byte-range GET. Instead of two GETs for blocks at offsets 4096-8191 and
8192-12287, one GET fetches bytes 4096-12287.

### Speculative data block fetch

For point reads where the target key falls near a block boundary (based
on the index), both candidate blocks are fetched in a single GET. This
avoids a second round-trip when the binary search result is ambiguous
between two adjacent blocks.

### SSTable-level GET budget

Each read operation has a configurable maximum number of S3 GETs
(default 8). If a read would exceed this budget, the excess fetches are
deferred and the result is marked with a staleness flag, allowing the
caller to decide whether to issue a follow-up request.

---

## S3 Byte-Range Read Protocol

When a point read misses all caches, the following sequence of S3 GETs
resolves the data:

```
Step 1: Footer read
        GET last 80 bytes of the SSTable file.
        Parse the footer to locate bloom filter and index offsets.

Step 2: Bloom filter check
        GET the bloom filter section (offset and size from footer).
        Check whether the target record_id is present.
        If negative --> skip this SSTable entirely.

Step 3: Index block binary search
        GET the index block (offset and size from footer).
        Binary search for the data block containing the target key.

Step 4: Data block fetch
        GET the target data block.
        Decompress. Scan for the key. Cache the block in DRAM.
```

**Small SSTable optimization:** For SSTables where the footer, bloom
filter, and index block are contiguous at the end of the file and total
roughly 200 KB or less, a single byte-range GET fetches all metadata at
once, collapsing steps 1-3 into one round-trip.

In steady state, steps 1-3 are served from the DRAM cache (index and
filter blocks are pinned), reducing the common case to a single GET for
the data block.

---

## L0 Parallel GETs

L0 is the only level where SSTables can have overlapping key ranges.
This means all L0 SSTables must be checked for any given key. The read
path handles this with concurrent I/O:

```
L0 SSTables:  [SST-A]  [SST-B]  [SST-C]  [SST-D]

Step 1: Bloom filter check (all in parallel, in-memory)
        SST-A: positive
        SST-B: negative  --> eliminated
        SST-C: positive
        SST-D: negative  --> eliminated

Step 2: Data block GETs (all candidates in parallel)
        SST-A: GET data block ----+
        SST-C: GET data block ----|---> wait for all
                                  |
Step 3: Merge by sequence number
        Highest sequence number wins.
```

Because all candidate GETs are issued concurrently, the effective latency
is approximately one S3 round-trip, not N round-trips. The bloom filter
check runs entirely in DRAM and completes in microseconds, so it adds
negligible overhead.

---

## NVMe Tier Prefetch

When the NVMe cache tier is active, sequential scan patterns trigger
automatic prefetching:

**Detection:** Three or more consecutive block reads from the same SSTable
qualify as a sequential scan.

**Behavior:** Once detected, the next 4 blocks beyond the current read
position are prefetched from S3 into the NVMe tier.

**Eviction:** Prefetched blocks are inserted into a temporary window cache
with aggressive eviction. If the scan is abandoned (no further sequential
reads within a short window), the prefetched blocks are evicted quickly
to avoid polluting the cache.

**Scope:** Prefetch is disabled for point reads and short scans (fewer
than 3 consecutive blocks). This avoids wasting S3 bandwidth and NVMe
capacity on reads that will not benefit from lookahead.
