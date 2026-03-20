# Task 1: Workspace Scaffolding

**Crate:** All crates
**Depends on:** Nothing
**Estimated complexity:** S

---

## Goal

Create the Cargo workspace with all crate shells so `cargo build --workspace` succeeds. Every subsequent task adds code to these crates.

---

## What to Build

### 1.1 Workspace Root `Cargo.toml`

Create a workspace root that declares all member crates and shared dependency versions via `[workspace.dependencies]`.

**Member crates:**
- `crates/flushdb-proto` — protobuf + tonic generated code
- `crates/flushdb-types` — core types, traits, errors, StorageBackend
- `crates/flushdb-wal` — WAL writer/reader (shell only in Phase 1)
- `crates/flushdb-engine` — SSTable, memtable, manifest, etc. (shell only in Phase 1)
- `crates/flushdb-server` — gRPC server (shell only in Phase 1)
- `crates/flushdb-test` — integration tests (shell only in Phase 1)

**Workspace-level dependencies to declare:**

| Crate | Version | Used By |
|-------|---------|---------|
| tokio | 1 (features: full) | flushdb-types, flushdb-wal, flushdb-engine, flushdb-server |
| tonic | 0.14 | flushdb-proto, flushdb-server |
| prost | 0.14 | flushdb-proto |
| tonic-build | 0.14 | flushdb-proto (build-dep) |
| bytes | 1 | flushdb-types, flushdb-wal, flushdb-engine |
| crc32fast | 1 | flushdb-wal, flushdb-engine |
| thiserror | 2 | flushdb-types |
| serde | 1 (features: derive) | flushdb-types, flushdb-engine |
| serde_json | 1 | flushdb-engine |
| rand | 0.10 | flushdb-engine |
| tracing | 0.1 | all crates |
| uuid | 1 (features: v7) | flushdb-types |
| async-trait | 0.1 | flushdb-types |
| tempfile | 3 | flushdb-types (dev), flushdb-test |
| byteorder | 1 | flushdb-types |

### 1.2 Crate Dependency Graph

```
flushdb-proto       → tonic, prost (standalone, generated code)
flushdb-types       → bytes, thiserror, serde, tokio, uuid, async-trait, byteorder
flushdb-wal         → flushdb-types, crc32fast
flushdb-engine      → flushdb-types, flushdb-wal
flushdb-server      → flushdb-engine, flushdb-proto, tonic
flushdb-test        → all crates, tempfile
```

### 1.3 Directory Structure

```
flushdb/
  Cargo.toml                     # workspace root
  crates/
    flushdb-proto/
      Cargo.toml
      build.rs                   # tonic-build for proto compilation
      proto/
        flushdb.proto            # placeholder, filled in Task 11
      src/
        lib.rs                   # re-exports generated code
    flushdb-types/
      Cargo.toml
      src/
        lib.rs                   # re-exports all public types
      tests/                     # external test directory (NOT inline #[cfg(test)])
    flushdb-wal/
      Cargo.toml
      src/
        lib.rs                   # empty shell
    flushdb-engine/
      Cargo.toml
      src/
        lib.rs                   # empty shell
    flushdb-server/
      Cargo.toml
      src/
        lib.rs                   # empty shell
    flushdb-test/
      Cargo.toml
      tests/                     # integration tests go here
```

### 1.4 Shell Crate Contents

For crates that are shells in Phase 1 (`flushdb-wal`, `flushdb-engine`, `flushdb-server`, `flushdb-test`):
- `lib.rs` should be empty or contain only a module-level doc comment
- No `todo!()` or `unimplemented!()` — just empty files
- Cargo.toml should list their dependencies but nothing is used yet

### 1.5 Proto Build Setup

`crates/flushdb-proto/build.rs` should use `tonic-build` to compile `.proto` files from the `proto/` directory. The proto file itself is a placeholder at this stage — Task 11 fills it in.

---

## Acceptance Criteria

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo clippy --workspace` — no warnings
- [ ] All six crates are members of the workspace
- [ ] Directory structure matches the layout above
- [ ] Shell crates compile with their declared dependencies
- [ ] No `todo!()`, `unimplemented!()`, or `unsafe` anywhere
