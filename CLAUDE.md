# flushdb — Agent Instructions

## Project Overview

flushdb is a distributed key-value database in Rust using S3 as source of truth with LSM-tree architecture.

| Document | Path | Purpose |
|----------|------|---------|
| System Design | `docs/STORAGE_DESIGN.md` | **Authoritative** — data model, storage engine, cluster coordination, API, multi-tenancy |
| PRD | `docs/PRD.md` | High-level summary — problem, data model, API surface, non-goals |
| Implementation Plan | `docs/plan.md` | Phased implementation plan with acceptance criteria per phase |
| Testing Standards | `docs/TESTING.md` | Testcontainers patterns, S3 test categories, parity tests, container cleanup |

> ALWAYS read the relevant `STORAGE_DESIGN.md` and `PRD.md` sections before implementing a task. Task descriptions reference specific sections (e.g., "PRD 4.2", "PRD 7.1").

---

## Hard Rules

These are non-negotiable. Violating any of these is a blocking error.

1. **NEVER use `unsafe` Rust** — no exceptions, even if the user approves. Always find a safe alternative.
2. **NEVER use `todo!()`, `unimplemented!()`, or stub implementations** in shipped code. Every code path must be complete.
3. **NEVER commit code that fails `cargo test --workspace`** — all tests must pass before any commit.
4. **NEVER skip error handling** — no `unwrap()` in production paths, no silent failures, no swallowed errors.
5. **NEVER use `Arc<Mutex<>>`** — this project uses single-owner, shard-per-core architecture.
6. **NEVER add `Co-Authored-By` lines** — no co-author trailers in commits.
7. **Every public function MUST have tests** — happy path, error cases, and edge cases. No exceptions.
8. **ASK before making architectural choices**, changing public APIs, picking between design alternatives, or deviating from the task description. Do not assume or guess.
9. **Fix broken previous code** — if you find existing code that is wrong or incomplete, fix it rather than preserving it.
10. **Design for future phases** — before implementing a phase, read its "Future Work Considerations" section. Design interfaces, data structures, and extension points so that later phases and post-Phase 7 features can build on top without requiring rewrites or format migrations. Retrofitting is always harder than planning ahead — leave the right seams open even if you don't fill them yet.

---

## Rust Conventions

### Code Style

| Rule | Details |
|------|---------|
| Error types | Use `thiserror` for all error types |
| Byte buffers | Use `bytes::Bytes` for owned returns in public APIs, not `Vec<u8>` |
| Input parameters | Prefer `&[u8]` for byte input parameters |
| Async runtime | All async code uses `tokio` |
| Async traits | Use `#[async_trait]` when traits need async methods |
| Comments | **Do not overuse comments.** Only where logic is genuinely non-obvious. No doc comments restating what the signature says. No inline comments explaining what the code clearly does. Prefer self-documenting code — good names over explanatory comments. |

### File Organization

- One module per major type (e.g., `composite_key.rs`, `storage_backend.rs`)
- Re-export public API from `lib.rs`
- Tests go in a **separate `tests/` directory** inside each crate (e.g., `crates/flushdb-types/tests/composite_key_tests.rs`), NOT inline `#[cfg(test)]` blocks
- Integration tests go in the `flushdb-test` crate

### Testing Rules

- Every public function MUST have tests
- Use `LocalFsBackend` with `tempdir` for all storage tests
- Test both happy path and error cases
- Use property-based tests where applicable (e.g., CompositeKey round-trip)
- Name tests descriptively: `test_composite_key_rejects_oversized_record_id`
- Tests must simulate real-world usage patterns and edge cases — no filler tests
- See `docs/TESTING.md` for full standards

### Crate Dependency Graph

```
flushdb-proto       → (standalone, generated code)
flushdb-types       → bytes, thiserror, serde, tokio (core types + traits)
flushdb-wal         → flushdb-types, crc32fast
flushdb-engine      → flushdb-types, flushdb-wal (SSTable, memtable, manifest, flush, compaction, read path, cache)
flushdb-server      → flushdb-engine, flushdb-proto, tonic
flushdb-test        → all crates (integration tests)
```

---

## Git Workflow

| Setting | Value |
|---------|-------|
| Working branch | `designing-0` |
| Main branch | `main` |
| Commit message format | `phase-N: <short description>` |
| Commit frequency | After each subtask completion |

Example commit: `phase-1: implement CompositeKey with separator encoding`

Pre-commit checklist (all MUST pass):
1. `cargo build --workspace` — no warnings
2. `cargo test --workspace` — all tests pass
3. `cargo clippy --workspace` — no warnings

