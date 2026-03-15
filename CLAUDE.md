# flushdb — Claude Code Instructions

## Project Overview

flushdb is a distributed key-value database in Rust using S3 as source of truth with LSM-tree architecture. See `docs/PRD.md` for full product spec and `docs/STORAGE_DESIGN.md` for storage internals. See `docs/plan.md` for the phased implementation plan.

## Core Principles

This is a **production-grade database**. Every decision — from error handling to memory layout — must reflect that. No shortcuts, ever.

- **Memory safety first**: Follow Rust's ownership, borrowing, and lifetime rules rigorously. **Never use `unsafe` Rust — no exceptions, even if the user approves.** No leaking resources, no unchecked indexing, no silent truncation. Always find a safe alternative.
- **Every public function must have tests, and all tests must pass**: No exceptions. If a function exists in the public API, it has tests covering happy path, error cases, and edge cases. `cargo test --workspace` must be green before any commit.
- **No shortcuts in implementation**: Do not stub out functions, skip error handling, use `todo!()` / `unimplemented!()` in shipped code, or take any implementation shortcut. Every code path must be complete and correct.
- **Verify major decisions with the user**: Before making architectural choices, changing public APIs, picking between design alternatives, or deviating from the task description — stop and ask. Do not assume or guess on anything that affects the system's correctness or design.
- **Think production**: Every line of code should be written as if it will handle real user data under load. Consider crash recovery, data integrity, error propagation, and resource cleanup in every component.
- **Extensive Production Testing**: Write tests that simulate real-world usage patterns and edge cases to catch bugs early, not filler tests. Alwaays look for critical gaps and edge cases that may not be covered by happy path tests.
- **Open to fix previous implementation**: If a previous implementation was wrong or incomplete, do not preserve it. Fix it instead. So for current and future implementations, if you see that previous code is broken, fix it rather than preserving it.

## ClickUp Task Management — Mandatory Workflow

All implementation work MUST be driven by ClickUp tasks. Never write code without a corresponding task. Treat ClickUp as the single source of truth for what to build, in what order, and what "done" means.

### Workspace Reference
- **Workspace ID**: `90161509939`
- **Space**: `flushdb` (ID: `90166458975`)
- **List**: `List` (ID: `901613787904`)
- **Task hierarchy**: Milestones (Phases 1-10) → Subtasks (implementation units)

### Task Lifecycle — Follow This Exact Flow

#### 1. Pick the Next Task
```
Before starting ANY work:
1. Use clickup_search to find milestone tasks sorted by priority
2. Within the current milestone, find subtasks ordered by creation (lowest ID first)
3. Only work on tasks whose dependencies are completed
4. Never skip ahead to a later phase — phases are sequential
5. Within a phase, prefer urgent → high → normal → low priority
```

#### 2. Understand the Task
```
Before writing any code:
1. Use clickup_get_task with the task_id to read FULL description
2. Read every file referenced in the task description
3. Read PRD.md and STORAGE_DESIGN.md sections referenced
4. Understand acceptance criteria ("Done when" section)
5. If anything is ambiguous, ask the user — do NOT guess
```

#### 3. Start Work — Update Status
```
When you begin implementation:
1. Use clickup_update_task to set status to "in progress"
2. Add a comment via clickup_create_task_comment:
   "Starting implementation. Approach: [brief plan]"
```

#### 4. Implement
```
While coding:
- Follow the Rust conventions section below
- Write tests that match the "Done when" criteria exactly
- Run `cargo build --workspace` and `cargo test --workspace` after changes
- Keep commits atomic — one logical change per commit
```

#### 5. Verify — Act as Your Own Reviewer
```
Before marking complete, review your own work as a human reviewer would:
1. Re-read the task description and acceptance criteria
2. Verify EVERY "Done when" bullet is satisfied
3. Run `cargo build --workspace` — must succeed with no warnings
4. Run `cargo test --workspace` — all tests must pass
5. Run `cargo clippy --workspace` — no warnings
6. Check: Does the code match the types/signatures in the task description?
7. Check: Are edge cases from the description handled?
8. Check: No unnecessary code, no over-engineering beyond what was asked
```

#### 6. Complete — Update ClickUp
```
After verification passes:
1. Use clickup_update_task to set status to "complete"
2. Add a completion comment via clickup_create_task_comment:
   "Completed. Summary of changes:
   - [files created/modified]
   - [key decisions made]
   - [tests added]
   All acceptance criteria verified."
3. Commit the code with a descriptive message
4. Move to the next task (back to step 1)
```

#### 7. If Blocked
```
If you cannot complete a task:
1. Add a comment explaining the blocker
2. Keep status as "in progress"
3. Ask the user for guidance — do NOT silently skip
4. Never mark a task complete if any acceptance criteria fails
```

### Task Discovery Commands

Use these patterns to find work:

```
# Find all milestones (phases) ordered by priority
clickup_search(keywords="Phase", filters={asset_types: ["task"]})

# Find subtasks for a specific milestone
clickup_get_task(task_id="<milestone_id>", subtasks=true)

# Find tasks in progress (check for stale work)
clickup_search(keywords="", filters={asset_types: ["task"], task_statuses: ["active"]})

# Find completed tasks (to understand what's already built)
clickup_search(keywords="", filters={asset_types: ["task"], task_statuses: ["done"]})
```

### Comment Conventions

Use structured comments for traceability:

- **Starting**: `"Starting: [approach summary]"`
- **Progress update**: `"Progress: [what's done, what remains]"`
- **Decision**: `"Decision: [choice made and why]"`
- **Blocked**: `"Blocked: [what's blocking and what's needed]"`
- **Completed**: `"Completed: [summary of changes and files touched]"`

### Milestone Gate Reviews

When ALL subtasks in a milestone are complete:
1. Run the milestone's top-level "Done when" criteria from `plan.md`
2. Add a comment to the milestone task summarizing the phase delivery
3. Mark the milestone as complete
4. Only then proceed to the next phase

---

## Core Principles

This is a **production-grade database**. Every decision — from error handling to memory layout — must reflect that. No shortcuts, ever.

- **Memory safety first**: Follow Rust's ownership, borrowing, and lifetime rules rigorously. **Never use `unsafe` Rust — no exceptions, even if the user approves.** No leaking resources, no unchecked indexing, no silent truncation. Always find a safe alternative.
- **Every public function must have tests, and all tests must pass**: No exceptions. If a function exists in the public API, it has tests covering happy path, error cases, and edge cases. `cargo test --workspace` must be green before any commit.
- **No shortcuts in implementation**: Do not stub out functions, skip error handling, use `todo!()` / `unimplemented!()` in shipped code, or take any implementation shortcut. Every code path must be complete and correct.
- **Verify major decisions with the user**: Before making architectural choices, changing public APIs, picking between design alternatives, or deviating from the task description — stop and ask. Do not assume or guess on anything that affects the system's correctness or design.
- **Think production**: Every line of code should be written as if it will handle real user data under load. Consider crash recovery, data integrity, error propagation, and resource cleanup in every component.
- **Extensive Production Testing**: Write tests that simulate real-world usage patterns and edge cases to catch bugs early, not filler tests.

---

## Rust Conventions

### Code Style
- Use `thiserror` for all error types
- Use `bytes::Bytes` for byte buffers, not `Vec<u8>` in public APIs
- Prefer `&[u8]` for input parameters, `Bytes` for owned returns
- All async code uses `tokio` runtime
- No `Arc<Mutex<>>` — single-owner data structures (shard-per-core architecture)
- Traits use `#[async_trait]` when async methods are needed

### File Organization
- One module per major type (e.g., `composite_key.rs`, `storage_backend.rs`)
- Re-export public API from `lib.rs`
- Tests go in a separate `tests/` directory inside each crate (e.g., `crates/flushdb-types/tests/composite_key_tests.rs`), NOT inline `#[cfg(test)]` blocks
- Integration tests in `flushdb-test` crate

### Comments
- Only add comments where the logic is non-obvious or surprising
- Do not add doc comments to every function — only where the signature doesn't speak for itself
- No redundant comments that restate what the code does (e.g., `// Returns the record_id` above a method called `record_id()`)
- Prefer self-documenting code over comments

### Testing
- Every public function must have tests
- Use `LocalFsBackend` with `tempdir` for all storage tests
- Test both happy path and error cases
- Property-based tests where applicable (e.g., CompositeKey round-trip)
- Name tests descriptively: `test_composite_key_rejects_oversized_record_id`
- **See `docs/TESTING.md`** for full testing standards: testcontainers patterns, S3 test categories, parity testing, container cleanup, and the rule that tests must never be skipped

### Dependencies Between Crates
```
flushdb-proto       → (standalone, generated code)
flushdb-types       → bytes, thiserror, serde, tokio (core types + traits)
flushdb-wal         → flushdb-types, crc32fast
flushdb-storage     → flushdb-types (SSTable format)
flushdb-engine      → flushdb-types, flushdb-wal, flushdb-storage
flushdb-server      → flushdb-engine, flushdb-proto, tonic
flushdb-test        → all crates (integration tests)
```

---

## Git Workflow

- Branch: `designing-0` is the current working branch
- Main branch: `main`
- Commit after each subtask completion
- Commit message format: `phase-N: <short description>` (e.g., `phase-1: implement CompositeKey with separator encoding`)
- Never commit code that doesn't compile or has failing tests
- **Do NOT add `Co-Authored-By` lines** — no co-author trailers in commits

---

## Reference Documents

| Document | Purpose |
|----------|---------|
| `PRD.md` | Full product requirements — data model, API, architecture |
| `STORAGE_DESIGN.md` | Storage engine internals — SSTable format, compaction, cache |
| `plan.md` | Phased implementation plan with acceptance criteria |
| `TESTING.md` | Testing standards — testcontainers, S3 test categories, parity tests, cleanup |

Always read the relevant PRD/STORAGE_DESIGN sections before implementing a task. The task descriptions reference specific sections (e.g., "PRD 4.2", "PRD 7.1").
