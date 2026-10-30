# Remove the partition-table `cf_name` override

## Goal

Remove the stored `cf_name` override from partition metadata while preserving compatibility with older Restate servers and tools, including alternating read-modify-write (RMW) operations and rollback.

Prerequisite supplied by the project owner: custom column-family overrides have never been relied upon. This plan preserves the derived column-family name; it does not migrate RocksDB data.

## Pre-change behavior

- `Partition::new` initializes the override to `None`. `Partition::cf_name()` honors an override when present, otherwise uses `CfName::for_partition`, which derives `data-{partition_id}`. RocksDB lookup uses this getter (`crates/types/src/partition_table.rs:258–260,289–296,317–320`; `crates/partition-store/src/partition_db.rs:95–99`).
- `PartitionShadow` stores `cf_name: Option<CfName>`, and both table conversions preserve it (`crates/types/src/partition_table.rs:463–469,499–505,530–536`).
- The table uses Serde/FlexBuffers for storage and metadata propagation (`crates/types/src/partition_table.rs:196`; `crates/types/src/net/metadata.rs:319–324,360–363`).
- Server replication updates rebuild the typed partition table (`crates/admin/src/cluster_controller/service.rs:796–825`).
- `restatectl metadata get` decodes and serializes the typed table. `metadata patch` operates on generic JSON; `metadata put` performs a root replacement through the patch path (`tools/restatectl/src/commands/metadata/get.rs:95–124`; `tools/restatectl/src/commands/metadata/patch.rs:73–105`; `tools/restatectl/src/commands/metadata/put.rs:50–73`).

Source line numbers refer to the revision inspected when this plan was written.

## Compatibility contract

1. New readers accept legacy metadata containing `cf_name` and ignore the override.
2. New writers omit the field.
3. Supported old readers accept omission and use their derived default.
4. Old typed writers may reintroduce `cf_name: null`; new readers continue accepting it.
5. Alternating old/new writes preserve all unrelated metadata and normal version semantics.
6. Field presence alone never triggers a cleanup write or migration.

Non-default overrides are outside the semantic compatibility guarantee: old code honors them, whereas the proposed new code derives the name. Merely accepting such metadata does not make mixed-version storage access safe (`crates/types/src/partition_table.rs:317–320`).

## Execution

### 1. Prove compatibility before removing the field

- [x] Identify the supported server/tool release range and inspect the actual old reader definitions. Do not infer compatibility with every release from the current optional field alone.
- [x] Freeze the pre-change wire representation in a test fixture or legacy test type.
- [x] Test actual `StorageCodec` encoding/decoding and JSON conversions, including these sequences:

| Sequence | Required result |
| --- | --- |
| Old writes null → new reads | Accepted; identical derived CF name |
| New omits field → old reads | Accepted; old override is `None` |
| New writes → old typed RMW → new reads | Reintroduced null is accepted |
| Old writes → new typed RMW → old reads | Omission is accepted |
| Generic tool patch between old/new writes | Both readers accept the result |
| Typed tool get → edit → put | Both readers accept the result |

- [x] Cover absent, null, and explicitly stored canonical names. Include a legacy non-default string to verify new-reader tolerance, without asserting mixed-version storage equivalence for that case.
- [x] Repeat alternating RMW cycles, checking partition IDs, key ranges, log IDs, replication, derived CF names, and expected version increments.
- [x] Preserve the existing fixed-table compatibility coverage, which tests the older `version`/`num_partitions` representation (`crates/types/src/partition_table.rs:586–623`).
- [x] Verify omission against supported historical reader definitions/binaries. Treat failures as a reason to revise the rollout design before implementation.

### 2. Remove the override

- [x] Remove the stored field from `Partition` and its constructor initialization.
- [x] Implement `Partition::cf_name()` directly using `CfName::for_partition(self.partition_id)`.
- [x] Remove `PartitionShadow::cf_name` and the corresponding conversion assignments.
- [x] Retain unknown-field tolerance so old writers can reintroduce the legacy field safely.
- [x] Search the workspace for affected field accesses and serialization consumers.

Original change points: `crates/types/src/partition_table.rs:286,295,317–320,469,503,535`.

Preserve the getter and derived name for existing storage and display consumers. The SQL partition row currently obtains its CF name through this getter (`crates/storage-query-datafusion/src/partition/row.rs:27`).

Prefer complete wire-field omission once the compatibility tests pass. A permanent null placeholder would not remove the need for old readers to tolerate omission by generic tools.

### 3. Preserve normal RMW behavior

- [x] Use ordinary versioned updates; introduce no field-presence-driven cleanup or special migration version.
- [x] Verify that old/new writers can alternate between null and omission without write loops, lost unrelated changes, decode errors, or panics.
- [x] Exercise a concurrent-update conflict through the existing RMW path.

The server RMW implementation already uses version preconditions and handles contention (`crates/core/src/metadata/metadata_client_wrapper.rs:110–145`).

### 4. Validate mixed-version operation and rollback

- [x] Start from existing partition data and verify reads on old and new servers.
- [x] Change replication through server RMW.
- [x] Perform unrelated metadata edits with old and new tools, covering both generic patch and typed get/edit/put workflows.
- [x] Restart servers and verify existing data remains accessible.
- [x] Roll back to an old binary after a new writer has omitted the field.
- [x] Confirm metadata propagation succeeds across the mixed-version cluster.

### 5. Complete repository validation

- [x] `cargo check`
- [x] `cargo nextest run --all-features`
- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --all-features --all-targets --workspace -- -D warnings`
- [x] Review release-note needs. If this remains an internal refactor with no user action or observable behavior change, no release note is required. Document any actual migration or behavioral impact if the design changes (`release-notes/README.md:111–124`).

## Acceptance criteria

- The override is absent from the new in-memory and serialized partition representations.
- Derived RocksDB column-family names remain identical for the supported metadata states.
- Supported old/new servers and tools can read and rewrite each other's output repeatedly.
- Legacy field reintroduction does not cause rejection, crashes, or cleanup churn.
- Mixed-version operation, restart, and rollback retain access to existing data.
- Compatibility tests and required validation pass.

## Status

Implemented and validated locally. The stored override is removed; the getter still derives the CF name. Compatibility tests live in `crates/types/src/partition_table/compatibility_tests.rs`. The server RMW conflict test is `partition_table_rmw_retries_preserving_concurrent_changes` in `crates/core/src/metadata/manager.rs:385`.

Historical-reader evidence:

- The checked-out workspace is v1.8.2-dev (`Cargo.toml:35`). The v1.8.0 release notes permit upgrade/rollback with v1.7.3 or newer, subject to their migration requirements (`release-notes/v1.8.0.md:265,333`). Checked every stable v1.7 tag from v1.7.3 through the latest v1.7.14, plus v1.8.0 and v1.8.1. Their partition-table shadow definitions and CF getter are identical. Historical binaries were exercised at v1.7.14 and v1.8.1, not at every patch release.
- Inspected upstream `crates/types/src/partition_table.rs` at tags [v1.7.3](https://github.com/restatedev/restate/blob/v1.7.3/crates/types/src/partition_table.rs#L463-L487), [v1.7.11](https://github.com/restatedev/restate/blob/v1.7.11/crates/types/src/partition_table.rs#L463-L487), [v1.8.0](https://github.com/restatedev/restate/blob/v1.8.0/crates/types/src/partition_table.rs#L463-L487), and [v1.8.1](https://github.com/restatedev/restate/blob/v1.8.1/crates/types/src/partition_table.rs#L463-L487). All four have the same partition-table shadow fields, including `cf_name: Option<CfName>` without a custom default or skip rule. The test freezes those definitions, rather than using the modified production shadows.
- Historical tool behavior differs: v1.7.14 `metadata get` is generic ([get.rs:30–34](https://github.com/restatedev/restate/blob/v1.7.14/tools/restatectl/src/commands/metadata/get.rs#L30-L34)); v1.8.1 uses typed partition-table decoding ([get.rs:95–124](https://github.com/restatedev/restate/blob/v1.8.1/tools/restatectl/src/commands/metadata/get.rs#L95-L124)). Unit tests model the JSON/storage boundary; live checks also executed the actual tools.

Live validation completed:

- Checked the external runner's matrix as required by `docs/dev/release-testing.md:20`. It still defaults to v1.7.2: [runner script:14](https://github.com/restatedev/e2e-verification-runner/blob/main/scripts/run-verification.sh#L14) and its forward/backward/random workflow matrices specify v1.7.2. That baseline does not supersede the v1.8 release notes' v1.7.3 minimum. Also inspected the identical v1.7.2 reader definitions; no v1.7.2 server upgrade was attempted.
- Created a separate local three-node cluster, `cf-name-compat`, running v1.8.1, replication 2, 24 partitions. Seeded 96 counters and verified the persisted values through all three ingress nodes. The user's existing `df-cluster` was not changed.
- Ran actual v1.8.1 and newly built v1.8.2-dev tools against the old servers: two alternating cycles of each tool's generic patch and get/edit/put, with exact version checks and preservation of all other known fields. Old typed writes restored null; new typed writes omitted the field. A stale-version patch was rejected without changing the metadata. Restarted all three old servers after omission and verified all 96 values through each ingress node, with unchanged partition-table version 9 and no stored `cf_name` key.
- Built the changed server with `just docker-local-fedora`: `localhost/restatedev/restate:4b55060b8456` (image manifest digest `sha256:796845bf79f20b673391b3b79b5148b44d70bdf2cf4631c91648a29ef93bc0fa`). Rolled one node at a time to the new image, verifying all 96 values through every ingress node at both mixed-version stages. In the mixed cluster, old-server replication RMW restored null (v18 -> v19), and new-server replication RMW omitted it (v19 -> v20). Repeated actual old/new tool workflows on mixed and all-new servers.
- Updated all 96 counters by +1000 on the new servers, restarted all new nodes, and verified those writes. Rolled back one node at a time to v1.8.1 after omission; checked data at every stage and repeated old/new tool workflows after rollback. Metadata remained at v28 throughout the restart/rolling rollback, then advanced normally to v36 through explicit tool edits.
- Rolled back further to v1.7.14 with `RESTATE_EXPERIMENTAL_ENABLE_VQUEUES=true`, as required by `release-notes/v1.8.0.md:333–339`. Verified the same data, exercised actual v1.7.14 generic patch/get/edit/put tools, and ran a mixed v1.8.2-dev/v1.7.14 cluster. Old-server RMW restored null at v41; new-server RMW omitted it at v42. Rolled the new node back to v1.7.14 and verified all values again.
- At every checkpoint, verified each node's local `partition_table_version` through its `/health` response, not just the stored metadata. Raw metadata bytes confirmed actual field presence/omission. Final metadata preserved all partition IDs, key ranges, effective log IDs, and DB names; all final containers had zero automatic restarts and no OOM kills.
- The rcluster health query has a cross-minor casing mismatch: v1.7.14 reports `effective_mode = 'Leader'`, while rcluster checks `'leader'`. Used a case-normalized SQL health query in the validation helper; all 24 partition leaders were active. No production change was needed for this test-harness issue.
- Stopped the `cf-name-compat` cluster after validation; its data/spec remain available. The user's existing `df-cluster` was not changed. Live helper, compose override, source snapshots/comparisons, tested image metadata, raw metadata bytes, JSON snapshots, node-identification responses, and final logs are retained under `/tmp/opencode/cf-name-compat/`.

The omission test passed before removing the production field, along with the existing partition-table tests (`cargo nextest run -p restate-types partition_table --all-features`: 4 passed). Final post-removal validation passed:

- `cargo check` (workspace).
- `cargo nextest run -p restate-types --all-features`: 420 passed, 3 skipped.
- `cargo nextest run --all-features`: 1702 passed, 12 skipped (77 binaries), including the server RMW conflict test.
- `cargo clippy -p restate-types --all-features --all-targets -- -D warnings`.
- `cargo clippy --all-features --all-targets --workspace -- -D warnings`.
- `cargo fmt --all -- --check`.

All planned local validation is complete.

Pre-commit checks were repeated. The first full test run failed in
`delayed_flush_is_fenced_across_partition_replacement` and timed out in
`snapshot_import_failure_invalidates_only_after_validation`. A full rerun with
`cargo nextest run --all-features --no-fail-fast` passed all 1702 tests, with
12 skipped, without code changes. Build, formatting, and workspace Clippy checks
also passed. The rerun log is `/tmp/opencode/cf-name-precommit-nextest.log`.

Release-note review: no note added for this internal refactor/test change, under the supplied prerequisite that custom overrides have never been relied upon (`release-notes/README.md:120–124`).
