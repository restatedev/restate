# Query correctness harness and validation plan

> Retained validation specification; the main-based C00-C30 sequence below is historical.

## Status and objective

This is the **implementation specification** for a reusable correctness harness, not a claim that
it already exists. It implements the validation requirements of the
[architecture design](query-access-and-fragment-design.md) and the
[incremental execution plan](query-infrastructure-execution-plan.md).

Build its first usable slice on main as **C00, before changing query execution**. Extend it in the
same commits that introduce new paths. C30 completes the acceptance matrix; correctness validation
must not wait until C30.

The initial C00 scope is bounded to deterministic state-table fixtures and the initial comparators.
Later sections are requirements
for the feature commits in section 12, not a request to build the entire harness in C00.

For a fixed logical data set and query, verify independently:

1. **Completeness:** no qualifying rows or groups disappear.
2. **Multiplicity:** no accidental duplicates, and legitimate duplicates remain.
3. **Values/schema:** columns, types, NULLs, and computed/aggregate values are correct.
4. **Ordering/truncation:** the stream respects ORDER BY, LIMIT, OFFSET, and ties.
5. **Execution contract:** snapshots, errors, completion reporting, and QueryId attribution are correct.
6. **Path coverage:** the intended optimization actually ran and reduced the intended work.

Every failure must be reproducible from a case/seed and produce a useful difference report. A
successful query, plausible count, or plan string containing an index name is insufficient.

## 1. Existing infrastructure to reuse

| Component | Useful foundation | Gap |
| --- | --- | --- |
| Main's `MockQueryEngine` | Real temporary RocksDB, native writes, `QueryContext`, system-table fixtures. [H1] | One local full-range partition, no remote execution. Generalize setup to several actual stores/scopes. |
| F's `LocatedTestScanner` / `TwoPartitions` | Small placement and rewrite tests. [H2] | Executes the original fragment in-process; does not prove serialization, negotiation, or Worker binding. |
| Existing result assertions | Table fixtures and column expectations. [H1] [H2] | Consume every batch and preserve duplicates. Group-keyed maps can overwrite duplicated groups. |
| Core `MockConnector` / `MockPeerConnection` | Production handshake, router/reactor, and RPC framing over test transport. [H3] | Add independent Worker state and test-only version/scheduling controls where needed. |
| Server tests using `restate-local-cluster-runner` | Real processes, provisioning, node lifecycle. [H4] | Add a small public-query/API smoke set; avoid process startup for every generated case. |
| I's cursor/metric regressions | Checks survivors and actual keys visited/seeks. [H5] | Reuse as generic scenarios and later with each concrete index. |

Use existing core/RocksDB test-util dependencies. A bounded seed-based generator can use the
workspace's random library if needed; a new general SQL fuzzing framework is not a prerequisite. [H6]

## 2. Three-way reference checking

### Oracle A: canonical logical rows + plain DataFusion

Create typed logical fixtures with explicit identities and versions. Independently construct Arrow
batches and query ordinary `MemTable` sources in a separate DataFusion session. Run the same SQL,
dialect, functions, and semantic settings, but **without Restate providers, routing, partition pruning,
index selection, or fragment rules**. [H7]

- Do not build expected rows with the production row builder or candidate scanner. Sharing schema
  declarations and fundamental ID codecs is acceptable; add explicit expected-value anchors for
  conversions likely to be wrong.
- Give the reference no invented ordering/uniqueness properties. Start with one execution partition
  to avoid distributed aggregate-state merging in the reference path.
- Use fixed IDs/timestamps and deterministic expressions. Volatile functions and externally changing
  metadata need dedicated contract tests rather than naive equality across executions.
- Include hand-calculated cases for counts, groups, joins, NULLs, ranges, and ordered ties. This
  supplies independent anchors without building a second SQL engine.

Plain DataFusion is the SQL semantic reference, not an independent proof against bugs in DataFusion
itself. The model/storage comparison and hand-calculated cases provide additional evidence.

### Oracle B: broad primary-storage scan

Populate real stores from the model. Enumerate the fixture's known partition set directly and read
the entire relevant primary relation at a fixed read view. Materialize those rows into a separate
reference session and run the same SQL.

Use the fixture-declared ownership ranges when reconstructing the logical relation. Include a split/
import case with stray physical keys outside an owned range; those are not logical rows of that partition.

Bypass secondary indexes, query partition pruning, exact-ID shortcuts, runtime filters, fragments,
and the new source/fanout planner. Low-level primary decoding may be reused; Oracle A detects
mistakes shared by that decoder and optimized execution. Do not obtain the expected partition set
from the production selector under test.

First compare broad primary rows to the model, then A's and B's SQL results. If references disagree,
report a fixture/write/decoding/reference failure; do not bless whichever matches the candidate.
Start with quiescent data on main. C09-C12 later supply explicit read views for controlled mutations.

### Candidates

```text
Canonical fixture
    +-- independent Arrow rows -> plain DataFusion ---------------- Oracle A
    +-- native writes -> all primary rows -> plain DataFusion ----- Oracle B
    +-- native writes -> production query pipeline ---------------- Candidates
                                                                  |
                                              typed comparison + contract checks
```

Legacy is a candidate and compatibility baseline, not the sole oracle. Record known legacy defects
with explicit cases/reasons and expected legacy outcomes. The new path still has to satisfy the
independent reference; do not normalize away or inherit a legacy wrong-results bug.

## 3. Compare according to SQL semantics

Compare complete typed results and terminal outcomes independently of batch boundaries. Do not
use formatted tables, JSON strings, sets, or group-keyed maps as the only representation.

| Query shape | Required comparison |
| --- | --- |
| No ORDER BY and no LIMIT/OFFSET | Equal schema and **row multiset**, including each full row's occurrence count. |
| Total ORDER BY | Exact ordered sequence plus an adjacent-key monotonicity check across batch boundaries. |
| Non-total ORDER BY, no truncation | Equal multiset and monotonic requested ordering; permutations within equal-key groups are allowed. |
| Non-total ORDER BY with LIMIT/OFFSET | Correct rank/tie-group membership, multiplicity, monotonicity, and length; do not require the reference's arbitrary tied-row choice. |
| LIMIT/OFFSET without ORDER BY | Correct permitted cardinality and a sub-multiset of qualifying rows. Separately ordered cases must test exact winners. |
| Expected failure | Correct category/stage and terminal diagnostics; a truncated stream is not successful completion. |

Most Top-K cases include a unique final SQL tie-breaker for decisive sequence comparison. Keep
additional tie-only cases, including ties across the OFFSET and LIMIT boundaries. For unordered
OFFSET O/LIMIT K, absent other SQL effects, result size is `min(K, max(N - O, 0))` for N qualifying rows.

For tied ordered results, partition the full reference into equal-sort-key groups with rank intervals.
Each group must contribute exactly the size of its intersection with the requested rank window;
within a boundary group any valid sub-multiset of that size is allowed. This checks missing better
rows without imposing arbitrary tie order. WITH TIES needs its own expanded-window rule if supported;
do not accidentally apply ordinary K-row validation to it.

LIMIT/OFFSET cases also build the reference result before the outer slice, at the same logical
row stage, to establish qualifying rows and ranks. Generate that reference form from the case's
query structure; do not remove nested limits or alter the candidate SQL with string manipulation.

If sort columns are absent from SELECT, derive ordering keys from the fixture/reference relation
using a returned identity, or provide a case-specific ordering/tie-class oracle. Do not add columns
to candidate SQL merely for validation: that can change index coverage and hide missing lookups.

Further rules:

- Check field names, logical types, nullability/required metadata, and column order. Any lossless
  representation normalization, such as dictionary decoding, is explicit. Never cast away SQL type
  or timestamp-precision differences.
- Preserve NULL versus empty string/zero, binary contents, and nested values.
- Count rows in zero-column batches. Empty batches may be ignored; projection must not erase row count.
- Compare integer/decimal/count/identity results exactly. Prefer exactly representable aggregate
  inputs. Floating aggregate cases may declare justified per-column absolute/relative tolerances
  and NaN/infinity rules; group membership, counts, and NULL behavior remain exact.
- Consume the entire stream and terminal completion. Late errors fail strict cases even when all
  preceding rows looked correct. Different batch sizes and legal arrival orders are not failures.

### Check the checker

C00 must deliberately corrupt a valid result: remove/duplicate a row, duplicate an aggregate group,
change a value/NULL/schema, swap unequal ordered rows across a batch boundary, or append a terminal
error. Every mutation must be detected. Legal batch splitting, unordered permutations, and equal-key
tie permutations must pass. This catches an overly permissive comparator before it certifies the engine.

## 4. Reusable case contract

The proposed case description contains:

- Canonical records/versions, schemas, native population adapter, and independent reference rows.
- SQL, semantic settings, comparison mode, and hand-calculated expectations where useful.
- Partition-key ranges, ownership, node roles/generations, and Coordinator location.
- Required execution variants and expected eligibility/fallback decisions.
- Lane count, batch sizing, protocol versions, and deterministic scheduling barriers.
- Optional mutations/failures with trigger points and expected terminal outcome.
- Semantic assertions and separate path/actual-work assertions.
- Replayable name and seed.

The population adapter is the index overlay's extension point. Maintenance tests write through real
lifecycle/transaction APIs; rejection tests explicitly construct incomplete/corrupt index fixtures.
Expected data comes from the model, never from the index under test.

Define the reference relation per logical source. An `_idx_*` inspection table models the explicitly
persisted index contents, including deliberately incomplete fixtures; it is not equivalent to the
complete base table. Transparent base-table substitution must instead match the complete base
relation and respect readiness. Derived stats views likewise retain their row-expansion and global
aggregation semantics in the model/reference SQL.

Live-state, node, and metadata sources get fixed records and explicit role/universe declarations.
Use the same comparator, but apply snapshot assertions only to sources claiming that guarantee.
Exact Rust types/module layout are defined in C00; these are proposed harness responsibilities.

## 5. Execution matrix and transport fidelity

Add these candidates as their implementations land:

| Candidate | What it isolates |
| --- | --- |
| Legacy local/remote | Existing SQL behavior and old-protocol compatibility. |
| New conservative primary access | Selection, placement, binding, and decoding without index shortcuts. |
| New localized filter/projection/aggregation | Rewrite semantics and intermediate-state merging. |
| Remote fragment accepted | Real encoding, transport, decoding, Worker binding, and execution. |
| Remote fragment declined | Equivalent fallback with preserved schema, ordering, and read view. |
| Covering path | Correct rows/multiplicity with zero primary lookups. |
| Non-covering path | Candidate keys, bounded lookups, order restoration, and residuals. |
| Exact-order path / local Top-K | Sort elimination, merging ordered runs, and global truncation. |
| Dynamic pruning off/on | Identical results with different runtime thresholds and storage work. |
| Mixed network versions | Query-wide mode selection, required-new rejection, request/response gating. |

Narrow test-only controls may force an eligible path, disable a new rewrite, decline a fragment,
or schedule updates. A forced path must execute or report explicit ineligibility; silently using a
fallback does not pass that feature's coverage gate. Do not add production options solely for tests.

### Three runners

1. **Fast local:** model + real storage + full production planning/execution. Extend the existing
   fixture to actual disjoint partitions, rather than reporting several logical partitions over
   one identical batch.
2. **In-process wire:** production client, codecs, router/reactor, query server, Worker contexts,
   and stores over core test transport. Workers have independent metadata, registrations, resources,
   and semantic settings. Never execute the Coordinator's original plan object as a remote shortcut.
3. **Process smoke:** a small set through real nodes/public APIs using the existing cluster runner:
   raw query, accepted fragment, ordered/aggregate query, version negotiation, and final diagnostics.
   Populate through normal APIs or a test fixture service; do not introduce a production seeding
   endpoint or mutate the live database outside its normal writer path.

`MockConnector` supplies real handshake/reactor behavior but not automatically a full independent
Worker environment or an old-version advertisement. Supply these explicitly, extending test-only
controls where needed. Process tests catch ambient task-context/registration assumptions that
loopback tests may conceal. [H3] [H4]

Do not run the full Cartesian product. Match adversarial cases to relevant modes and add bounded
seeded combinations for interactions.

Candidate execution uses the complete production optimizer sequence, including distribution/order
enforcement and final sanity validation. Direct single-rule tests remain useful supplements; they
cannot certify that the resulting plan composes correctly with the rest of DataFusion.

## 6. Mandatory adversarial corpus

Use small fixtures with stable row identities, duplicate projected values, nullable fields, several
groups/stages/services, timestamp ties, primary locators, and fields absent from a covering source.
These are harness data requirements, not new production tables.

| Family | Cases that must be represented |
| --- | --- |
| Selection | Equality/IN/AND/OR, correlated OR, negation, NULL, unsupported expressions, duplicate IDs, several IDs in one partition, empty/disjoint domains. |
| Encoded bounds | Inclusive/exclusive endpoints, finite-set holes, empty constraints, prefix boundaries, Unicode/NUL, descending fields, unconstrained leading fields. |
| Parent-prefix carry | Matching rows in later stage/service groups after an earlier suffix is exhausted; seeks must not terminate the whole scan. |
| Projection/coverage | Key-only output, missing primary fields, residual-only columns, hidden sort keys, zero-column scans, computed projection. |
| Ordering | ASC/DESC, NULLS FIRST/LAST, compound keys, equal values across batches, HLC-to-millisecond truncation, textual versus encoded ID order. |
| Top-K | Winners at the end or on another node, K=0/1/larger than input, OFFSET, tied boundaries, many rejected candidates before K qualifying rows. |
| Aggregation | Scalar/grouped COUNT/SUM/AVG/STDDEV, aggregate FILTER, empty/all-NULL groups, duplicates, negative values, groups spanning nodes. |
| Coordinator joins | Cross-node matches, duplicate/many-to-many keys, supported unmatched-row semantics, and separate scans of the same partition. Remote join pushdown remains later work. |
| Layout | One partition, several per Worker, local plus remote, at least two remote Workers, skew, empty partitions, more physical partitions than lanes. |
| Negotiation/fallback | Old peers, new binaries negotiating old protocol, unknown fragment format/operator, schema or semantic-configuration mismatch. |

Make partition sort ranges overlap and interleave winners between prefix groups. Place batch
boundaries immediately around selected rows and update points. The data must make unordered
concatenation, lost/duplicated states, and pre-residual LIMIT visibly wrong.

For each access/filter family include identity-preserving row queries as well as aggregates.
A missing row and an extra duplicate can cancel out in COUNT/SUM, so aggregate equality alone
does not prove correct candidate membership. Keep the original covering/non-covering query cases
too; these companion queries supplement them rather than changing their projections.

For quiescent data, repeat selected cases after changing insertion order, owner placement, lane
count, and batch boundaries. The same logical data must produce the same result under the chosen
comparison contract. Explicit mutation/failure cases instead follow their controlled schedule.

Include a configuration-sensitive expression with differing Worker settings. Correct portable
execution, deliberate decline/fallback, or the specified error is acceptable as declared by the
case; accepting different values because schemas match is not.

Generate only bounded, known-supported SQL shapes. Error cases should force unavoidable errors;
do not demand identical evaluation order for fallible expressions an optimizer may legally reorder
or avoid. Unsupported fragment shapes must remain correct on the Coordinator or explicitly fail
eligibility, not be presumed remotely supported.

## 7. Snapshot and mutation schedules

Use barriers/acknowledged events, not sleeps. The controller owns canonical versions and write timing:

```text
1. Commit model/store state V0.
2. Start query Q; wait until its partition read view is acquired.
3. Produce index candidates; pause before primary lookups.
4. Commit V1: update/delete candidates, insert matches, change indexed fields.
5. Resume lookup and a separately opened table scan under Q.
6. Both must match V0 for this partition.
7. A new query acquired after V1 must match V1.
```

Expected rows come from the controller model, not returned rows. Before concrete indexes exist,
run the same schedule with primary scans and point lookups; the overlay later supplies actual
index/maintenance adapters.

Also cover shared views across concurrent children, a gap between child openings, separate QueryIds,
mutations between lookup batches, and loss of the Worker/read context. Never accept silently recreated
snapshots under Q. For different partitions, use controlled acquisition points and build the oracle
from each partition's assigned model version; do not demand cross-partition snapshot consistency.

Cleanup notification while reads are active must stop children safely, preserve siblings, and release
snapshots after their last user. Test observers expose acquisition/completion control points without
implementing alternate execution. Watchdogs only prevent hung tests; production timeout/keepalive
policy remains outside scope.

Include a constrained-storage-pool case with backpressured scanning and pending primary lookups:
the scoped pipeline must make progress rather than occupy every storage thread waiting for work
that is queued on the same pool.

## 8. Dynamic-filter schedules

Run the same case with updates disabled, immediate, delayed to a chosen batch/key boundary, and
coalesced to a later generation. Compare full results. Exercise:

- No useful threshold until late in scanning.
- Tightening after stale batches have already escaped.
- Updates while a Worker is producing a blocking aggregate/Top-K batch.
- Rejection of an earlier parent suffix while later parents still have winners.
- NULL-aware thresholds and projected sort-key mappings.
- Out-of-order updates that must not overwrite a newer accepted generation.
- Completion without any update; progress must not depend on feedback.

Use the real producer or inject only predicates proved sound against the fixture and consumer stage.
Arbitrarily aggressive cutoffs are not valid correctness tests. Preserve I's checks for surviving
candidates and actual keys/seeks; result equality alone could pass with pruning entirely disabled. [H5]

## 9. Failures, partial results, and terminal completion

Until partial partition results exist, strict storage cases must fail on injected source failures.
Node introspection retains its declared warning behavior. Inject faults at Open, before the first
batch, after a controlled emitted prefix, before EOF, and during cleanup notification, locally/remotely.

For the optional partial-results feature:

- Check default-off, explicit settings, invalid settings, and concurrent/subsequent request isolation.
- Define retained contribution at a controlled boundary. A raw stream can acknowledge a known row
  prefix. A remote aggregate failing before its first output may contribute no state despite having
  read input. Compute expected contributions from the model/schedule, not returned aggregate values.
- Compare those contributions and terminal completeness, failed scope/node, and partial versus
  unstarted work. Partial runs with different plans/timing need not retain identical data by contract.
- Corruption/expression errors remain fatal; all-failed differs from genuine empty input; unsupported
  partial shapes are rejected; pruning must not depend on evidence discarded after failure.
- Verify every supported API's terminal report. Correct-looking batches without required completion
  metadata do not certify a complete result.

Compare failure categories/stages and diagnostics separately from values. Matching arbitrary error
strings or accepting any error as the expected error is insufficient.

## 10. Optimization coverage and actual work

Feature cases separately assert:

- Selected access path, lookup present/absent, requested/guaranteed order, and residual placement.
- Fragment accepted/declined as intended; a freshly decoded plan executes on the Worker.
- Sort removed/retained correctly, without relying on the entire formatted EXPLAIN string.
- Broad-scan work versus optimized keys visited, seeks, primary reads, and transmitted rows/bytes.
- Zero primary reads for covering cases; bounded reads after index-covered filtering.
- No reads/RPCs for proved empty selections and no duplicated work when grouping keys.
- Original QueryId on all child protocol/storage observations, isolated from concurrent queries.

Use deterministic counter bounds on controlled fixtures for CI. Keep elapsed-time performance
thresholds in separate benchmark analysis. Correct rows from an unintended fallback are a path-
coverage failure, not proof that the advertised optimization works.

## 11. Generated cases and replay artifacts

Run a fixed adversarial corpus by default. Add bounded seeded generation of data, predicates,
ordering, topology, lanes, batch boundaries, and update schedules. Fixed CI seeds make failures
reproducible; extended scheduled/manual runs explore more combinations through the same runner.

On failure, emit:

1. Case/seed, SQL, schemas, semantic settings, and comparison contract.
2. Canonical records/versions, native population recipe, partition ranges, and placement.
3. Candidate mode, negotiated versions, forcing controls, and mutation/failure schedule.
4. Reference/candidate plans, serialized fragment descriptors, and advertised properties.
5. Missing/excess row **counts**, first differing value, ordering inversion/invalid rank, schema
   mismatch, and terminal failure location.
6. QueryId/child IDs, snapshot events, predicate generations, work counters, and final diagnostics.

A replay entry point reruns one saved case/seed. Minimization removes rows/clauses/partitions only
while preserving the original failure category and path eligibility. A silent fallback or unsupported
plan is not a successful reduction of a wrong-results bug. Check minimized failures into the corpus.

## 12. Landing schedule and CI gates

| Feature commit | Required harness extension |
| --- | --- |
| **C00 on main** | Comparator/self-tests, canonical MemTable oracle, broad primary reference for one existing table, full-stream capture, local legacy runner, fixed cases and replay identity. |
| **C04-C07** | Selection cases across IDs/partition/node domains and budgets; exact multiplicity and selected-work assertions. |
| **C09-C12** | Real-storage mutation barriers, cross-scan shared snapshots, parent/child cleanup. |
| **C13-C15** | Production-wire runner with independent Worker state; version/mode matrix; representative process/API smoke. |
| **C16** | Storage work metrics and cumulative remote accounting as observations. |
| **C17-C20** | Decoded fragment execution, forced acceptance/decline, portability, complete aggregate-state round trips. |
| **C21-C23** | Tie/rank-aware ordered comparison, adversarial Top-K, dynamic updates while output is blocked. Basic sequence checking already exists in C00. |
| **C24-C29** | Generic keyspace fixtures, covering/non-covering lookup, prepared bounds/live seeks, eligibility/coverage. |
| **C30** | Complete cross-path suite through one case API; no critical feature validated only by a manual query or mock-plan test. |
| **Index overlay** | Real layout/maintenance adapters; readiness/backfill and snapshot consistency before base-table substitution. |
| **Partial results / C31** | Incomplete-result/API oracles; later legacy rejection and new-only execution after protocol retirement. |

Proposed test filter/module convention: `query_correctness`. The commands below become applicable
as tests land; these modules do not exist yet:

```sh
cargo nextest run -p restate-storage-query-datafusion --all-features query_correctness
cargo nextest run -p restate-server --all-features query_correctness
```

Keep fixed local/wire cases in normal nextest runs. Provision a small process smoke set using the
cluster runner. Extended generation can run separately; no new nextest profile is assumed to exist.
The current default nextest configuration only defines a slow-test timeout. [H6]

Run the workspace-required checks before committing implementation. This specification reports
no new runtime test results. C00 makes the first differential checks executable; every feature
adds its candidate/scenarios before being enabled.

## Source references

**B:** main `a1975009e3ef`; **F:** `28e937d70cb0`; **I:** `ef8eca39d82a`. Proposed harness controls
and modules above are not existing APIs.

- **[H1]** B: `crates/storage-query-datafusion/src/mocks.rs:152-238`; F: `crates/storage-query-datafusion/src/tests.rs:91-175` — real local storage fixture and example assertions.
- **[H2]** F: `crates/storage-query-datafusion/src/mocks.rs:167-255`; `crates/storage-query-datafusion/src/partial_aggregation.rs:386-468` — in-process fragments, schema-only wire check, and group-map result collection.
- **[H3]** F: `crates/core/src/network/transport_connector.rs:65-208`; `crates/core/src/network/connection_manager.rs:414-451`; `crates/core/src/network/connection.rs:443-471` — production handshake/router/reactor test helpers.
- **[H4]** F: `server/tests/cluster.rs:30-36,47-96`; `crates/local-cluster-runner/src/node/mod.rs:678,752` — process cluster setup and admin access.
- **[H5]** I: `crates/storage-query-datafusion/src/index/tests.rs:1153-1264`; `crates/partition-store/src/index/scan.rs:272-405`; `crates/storage-query-datafusion/src/scan_metrics.rs:29-147` — survivors plus actual-work assertions.
- **[H6]** F: `crates/storage-query-datafusion/Cargo.toml:56-63`; `Cargo.toml:226`; `.config/nextest.toml:1-2` — test dependencies, random library, nextest config.
- **[H7]** DataFusion 55.1.0: [MemTable](https://docs.rs/datafusion-catalog/55.1.0/datafusion_catalog/memory/struct.MemTable.html); crate source `datafusion-catalog-55.1.0/src/memory/table.rs:61-108,126-137` — independent in-memory reference and truthful ordering requirements.
