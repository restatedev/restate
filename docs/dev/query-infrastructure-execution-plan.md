# Query infrastructure: incremental execution plan from main

> Historical main-based implementation sequence, retained as design notes.

## Purpose and baseline

This is the commit-level execution plan for
[Query selection, routing, access paths, and remote fragments](query-access-and-fragment-design.md).
That document defines the contracts; this document defines the implementation order and stack boundary.

The historical sequence below records the earlier main-based delivery gates.

**Start a new implementation stack from `main`, not from either experimental stack.** The inspected
baseline is `a1975009e3ef98d2876dca4b21e8ef0d821fcbec` (main at the time of planning). Recheck the
baseline before starting implementation; line citations below are pinned to this revision.

The new stack supplies the replacement for:

- The query architecture and fragment work in Igal's `28e937d70cb0` stack (**F**), including any
  prerequisites that have not independently landed on main.
- The reusable predicate, encoded-key navigation, query integration, and metrics portions of
  the stack ending at `ef8eca39d82a` (**I**).

F and I are reference implementations and regression-test sources. Extract individual mechanisms
against main's APIs; do not merge their full diffs. Main has unrelated changes beyond both stacks.

The infrastructure stack must be useful and testable **without any new persisted secondary index**.
Existing primary-table readers are its first production consumers. At its completion, the remaining
index work can be rebased as an additive consumer of stable contracts rather than another query engine.

Rollout is side by side: a new negotiated network protocol version selects the new query message
family and infrastructure. Keep legacy execution/wire behavior unchanged until its protocol range
is deprecated. C15 isolates legacy code; **C31**, gated on protocol retirement, removes it. This
temporary compatibility path is not a second home for new query or index features.

`C00` establishes the correctness harness on main; `C01`-`C30` build the infrastructure, and C31 is
later protocol-retirement cleanup. The detailed validation contract is in the
[query correctness harness specification](query-correctness-harness.md).
These are review-sized units, not existing commits or finalized Rust type names.
Each is a coherent patch with its own tests and a compiling intermediate state. Split mechanical
call-site migrations further if necessary; do not combine adjacent semantic changes just to reduce
the commit count.

## 1. What main already contains

| Area | Baseline behavior | Implication |
| --- | --- | --- |
| DataFusion | 55.1.0 and `datafusion-proto` 55.1.0. [B1] | No DataFusion upgrade is part of this work. |
| Partition scans | `PartitionedTableProvider`, `PartitionedExecutionPlan`, key extraction, per-key/grouped scans, and round-robin execution lanes. [B2] | Replace their decisions incrementally with the common source/work planner. |
| Routing | `RemotePartitionsScanner::scan_partition` resolves local versus remote when execution reaches each scan. [B3] | Planned placement and open-time validation are new behavior. |
| Node and metadata queries | Separate node-fanout and generic providers; node warnings are found by plan-node downcasting. [B4] | Unification includes these paths and the offline consumer, not just indexed scans. |
| Remote protocol | Raw `Open`/`Next`/`Close`; client-minted scanner ID is optional; Open fields end at tag 8. [B5] | `QueryId`, scoped execution, capabilities, fragment negotiation, and storage metrics need a deliberately versioned extension. |
| Network negotiation | Supported versions V2-V4; handshake selects a common version, exposed on the connection. [B12] | Gate the new message family on a new version; retain the old minimum during coexistence. |
| Dynamic predicates | Predicate generations already travel on `Next`; scans already receive physical predicates. [B6] | Preserve this while separating static access analysis from live refinement. |
| Read views | Repeatable-read transactions use explicit snapshots; query scanners take `PartitionStore` and ordinary scan/lookup APIs. [B7] | Add query-owned read views and migrate readers; do not hold a processor transaction. |
| Point lookup and background I/O | Primary readers already have multi-get paths; RocksDB work can run on background pools. [B8] | Reuse storage mechanics, adding read-view and ordering contracts. |
| Runtime | Remote scanner setup creates a minimal query context. [B9] | Review runtime sharing before moving memory-intensive operators to Workers. |
| Index foundation | Main's `KeyKind` has no I secondary-index/stat regions; its utility string module does not expose I's mem-comparable API. [B10] | Separate generic codec/filter infrastructure from new persisted kinds, index identities, and lifecycle changes. |

The reference collision between I's `collect_metrics` and F's `expected_partition_owner` at Open
tag 9 is **not an existing collision on this main baseline**. It is a reason to allocate one new
contract instead of cherry-picking both extensions. Check deployed experimental binaries before
assigning tags or negotiating versions. [B5] [R1]

## 2. Scope and handoff

### Infrastructure delivered by this stack

- Query Coordinator/Query Worker roles and original `QueryId` propagation.
- Query-owned source declarations, predicate analysis, work selection, placement, and fanout.
- Shared local/remote execution, strict versus source-declared best-effort handling, and diagnostics.
- Worker-parent lifetime and one shared read context per `(QueryId, PartitionId)`.
- Versioned storage-local physical subtrees, with capability validation and equivalent fallback.
- Safe filter/projection and partial-aggregate pushdown; ordering and partition-local Top-K.
- Dynamic filter delivery and storage refinement, including updates while output production is busy.
- Generic ordered codecs, prepared predicates, forward seek cursors, and storage metrics.
- Unary primary lookup, coverage analysis, and the access-path registration/selection contract.

### Supplied later by the secondary-index stack

- Base/canonical entry identity changes and their lifecycle/API consequences.
- Actual index identities, fixed index prefixes, persisted `KeyKind` additions, and concrete key layouts.
- Index writes/deletes, covering values, VQueue lifecycle integration, and required statistics maintenance.
- Index feature activation, backfill/rebuild, and persistent completeness/readiness publication.
- Registration of concrete index access paths and their SQL field/ordering/population proofs.
- Optional `_idx_*` inspection surfaces and transparent substitution for eligible base tables.

The infrastructure defines readiness and population **requirements** but does not implement a
generic backfill service or certify indexes that do not exist yet. It must reject an unready path
or choose a complete fallback. This is the boundary that keeps I's remaining work additive.

### Agreed external boundary

Assume Coordinator-side stream drop notifies the Worker to clean up the associated subtree.
Implement Worker cleanup in response to that notification. Timeout, keepalive, lease, loss-detection,
and notification-delivery mechanisms remain separate work. Dynamic-predicate updates are query
execution control, not keepalives.

One `QueryId` identifies one execution. No automatic retry/restart or additional attempt identity
is introduced here. Different partitions may have different snapshots; all storage reads for one
query within one partition must share its view.

## 3. Delivery order and usable checkpoints

The default review order is linear; explicit dependencies identify the earliest valid branching point.

```text
main
  C00       independent result oracles, comparator, and replayable baseline corpus
      |
  C01-C07   identity, diagnostics, source declarations, common selection
      |
  C08-C15   physical source plans, read views, Worker scopes, protocol-gated execution
      |     checkpoint A: unified new engine; legacy isolated for old-protocol operation
      |
  C16-C23   metrics, remote subtrees, relational pushdown, ordering, Top-K feedback
      |     checkpoint B: replacement for F on main
      |
  C24-C30   reusable encoded-key engine, lookup, access-path contract, handoff verification
            checkpoint C: index infrastructure ready
                 |
                 +-- adapted secondary-index stack (section 6)
                 +-- optional partial-result feature
                 +-- later Worker-local multi-partition plans and colocated joins
                 +-- C31: remove legacy engine once its network protocol range is retired
```

**Activation discipline:** C01-C13 build the new engine beside legacy execution, reusing only
storage/protocol-independent helpers whose behavior remains compatible. New-side declarations and
normalization are tested against legacy/reference results; do not replace legacy behavior during
construction. C14 connects the new planner, binder, and protocol end to end and advertises/selects
the new version only once its baseline execution contract is implemented. Small pure libraries
may land with direct tests; no commit leaves `todo!()` paths or needs a later commit to compile.

Before data consumption, C14 selects one query-wide mode from actual negotiated connections:
new mode when all required peers support it; legacy mode for a mixed-version query that permits
legacy semantics; otherwise an explicit unsupported-contract error. New-mode consistency is never
claimed for a legacy query, and a query cannot switch modes after rows escape. A failed connection
is not a protocol downgrade. C15 isolates the mode boundary; C31 later removes legacy execution.

The network version guarantees the new message family and baseline QueryId/read-view contract.
Fragment format/operator support, semantic options, index readiness, and ownership are still checked
inside that family. Subsequent commits may add optional fragment capabilities with explicit decline;
they must not assume that every peer negotiating the new version understands every later operator.

## 4. Commit sequence

Paths below are implementation touchpoints, not a requirement to retain current module boundaries.
Test names describe scenarios; exact API/type/file names beyond existing identifiers are decided
in the relevant patch.

Every execution-changing commit adds its candidate path and adversarial cases to the C00 harness.
Its gate includes reference result comparison and terminal-contract checks, plus path/work assertions
where it introduces an optimization. The matrix grows with the features; no rewrite waits for C30
to receive end-to-end result validation. See the [harness landing schedule](query-correctness-harness.md#12-landing-schedule-and-ci-gates).

### Wave A: query identity and shared planning inputs

#### C00 — `[query] Add the differential correctness harness`

- **Change:** implement the first slice of the [harness specification](query-correctness-harness.md):
  canonical logical fixtures, a plain DataFusion/MemTable oracle, a broad primary-storage reference,
  complete typed result capture, duplicate-sensitive bag/ordered-sequence comparison, checker
  self-tests, and replayable cases. Exercise an existing main table before changing execution.
  Use `state` as the first concrete fixture and the bounded scope in the
  [correctness specification](query-correctness-harness.md); extend the full matrix later.
- **Touchpoints:** test-only query harness/fixtures beside existing query tests and storage utilities.
- **Depends on:** main only; neither experimental stack is required.
- **Gate:** dropped/duplicated rows, repeated aggregate groups, wrong values/schema, ordering inversions,
  and late errors are detected. Legal batch splitting/unordered permutations pass. The local legacy
  path runs against both references; wire, snapshot, and index runners arrive with their features.

#### C01 — `[query] Add query identity to execution context`

- **Change:** add `QueryId` as a ULID-backed `ResourceId` with prefix `qry`, using the existing
  resource-kind registration and `ulid_backed_id!` resource-ID variant. Its V1 display form is
  `qry_1<base62-encoded ULID>`. Allocate it at Coordinator ingress and capture it in per-query execution
  state; expose it to local plan execution and diagnostics. Keep it separate from `ScannerId`.
  Do not store a mutable current-query ID on a shared `SessionContext` or require task-local lookup.
- **Touchpoints:** `crates/types/src/identifiers.rs`, `id_util.rs`,
  `storage-query-datafusion/src/context.rs`, query result/ingress adapters.
- **Depends on:** C00 and main's ID APIs. **Reference:** [B1] [B5] [B11].
- **Gate:** resource-ID text/serde/binary round trips and wrong-resource-prefix rejection; two
  concurrent queries retain distinct IDs through spawned local work; cloning execution state
  preserves the ID. Public query-protocol propagation follows in C13.

#### C02 — `[query] Centralize execution diagnostics and source failures`

- **Change:** add a query-owned diagnostics sink and typed source failure categories. Adapt existing
  `NodeWarnings` reporting semantics to the new path without changing legacy node-table best-effort
  or storage strictness. The new path does not discover warnings by concrete plan-node downcasts;
  preserve the old implementation behind legacy mode until C31.
- **Touchpoints:** `context.rs`, `node_fan_out.rs`, admin gRPC query response adapter.
- **Depends on:** C01. **Reference:** [B4].
- **Gate:** existing warning responses survive a plan rewrite; storage errors still fail; node
  warnings retain source identity. No partial-partition-results option yet.

#### C03 — `[query] Declare source scope and read capabilities`

- **Change:** introduce source declarations for persisted partitions, leader-live state, per-node
  introspection, and coordinator-local metadata. Register existing producers with field identities,
  routing derivations, role eligibility, completeness policy, and primary access capabilities.
  New-side adapters reuse existing row producers; legacy provider registration remains unchanged.
- **Touchpoints:** query table registration, `context.rs`, `table_providers.rs`, node scanner registry.
- **Depends on:** C01-C02. **Reference:** [B2] [B3] [B4], architecture section 2.5's consumer manifest.
- **Gate:** representative sources from every family resolve to the correct declarations; offline
  sources use a fixed universe and do not acquire live-node dependencies.

#### C04 — `[query] Normalize predicates into reusable typed domains`

- **Change:** extract I's storage-neutral `FilterTarget`/value-conversion ideas without its index
  types. Define shared unconstrained/empty/set/range semantics and adapters for bound logical and
  physical expressions; retain original expressions as residuals. Keep SQL interpretation above storage.
  If reusing `define_filter!`, extract its small logical-marker dependency separately: I's
  `storage-api/src/table.rs` also contains canonical-ID-specific primary-key implementations. [R2]
- **Touchpoints:** `crates/storage-api` logical filters; `storage-query-datafusion/src/filter.rs`.
- **Depends on:** C03. **Reference:** [R2].
- **Gate:** one suite covers AND/OR, finite-set holes, NULL, unsupported conversion versus empty,
  and column lineage. Unsupported expressions broaden access; they never discard valid matches.

#### C05 — `[query] Share partition and primary-key selection`

- **Change:** migrate partition-key, invocation-ID, VQueue-ID/entry-ID, scope, and service-key
  extraction to C04. Reuse decoded IDs for routing and exact primary lookup. Preserve conditional
  scoped/unscoped hashing. Combine sound restrictions rather than choosing only the first match.
- **Touchpoints:** `filter.rs`, table registration derivations, existing primary filter adapters.
- **Depends on:** C04. **Reference:** [B2] [B8].
- **Gate:** existing ID/range queries match the reference path, including several IDs in one
  partition, negated predicates, contradictory selections, and scope NULL cases. New native-filter
  bindings use the shared analysis; the old visitors remain isolated in legacy mode until C31.

#### C06 — `[query] Share node selection and metadata source planning`

- **Change:** implement new-engine node selection with C04 instead of copying the legacy fanout
  parser; represent all-node/role eligibility as declarations. Give metadata sources the same planning
  inputs while retaining single-copy Coordinator execution. Keep legacy provider behavior unchanged.
- **Touchpoints:** `node_fan_out.rs`, generic provider, node/metadata registrations.
- **Depends on:** C03-C05. **Reference:** [B4].
- **Gate:** plain/generational node constraints, role restrictions, empty targets, and metadata
  queries are correct. Preserve executable baseline role policy; resolve misleading comments separately.

#### C07 — `[query] Centralize work selection and fanout budgets`

- **Change:** build explicit selected work from a query-owned partition/node universe. Preserve
  exact ID sets independently of access batching; centralize per-key/range grouping, overlap
  normalization, and execution-lane budgets. The new engine uses one capability-aware decision
  point instead of distributed `PointReadFanout` choices and the provider's 4096-key branch.
  Keep those old choices intact in legacy mode until protocol retirement.
- **Touchpoints:** `table_providers.rs`, `node_fan_out.rs`, selection/planning helpers, offline adapter.
- **Depends on:** C05-C06. **Reference:** [B2] [B3] [B4].
- **Gate:** zero work means zero RPCs; changing budgets changes work shape but not row multiplicity;
  repeated keys and overlapping ranges do not duplicate one source scan. Independent SQL inputs stay distinct.

### Wave B: execution placement and partition-consistent reads

#### C08 — `[query] Represent selected work as physical source plans`

- **Change:** construct primary-scan physical nodes and placement scopes from C07. Resolve targets
  once per plan and expose schema/partitioning/order truthfully. Keep source subtrees visible to
  access-planning rules; freeze the opaque remote boundary at lowering. Do not copy F's early
  opaque scan wrapper and later clear its ordering.
- **Touchpoints:** `table_providers.rs`, new source/placement plan implementation.
- **Depends on:** C03, C07. **Reference:** [B2] [B3] [R3].
- **Gate:** local and mixed-target plan construction, missing placement, empty selection, and
  full DataFusion sanity validation. New execution is not activated through legacy raw RPC yet.

#### C09 — `[storage] Add an owned read-only partition view`

- **Change:** add a query-suitable read view that keeps DB/column-family resources and an explicit
  RocksDB snapshot alive. Supply read options for iterators and Get/MultiGet through that view.
  Respect Rust lifetimes; do not export borrowed raw DB values from background closures or hold a
  processor-owned mutable transaction. Cleanup consumes the agreed cancellation notification.
- **Touchpoints:** `partition-store/src/partition_store.rs`, `partition_db.rs`, `rocksdb/src/lib.rs`.
- **Depends on:** main storage APIs; review after C08. **Reference:** [B7] [B8].
- **Gate:** a concurrent committed update is visible to a new view but not the original; iterator
  and point reads agree; dropping a parent cannot invalidate an active child read.

#### C10 — `[query] Bind primary iterators to the shared read view`

- **Change:** adapt persisted table scan factories to execute against C09 and pass the same view
  into all underlying iterators. Separate shared read binding from the row-conversion callback.
  Cover all persisted range-scan consumers on the new path. Retain legacy/default read entry points
  and their semantics until C31; sharing low-level code must not implicitly change their read view.
- **Touchpoints:** `partition_store_scanner.rs`, storage scan APIs, persisted query table adapters.
- **Depends on:** C08-C09. **Reference:** [B7].
- **Gate:** cross-table reads under the same explicit view remain coherent through writes; scan
  errors and cancellation propagate. Commit-wide signature changes update every caller atomically.

#### C11 — `[query] Bind primary point reads to the shared read view`

- **Change:** extend existing invocation/VQueue point and multi-get paths to C09. Reuse their
  bounded I/O loops; retain exact selections and declared output order. Keep the existing primary
  key encodings and main's identity types. Legacy readers continue using their original default
  read semantics; the new binder selects the explicit-view variants.
- **Touchpoints:** `partition-store/src/invocation_status_table`, `vqueue_table`, query adapters.
- **Depends on:** C09-C10. **Reference:** [B8].
- **Gate:** interleave writes between lookup batches; results remain from one view. Range and
  point access agree. This provides storage primitives; the composable unary lookup node is C28.

#### C12 — `[query] Own child execution under a Worker query context`

- **Change:** create the Worker parent for `QueryId` with a shared `(QueryId, PartitionId)` read
  context registry and child execution scopes. Independently opened scans reuse the same snapshot.
  Bind live/node sources to their own resources. Use the shared runtime/memory pool.
- **Touchpoints:** query execution/binding, scanner factory, node runtime wiring.
- **Depends on:** C01-C03, C09-C11. **Reference:** [B7] [B9].
- **Gate:** two independent child scans of one partition see the same version while the processor
  continues; cancelling one child preserves siblings. Parent notification stops all children before
  final snapshot release; a temporary absence of active children does not create a new view.

#### C13 — `[query] Gate new execution messages on the network protocol`

- **Change:** introduce the next unused `ProtocolVersion` and a distinct scoped-execution message
  family. Carry `QueryId`, child identity, typed source scope, placement/output requirements, and
  typed failures. Keep legacy scanner messages/responses unchanged. New send/decode/dispatch and
  response selection require the new negotiated version; add codec minimum-version checks beyond
  the generic bilrost macro's V2 minimum. Expose incoming connection version to dispatch as needed.
- **Touchpoints:** `types/protobuf/restate/common.proto`, network constants/codecs, incoming context,
  query request/response types, client/server dispatch. Audit exhaustive version matches in other services.
- **Depends on:** C02-C03, C08, C12. **Reference:** [B5] [B12] [R1].
- **Gate:** old wire fixtures and behavior are unchanged; new requests and responses cannot be used
  below the version boundary; unsupported requests get an old-compatible transport error rather
  than an unencodable response. QueryId reaches Worker children and source binding validates placement.
  Do not advertise the new maximum in production until C14's baseline path is complete.

If needed, split this review unit into the enum/codec audit, new message/dispatch family, and query
binding patches. Keep minimum-version tests with each message introduction; raising the supported
maximum and raising the minimum are different changes.

#### C14 — `[query] Execute raw sources through the shared local and remote path`

- **Change:** connect C08 physical plans to C12 binding locally and C13 remotely; activate the common
  pipeline for new-protocol queries across all source families. Add Coordinator preflight/mode
  selection outside table scan construction. Preserve legacy whole-query routing when selected,
  and capture static access predicates separately from live updates on the new path. Advertise the
  new maximum once the baseline contract works; keep the old supported minimum during coexistence.
- **Touchpoints:** `context.rs`, providers/plans, scanner manager/client/server, all source families.
- **Depends on:** C08-C13. **Reference:** [B2] [B3] [B4] [B5] [B6] [B7].
- **Gate:** run the consumer manifest through production local and remote execution. Verify the
  old/new Coordinator/Worker version matrix and query-wide mode selection, along with range
  clamping, leader requirements, placement change, diagnostics, QueryId propagation, shared snapshots,
  and cleanup notification. Required-new queries fail before reading from a legacy peer; ordinary
  legacy-mode queries retain baseline behavior. Record selected mode and document rollout semantics.
  Add the production-wire harness runner with independent Worker contexts and representative
  process/API smoke cases; an in-process fragment shortcut does not satisfy this gate.

#### C15 — `[query] Isolate legacy execution behind protocol selection`

- **Change:** establish one explicit legacy/new mode boundary. Keep `RemotePartitionsScanner`, old
  plan/fanout scheduling, sentinel adaptation, and warning discovery contained in legacy execution.
  All new-engine sources use the common planner/binder. Allow shared low-level helpers only where
  parity tests prove legacy behavior unchanged. Record the exact C31 deletion inventory.
- **Touchpoints:** provider/manager/fanout module boundaries, query dispatch, constructor/call sites.
- **Depends on:** C14. **Reference:** [B2] [B3] [B4].
- **Gate:** manifest tests cover the common engine and unchanged legacy path; a code audit shows
  no accidental crossing or per-scanner downgrade. **Checkpoint A:** the unified new engine is
  usable without fragments or indexes, while the old protocol remains supported.

### Wave C: remote physical subtrees and Top-K

#### C16 — `[query] Attribute storage work and remote metrics to queries`

- **Change:** extract I's iterator counters and cumulative high-water reporting into the new execution
  context/protocol. Attach QueryId through context plus child/partition identity; preserve unsupported
  versus zero and complete versus partial accounting. Attribute existing primary reads first.
- **Touchpoints:** `rocksdb/src/iterator.rs`, metric support, scoped query responses, DataFusion metrics.
- **Depends on:** C12-C15. **Reference:** [R4].
- **Gate:** real scans and early stops report visited keys/seeks/bytes correctly; repeated reports
  do not double-count. Network measurements do not masquerade as disk I/O or vice versa.

#### C17 — `[query] Encode and bind physical source subtrees`

- **Change:** introduce the fragment codec/binder using DataFusion serialization and custom primary
  scan leaves that describe source access. Bind resources from the Worker context, not serialized
  handles. Start with one selected partition scope and supported unary operators. Carry output
  properties and semantic requirements, not only an output schema.
- **Touchpoints:** new physical codec/binder, source plan nodes, scoped envelope.
- **Depends on:** C08, C12-C16. **Reference:** [R3].
- **Gate:** encode/decode/execute a real primary subtree against the same read view; reject unknown
  nodes, incompatible versions, invalid scope, and mismatched contracts before consuming rows.

#### C18 — `[query] Execute negotiated fragments with equivalent fallback`

- **Change:** connect C17 to the shared remote boundary. Negotiate required expression semantics,
  read-view capability, and output guarantees. Define a primary-scan/coordinator-computation fallback
  that preserves required order and schema. Storage-local operations cannot simply be rebound to
  an arbitrary Coordinator raw stream. Partial consumption remains an execution failure.
- **Touchpoints:** remote lowering, client/server negotiation, local/remote fragment execution.
- **Depends on:** C17. **Reference:** [R3].
- **Gate:** local, remote accepted, and remote declined executions agree within new-protocol mode;
  fallback preserves the same partition view. A fragment decline never switches to legacy execution.
  Include a configuration-sensitive expression case, not merely a schema round trip.

#### C19 — `[query] Push portable filters and projections into source subtrees`

- **Change:** add the row-wise localization rule over the new source scope. Retain exact residuals
  unless equivalence is proved. Use a conservative portable-expression set and remap field lineage;
  keep mutable predicates at their existing scan update boundary instead of encoding frozen copies
  inside the fragment. C23 generalizes their bindings to arbitrary supported subtree inputs.
- **Touchpoints:** physical optimizer rule and expression portability checks.
- **Depends on:** C04-C08, C18. **Reference:** F's `scan_fragment.rs` and expression-safety tests.
- **Gate:** computed projection plus residual filtering, volatile/config-sensitive cases, local/remote
  mixtures, full optimizer sequence, and accepted/declined output equivalence.

#### C20 — `[query] Push eligible partial aggregates with state reduction`

- **Change:** localize supported partial aggregates plus their safe inputs and leave the appropriate
  state-reduction/final stages upstream. Retain F's checks on order, distinctness, grouping sets,
  limits, expression safety, and accumulator support. Do not describe `PartialReduce` as global grouping.
- **Touchpoints:** partial-aggregation rule and coordinator finishing-plan construction.
- **Depends on:** C19. **Reference:** F's `partial_aggregation.rs`.
- **Gate:** decoded remote fragments execute and merge real aggregate states, including AVG/STDDEV,
  aggregate FILTER, empty inputs, multiple storage partitions per lane, and coordinator fallback.

#### C21 — `[query] Preserve ordering across source planning and remote lowering`

- **Change:** implement requested-order negotiation and truthful per-stream properties through scan,
  projection, fragment boundary, and fallback. Distinguish exact, inexact, and unsupported order.
  Keep separate sorted runs or merge them; never concatenate overlapping ranges and claim sortedness.
  Use existing primary ordering only where proved; exercise alternative orderings with a test source.
- **Touchpoints:** source properties, `try_pushdown_sort`, lane planning, remote wrapper/fallback.
- **Depends on:** C18-C20. **Reference:** [R5] and DataFusion's sort-pushdown contract.
- **Gate:** mixed local/remote filtered ORDER BY passes the full optimizer and sanity check; sorting
  is removed only for exact order. Unsupported ordering retains a correct sort, including on fallback.

#### C22 — `[query] Execute partition-local sort and Top-K subtrees`

- **Change:** permit supported local Sort/Top-K and qualified limits in fragments, keeping the global
  merge/Top-K/limit. Apply K/O+K only at the correct logical row stage. Preserve comparator and NULL
  semantics; exclude unsupported WITH TIES and cardinality-changing rewrites. Establish correctness
  without requiring new cross-boundary dynamic feedback; C23 adds that optimization. If an existing
  mutable binding cannot be preserved yet, leave that plan on the already-correct execution path.
- **Touchpoints:** localization rule, fragment allowlist, coordinator finishing operators.
- **Depends on:** C21. **Reference:** architecture section 3.6.
- **Gate:** ordered and unordered primary inputs, duplicate/tied keys, OFFSET, residual rejection,
  several partitions/nodes, and full remote decode execution produce the reference result. Resource
  use is charged to the Worker runtime and cancellation is tested through the assumed notification.

#### C23 — `[query] Route dynamic filters to active subtree inputs`

- **Change:** unify runtime predicate slots and generation tracking across physical rewrites and
  remote boundaries. Preserve main's Next piggybacking; add independent predicate update delivery
  for a subtree busy producing one output batch. Updates target the correct source fields/stage.
  Worker-local Top-K can feed its own scan without a network round trip.
- **Touchpoints:** dynamic-filter binding, source control interfaces, remote scanner/subtree loop.
- **Depends on:** C19-C22. **Reference:** [B6] [R6].
- **Gate:** updates take effect while a Next is pending; stale snapshots remain conservative; queries
  progress without receiving updates. **Checkpoint B** replaces F's functionality and adds the
  ordered/Top-K foundation without depending on secondary indexes.

### Wave D: index-ready storage access primitives

#### C24 — `[storage] Extract reusable mem-comparable field encoding`

- **Change:** extract I's generic mem-comparable string support and primitive field codecs as a
  reusable library. Preserve documented encoded bytes. Separate generic fields from concrete
  index/stat headers and ID adapters. Do not change existing primary-key encodings.
- **Touchpoints:** `util/string`, generic ordered-field helpers in partition-store.
- **Depends on:** main utility APIs; can be reviewed alongside earlier waves. **Reference:** [R7].
- **Gate:** comparison/round-trip and prefix-bound tests cover empty, NULL where supported, embedded
  NUL, Unicode, group boundaries, and descending primitive encodings. No production index prefix is allocated.

#### C25 — `[storage] Prepare typed constraints against ordered key schemas`

- **Change:** extract `PreparedIndexPredicate`, interval unions, prepared fields, and schema binding
  from I. Prepare literals once and retain exact finite sets. Parameterize fixed scan identity and
  field codecs rather than depending on `IndexId`, new entry identity types, or stats macros.
  Connect the C04 logical-domain binding to this compiler.
- **Touchpoints:** storage-api typed adapters and partition-store generic key-filter modules.
- **Depends on:** C04, C24. **Reference:** [R2] [R7].
- **Gate:** encoded matches agree with logical conditions; absent-field bindings error rather than
  silently disappear; timestamp adapters are tested only where their exact SQL conversion is supported.

#### C26 — `[storage] Execute prepared bounds and forward seeks`

- **Change:** extract the static `KeyFilterCursor` and controlled iterator adapter. Keep the caller's
  physical scope/read view fixed, preserve range/prefix mode, and validate strictly advancing seeks.
  Reuse C16 metrics. No query-specific navigation logic belongs in a concrete index scanner.
- **Touchpoints:** prepared cursor, `partition_store.rs` iterator adapter, RocksDB iterator interface.
- **Depends on:** C09-C11, C16, C25. **Reference:** [R6] [R7].
- **Gate:** real RocksDB tests in an isolated fixture keyspace verify finite-set gaps, parent-prefix
  carry, bounds, errors, and keys/seeks. Test fixtures do not add a production index or require I's schema.

#### C27 — `[query] Bind live predicates to prepared storage cursors`

- **Change:** adapt I's `LivePredicate`/`LiveFilter` split to C23 slots and C25-C26 compilation.
  Preserve static access constraints, poll generations cheaply, and compile changed snapshots only.
  Apply sound live predicates before materialization; allow safe seeks within the static scope.
- **Touchpoints:** query predicate adapter and generic storage cursor.
- **Depends on:** C23, C25-C26. **Reference:** [R6].
- **Gate:** reproduce the multi-parent live Top-K seek regression without a production VQueue index;
  assert surviving candidates and actual visited-key reduction. Unsupported live conditions fall
  back to later filtering and do not weaken the read-view or static-scope contract.

#### C28 — `[query] Compose ordered primary lookup and covering paths`

- **Change:** add the explicit unary primary-lookup operator over C11, preserving input multiplicity
  and order with bounded batches. Carry native locator and covered fields, without formatting IDs
  for internal lookup. Add coverage analysis to eliminate lookup when all required fields are available.
  Keep identity-specific conversion in source adapters so I can later add canonical locators.
- **Touchpoints:** lookup physical node/codec, source field mappings, projection/residual planning.
- **Depends on:** C03-C04, C11-C12, C17-C23. **Reference:** [B8] [R8].
- **Gate:** feed candidate IDs from a conformance source into real existing primary-table lookups;
  index-only fixtures do zero primary reads; non-covering paths survive concurrent writes, preserve
  order/duplicates, and filter before qualified limits. Remote codec execution uses the same view.

#### C29 — `[query] Register and select alternative access paths`

- **Change:** expose the final source-level access-path registration contract: population proof,
  readiness requirement, logical field mapping, bounds/filter capabilities, covering fields, order,
  locator/lookup binding, metrics, and equivalent fallback. Select deterministically among eligible
  candidates; use requested sort order through C21. Existing tables register their primary paths.
  Exercise alternative indexes through conformance fixtures only.
- **Touchpoints:** source registry, access selection and order negotiation, custom leaf descriptors.
- **Depends on:** C03-C08, C21, C25-C28. **Reference:** architecture sections 3.1-3.6.
- **Gate:** ready/applicable fixtures can be selected; unknown/incomplete/population-incompatible
  paths are never authoritative. Broad index intervals retain exact predicates. The selected
  path is visible in EXPLAIN and is revalidated at Worker binding before execution.

#### C30 — `[query] Verify the infrastructure handoff and protocol isolation`

- **Change:** complete the C00 harness's end-to-end conformance matrix for an external access path;
  finish documentation and remove new-engine migration scaffolding. Keep intentional legacy support
  for C31. Audit source registration against the full manifest and version boundary. This is a finite gate,
  not a catch-all commit for unfinished features from C01-C29.
- **Touchpoints:** integration tests, docs, obsolete compatibility facades/internal hooks.
- **Depends on:** C00-C29.
- **Gate:** cover primary, covering fixture, non-covering lookup, filter/range/finite-set selection,
  exact-order/local Top-K, live seeks, remote acceptance/fallback, same-partition cross-scan snapshots,
  and cleanup notification. Validate work counters as well as rows. **Checkpoint C:** the concrete
  index stack needs declarations/codecs/readers and maintenance, not new query routing machinery.

### Deferred protocol cleanup

#### C31 — `[query] Remove legacy execution after protocol retirement`

- **Change:** once the release policy retires the old network protocol range, raise the supported
  minimum as part of that coordinated retirement and delete the legacy query message family,
  handlers, providers/executors, extraction/routing loops, sentinel adaptation, and mode branches.
  Preserve protocol numeric reservations; do not reuse old message identities. Other services'
  protocol-retirement work follows the repository-wide release policy.
- **Touchpoints:** network version policy plus C15's explicit legacy deletion inventory.
- **Depends on:** new-path production adoption, no supported Coordinator requiring legacy query
  execution, and completion/termination of in-flight old-protocol work. It is not gated on deploying
  secondary indexes and is not a prerequisite for the index overlay.
- **Gate:** supported-version handshake tests exclude the old range; new queries use only the
  common engine; removed symbols have no callers; historical fixtures remain where useful to
  document compatibility. Include release notes stating the upgrade/deprecation requirement.

## 5. Review and activation rules

### Preserve a compiling chain

- C00 precedes engine changes. Correctness uses independent logical-data and broad-primary references,
  not merely legacy-versus-new equality. Compare bags with multiplicity and ordered results using
  the query's tie/rank contract; consume the complete stream and final status. Check that the
  intended index/fragment path actually ran. [Query correctness harness](query-correctness-harness.md)
- Each API-changing commit updates every caller in the same patch. Use temporary delegation adapters
  only when they preserve the old contract; distinguish new-engine scaffolding (removed by C30)
  from protocol compatibility (removed at C31).
- Pure codec/normalization commits carry meaningful direct tests. Execution commits connect to an
  existing source or a conformance fixture; do not land unused framework stubs expecting later patches
  to supply correctness.
- Do not mix storage-format changes, identity migrations, and query-planning rewrites in one commit.
- C01-C13 build/test new infrastructure beside legacy behavior. C14's release note explains
  network-version selection, new consistency, and legacy-mode semantics during rolling upgrade.
  Do not infer support from binary-version strings, optional-field presence, or ownership replies.
- After C15, new query/index features extend only the common engine. Legacy stays behaviorally
  frozen except for necessary compatible fixes; its removal is tied to C31's protocol gate.
- Requests and responses are both version-gated. A new binary can still have a connection negotiated
  on an old version. Revalidate on reconnect and never switch a running new query to the old path.
- Exact filter/order claims, limits, and fragment capability are enabled only with their associated
  proof tests. Unknown capability retains a semantically correct fallback or produces an explicit error.
- Property correctness is required from the first physical rewrite. C21 adds requested-order
  optimization; it is not permission for C08-C20 to invalidate existing ordering. Earlier rules must
  preserve their root contract, re-enforce affected requirements, or decline the rewrite.

### Per-commit review template

Every implementation PR/commit description should answer:

1. What one contract or behavior changes?
2. Which existing production path or conformance fixture consumes it now?
3. What moved from main/F/I, and what was deliberately redesigned?
4. Which new-engine shim is removed now/by C30, and which legacy code is retained explicitly for C31?
5. What demonstrates semantic equivalence and, for optimizations, less actual work?
6. Does it change persisted bytes, wire compatibility, query consistency, or public behavior?
7. Which differential harness cases exercise the production path, and how can their seeds be replayed?

Before committing code, follow the workspace's checks: `cargo check`,
`cargo nextest run --all-features`, `cargo fmt --all -- --check`, and
`cargo clippy --all-features --all-targets --workspace -- -D warnings` for Rust changes.
For dependency changes, also regenerate workspace-hack with `cargo hakari generate` and run
`cargo deny --all-features check`. Run focused tests while developing, then the required checks;
these commands are future gates, not results of writing this plan.

### Boundaries requiring an explicit choice before implementation

- C01: register the agreed `qry` resource kind and reuse ULID-backed `ResourceId` generation and
  encoding; representation is settled. C13 decides its wire placement, separate from child ScannerId.
- C03-C04: field identity and logical/physical-expression adapter contract; keep original residuals.
- C09/C12: safe snapshot ownership across storage tasks and parent/child lifetime, including lazy children.
- C13: next unused network version, deployed experimental variants, new message identities, and
  failure categories. Existing negotiation is the agreed mechanism; do not add a competing handshake.
- C14: finalize the proposed query-wide mixed-version selection policy and which requests may
  use legacy semantics versus require new guarantees. Decide this before data consumption.
- C17-C18: portable expression set and semantic context, plus contract-preserving fallback.
- C25/C29: generic field schema and access-path API boundary exposed to the index overlay.

These are localized design reviews within named commits. They do not reopen the agreed roles,
QueryId identity, per-query/per-partition consistency, or timeout/keepalive scope exclusion.

## 6. How the secondary-index stack is laid on top

### Extraction ledger

| Work from F or I | Infrastructure disposition | Follow-on index disposition |
| --- | --- | --- |
| F placement nodes/rules | Reimplement in the new C07-C08/C14-C15 path; preserve useful tests and isolate legacy routing until C31. | No new routing layer. |
| F fragment serialization/execution | Rework around real source leaves and Worker contexts in C17-C18. | Register supported custom access descriptors/codecs. |
| F filter/projection and partial aggregation | Adapt semantic restrictions/tests in C19-C20. | Use unchanged localization rules where contracts match. |
| F client/server protocol changes | Superseded by C13's unified contract. | Do not reapply old Open tags or acknowledgements. |
| I `storage-api/filter*` and `filter/typed.rs` | Extract reusable semantics into C04-C06/C27; reconcile with main's ID/node analysis. | Add concrete table field mappings and value conversions only. |
| I utility mem-comparable strings | Reuse compatible library work in C24. | Do not duplicate codec types. |
| I `keys/predicate.rs`, generic `keys/filter*`, field-schema macros | Extract the independent compiler/cursor in C25-C27. | Supply concrete index key declarations and adapters. |
| I `keys/index.rs` | Split: generic field/payload traits move to infrastructure; the file also contains `IndexKeyPrefix` tied to `KeyKind::SecondaryIndex` and `IndexId`. [R7] | Keep concrete persisted headers/identities in the overlay. |
| I iterator control and metrics | Integrate into read-view-aware C16/C26. | Consume the common callbacks/counters. |
| I `scan_metrics.rs` and metrics wire fields | Adapt in C16 to the shared query/child identity and versioned envelope. | No separate metrics protocol. |
| I base/canonical entry IDs and lifecycle changes | No dependency in C01-C30; generic locator adapters remain extensible. | Rebase with their own API/storage compatibility tests before dependent indexes. |
| I concrete `index/*`, maintenance, feature flags, backfill, covering values | Descriptor contracts only; no new index writes or readiness certification in infrastructure. | Retain and adapt here. |
| I `_idx_*` and stats SQL registration | Common source interface only; no new product tables in infrastructure. | Register through it, preserving index-inspection and aggregate-view semantics. |
| F/I query and iterator regressions | Reuse their data and assertions as C00-harness scenarios; retain focused unit tests. | Add native layout/maintenance adapters to the same case API, not a separate result checker. |

I's `keys/index_key_codec.rs` also mixes primitive codecs with concrete canonical IDs and entry
types. Split those dependencies; importing an entire module would accidentally pull the identity
migration into the infrastructure. Preserve the existing encoding contract when implementing the
overlay adapters. [R7] [R8]

### Suggested follow-on order

| Overlay step | Change | Dependency/gate |
| --- | --- | --- |
| **I-A** | Rebase base/canonical identity and entry-lifecycle prerequisites actually needed by the selected indexes. | Compile independently against main plus infrastructure; preserve public/storage identity semantics. |
| **I-B** | Add concrete index IDs, physical prefix/layout declarations, and SQL/native codec mappings. | Reuse C24-C27; no duplicated prepared-predicate engine. |
| **I-C** | Add transactional membership maintenance and covering-value updates. | Primary and index changes share one commit; preserve `SingleDelete` lifetime requirements. |
| **I-D** | Register optional index-inspection sources using the shared declarations and access nodes. | They describe persisted contents honestly, without implying complete base-table coverage. |
| **I-E** | Certify concrete SQL orderings and bind runtime filters/lookup locators. | Test HLC-to-millisecond ties, ID display order, NULL placement, finite leading prefixes, and snapshot-coherent lookup. |
| **I-F** | Implement backfill/rebuild and persistent query-readiness publication. | Handle concurrent writes, old stores, split/restore, and population applicability. |
| **I-G** | Enable one proven base-table index alternative, then expand selectively. | C29 eligibility plus I-F readiness; primary/index/covering/remote paths agree and metrics show a benefit. |

Preserve the old stack's seven concrete index definitions as candidates, not an obligation to enable
every access path simultaneously. A busy-queue covering index may need its statistics/lifecycle
dependencies; those are overlay work. Index-specific features are declared once and consumed by the
common planner, codec, Worker binder, ordered lookup, and diagnostics paths.

### Handoff contract checklist

Before rebasing I, C30 must demonstrate that an access-path implementer can supply:

- Logical source/field mapping and row-population applicability.
- A partition-bound primary or secondary access descriptor, plus capability/readiness validation.
- Ordered field codecs and fixed-prefix binding without changing the generic predicate engine.
- Covered fields and a native locator-to-primary-reader adapter under the shared read view.
- Proven order or an inexact/unsupported response; finite-prefix runs and merge behavior as needed.
- Metrics hooks and supported dynamic-filter bindings.

That implementer must not need to write new SQL predicate visitors, choose nodes, schedule fanout,
invent a new remote scanner protocol, manage another query lifetime, or implement primary-row
materialization outside the common lookup contract.

The overlay must also supply fixture population/maintenance adapters to the existing correctness
harness. Its index-only, non-covering, sorted, live-pruned, remote, and declined paths must match
the same canonical and broad-primary references before authoritative substitution is enabled.

## 7. Optional follow-ups after the infrastructure cut

These are not prerequisites for layering the secondary indexes:

1. **Partial partition results:** split into request-local setting handling; classified failure policy;
   terminal completion metadata per API; client presentation and optimizer eligibility. Keep default-off
   semantics from architecture section 5.4. Existing node-table warnings are preserved earlier.
2. **Worker-local multi-partition plans:** extend scoped leaf binding and merge/reduction within one
   Worker task. QueryId and snapshot ownership already exist; this is additional plan composition.
3. **Colocated joins:** introduce multi-input fragment validation and proven join-locality rules.
   General cross-worker shuffle remains a separate design.

The cleanup transport assumption continues to apply throughout. No timeout, lease-refresh, or
keepalive policy is introduced as a hidden prerequisite in this sequence.

C31 protocol retirement is an independent release milestone. New infrastructure and indexes may
ship while the negotiated old-protocol path is still retained.

## 8. Source references

**B** sources are at main revision `a1975009e3ef98d2876dca4b21e8ef0d821fcbec`. **R** sources refer to
the two experimental stacks already identified in the architecture document. Use
`sl cat -r <revision> <path>` to inspect them without changing the working copy.

- **[B1]** B: `Cargo.toml:149-158`; `crates/storage-query-datafusion/src/context.rs:619-668` — DataFusion version and baseline query execution.
- **[B2]** B: `crates/storage-query-datafusion/src/table_providers.rs:45-130,146-287,290-395`; `crates/storage-query-datafusion/src/filter.rs:35-67,85-275` — raw scan interface, selection, execution lanes and plan.
- **[B3]** B: `crates/storage-query-datafusion/src/remote_query_scanner_manager.rs:76-120,190-208,238-318` — execution-time target selection and local/remote scanner.
- **[B4]** B: `crates/storage-query-datafusion/src/node_fan_out.rs:69-222,271-324,420-488,573-590`; `crates/storage-query-datafusion/src/table_providers.rs:493-610`; `crates/storage-query-datafusion/src/context.rs:681-705`; `tools/restate-doctor/src/commands/snapshot/mod.rs:140-148,328-335` — separate node/generic paths, warnings, offline selection.
- **[B5]** B: `crates/types/src/net/remote_query_scanner.rs:24-119` — baseline wire fields and responses.
- **[B6]** B: `crates/storage-query-datafusion/src/remote_query_scanner_client.rs:151-257`; `crates/storage-query-datafusion/src/scanner_task.rs:74-97,131-181` — bounded raw streaming and mutable predicate updates.
- **[B7]** B: `crates/storage-query-datafusion/src/partition_store_scanner.rs:31-61,88-137`; `crates/partition-store/src/partition_store.rs:523-624,980-1013`; `crates/storage-api/src/lib.rs:81-112` — query scan binding and existing transaction snapshots.
- **[B8]** B: `crates/rocksdb/src/lib.rs:345-430`; `crates/partition-store/src/vqueue_table/mod.rs:563-662`; `crates/partition-store/src/invocation_status_table/mod.rs:88,270` — background reads and existing primary lookup consumers.
- **[B9]** B: `crates/node/src/lib.rs:351-422` — role-independent scanner registration and minimal remote runtime context.
- **[B10]** B: `crates/partition-store/src/keys.rs:29-86`; `util/string/src/lib.rs:11-19`; `crates/storage-api/src/lib.rs:65-79` — baseline persisted kinds and module exports.
- **[B11]** B: `crates/types/src/identifiers.rs:1059-1186`; `crates/types/src/id_util.rs:26-63` — reusable ULID-backed resource-ID macro and versioned prefix/encoding registry.
- **[B12]** B: `crates/types/protobuf/restate/common.proto:14-28`; `crates/types/src/net/mod.rs:24-28,97-138`; `crates/core/src/network/handshake.rs:59-84`; `crates/core/src/network/connection.rs:368-375`; `crates/core/src/network/incoming.rs:37-59,745-759` — supported range, negotiated-version access, generic codec minimum, and reply-encoding contract.
- **[R1]** I: `crates/types/src/net/remote_query_scanner.rs:60-63`; F: same path `60-75` — independent tag-9 allocations.
- **[R2]** I: `crates/storage-api/src/filter.rs:28-168`; `crates/storage-query-datafusion/src/filter/typed.rs:34-229`; `crates/storage-api/src/filter/macros.rs:11-23`; `crates/storage-api/src/table.rs:11-57` — reusable typed semantics, table-marker dependency, and separable canonical-ID implementations.
- **[R3]** F: `crates/storage-query-datafusion/src/partitioned_scan.rs:82-164,500-582`; `crates/storage-query-datafusion/src/remote_fragment.rs:124-195,425-462,492-578` — placement boundary, unary template, codec, binding and fallback foundations.
- **[R4]** I: `crates/storage-query-datafusion/src/scan_metrics.rs:29-147`; `crates/types/src/net/remote_query_scanner.rs:96-168` — iterator/query metrics and cumulative remote accounting.
- **[R5]** F: `crates/storage-query-datafusion/src/partitioned_scan.rs:531-554`; `crates/storage-query-datafusion/src/context.rs:682-698,752-768`; [DataFusion 55.1.0 sort pushdown](https://docs.rs/datafusion-physical-plan/55.1.0/datafusion_physical_plan/execution_plan/trait.ExecutionPlan.html#method.try_pushdown_sort) — property-sensitive rewrite placement.
- **[R6]** I: `crates/storage-query-datafusion/src/filter/typed.rs:50-135`; `crates/storage-api/src/filter.rs:124-132`; `crates/partition-store/src/index/scan.rs:39-41,180-218`; `crates/partition-store/src/keys/filter.rs:520-609`; `crates/storage-query-datafusion/src/index/tests.rs:1153-1264` — live filtering and actual seek savings.
- **[R7]** I: `crates/partition-store/src/keys/index.rs:19-62,138-147`; `crates/partition-store/src/keys/filter.rs:16-44,322-495`; `crates/partition-store/src/keys/predicate.rs:17-87,171-230`; `docs/dev/ordered-key-filtering.md:54-69,177-195` — generic primitives versus persisted/index-specific dependencies.
- **[R8]** I: `crates/partition-store/src/keys/index_key_codec.rs:50-74,141-160`; `crates/partition-store/src/vqueue_table/mod.rs:771-850`; `crates/partition-store/src/index/entry.rs:22-84` — concrete locator and timestamp codecs, lookup ordering, and index layouts.
