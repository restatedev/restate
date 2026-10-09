# Query selection, routing, access paths, and remote fragments

> Historical design retained for access-path requirements and source evidence.
> Its execution sequence and proposed query-identity model are historical proposals.

## Status and direction

**Proposed design and implementation plan.** This document combines the storage/index work ending
at `ef8eca39d82a` with the remote-fragment work at `28e937d70cb0`. It describes a destination and
incremental delivery gates, not capabilities already implemented by either stack.

The implementation starts fresh from main. The authoritative commit sequence and boundary for
reapplying the concrete index work are in
[Query infrastructure: incremental execution plan from main](query-infrastructure-execution-plan.md).
F and I provide evidence and reusable mechanisms; neither is a prerequisite branch to merge.

Roll out the new execution infrastructure behind a new negotiated **network protocol version**.
Keep the existing query implementation and its wire behavior available for the legacy protocol
range until that range is deprecated. The new engine unifies selection/routing internally;
temporary protocol-gated coexistence has an explicit retirement gate.

The central decision is to make **access-path decisions visible in physical plans** and execute
the resulting storage-local subtree at the partition owner. RocksDB iteration, encoding, seek
mechanics, and lookup batching remain implementation details behind those nodes.

**Consolidation is a delivery requirement:** partition-key/ID extraction, node selection, owner
resolution, fanout planning, and local/remote execution must become one query-planning pipeline.
The index path must not become another parallel selection/routing mechanism. Source-specific
semantics remain explicit declarations and adapters; duplicated control flow is retired.

This consolidation applies to all sources in the new engine. The intentionally retained legacy
engine is frozen compatibility code, removed with its protocol support rather than during rollout.

The first useful destination is:

```text
Coordinator: final aggregation / global Top-K / ordered merge / remaining query
    |
    +-- local or remote partition-local subtree
          Projection
            Exact residual filter, if needed
              Primary-row lookup, if needed
                Index-covered filter, if needed
                  Secondary-index scan
```

The labels above describe proposed operator responsibilities; they are not new Rust API names.
Use existing DataFusion operators for filters, projections, sorts, merges, limits, and aggregates
where their contracts fit. Add custom nodes for storage access and the execution boundary.

Filtering, sorting/Top-K, index-only reads, and delayed primary lookups are core goals. Worker-local
multi-partition subtrees and colocated joins are later extensions of the same model. Cross-worker
shuffle, general distributed joins, and cluster-wide snapshot isolation are separate projects.

For a short reading path, start with [what to carry forward](#1-what-to-carry-forward),
the [architectural boundaries](#2-architectural-boundaries), and the
[implementation milestones](#7-implementation-milestones). The remaining sections define the
contracts each milestone must preserve.

### Terminology and agreed lifecycle boundary

These names describe **per-query execution roles**, not deployment roles or additional services:

| Term | Definition |
| --- | --- |
| **Query Coordinator (Coordinator)** | The execution role that accepts the query, owns its `QueryId` and overall plan, assigns work, runs the remaining/global operators, and returns results and completion information to the caller. |
| **Query Worker (Worker)** | The execution role on a participating Restate node that binds and executes assigned subtrees, owns their query-local resources and partition read views, and streams results/diagnostics to the Coordinator. |
| **Restate node** | The physical process hosting either or both execution roles. A node may coordinate one query and work on another concurrently. |
| **Worker query context** | The parent resource/lifetime scope for one `QueryId` on one Query Worker. It owns partition read contexts and the child subtree/stream executions. |
| **Partition read context** | The shared storage view for one `(QueryId, PartitionId)` on its assigned Query Worker. All storage scans and primary lookups for that query and partition use it. |
| **`QueryId`** | The agreed new ULID-backed `ResourceId` with resource prefix `qry`, identifying one query execution and propagated unchanged for correlation and attribution. It is separate from child scanner/stream identities and partition identities. |

The unqualified words coordinator and worker below mean Query Coordinator and Query Worker.
An explicit **Restate Worker role** refers instead to deployment capability. A log-server node
serving an introspection subtree can act as a Query Worker; a Coordinator executing a local
subtree also fulfills the Query Worker responsibility without a network round trip. Source
placement still respects the actual deployment roles and data availability. [U5]

For this design, one `QueryId` names one execution. There is no additional "query attempt" identity
and no automatic restart with fresh snapshots. The Coordinator can later associate `QueryId` with
the initiating user; downstream execution preserves the ID in context and diagnostics without
requiring user-attribution policy to be designed here. The Coordinator allocates it using the
existing ULID-backed resource-ID machinery. The conceptual form is `qry_<ulid>`; under the current
V1 resource-ID schema, the actual text is `qry_1<base62-encoded ULID>`, including the schema version.
Use the shared parser, formatter, serde, and binary ID conventions rather than a query-specific
string codec. The new resource-kind registration and protocol plumbing are C01/C13 work. [Q4]

**Assumed cleanup contract:** when the Coordinator drops its side of an execution stream, the
Query Worker is notified to cancel and clean up the associated subtree. Local execution uses the
equivalent stream-drop notification. Whole-query cancellation drops/notifies all assigned work.
This is an infrastructure assumption, not a claim that F's current Close path already guarantees it.

Timeout values, absolute deadlines, inactivity expiry, keepalives, lease renewal on Open/Next,
and the mechanism that detects/delivers stream loss are **out of scope** and will be designed
separately. This document specifies how the Worker cleans up once notified; no milestone depends
on choosing a timeout or keepalive policy.

### Evidence and revision notation

Sources marked **I** refer to the index stack at `ef8eca39d82a679b266936d62c59fbf19c504322`.
Sources marked **F** refer to the fragment stack at `28e937d70cb0`, whose parent is the
client-minted-scanner-ID change `9712dabd7925`. Historical paths may not exist in the current
checkout; retrieve them with `sl cat -r <revision> <path>`. Source references at the end give
revision-specific line numbers. The implementation guidance targets DataFusion **55.1.0**,
as selected in `Cargo.toml:149-158`, with versioned upstream API references.

Working-copy edits present during this investigation are not treated as landed behavior.
This is a synthesis of the two approaches, not a mechanical prescription to merge both stacks.

## 1. What to carry forward

| Building block | Existing evidence | Role in the combined design |
| --- | --- | --- |
| Storage-neutral typed conditions | I: `FilterTarget`, `Filter<T>`, `ValuePredicate` | Keep SQL normalization separate from key encodings and access strategy. [I1] |
| Prepared encoded predicates | I: `PreparedIndexPredicate`, `PreparedKeyFilter` | Encode literals once; evaluate borrowed key fields without materializing rows. [I2] |
| Safe key navigation | I: `KeyFilterCursor` | Derive bounds and seeks from the same constraints; retain fixed physical identity. [I3] |
| Runtime filtering | I: `LivePredicate`, `LiveFilter`, `IndexFilter` | Carry sound snapshots to the storage cursor, before primary fetch/materialization. [I4] |
| Secondary-index layouts and maintenance | I: seven index identities, primary-key suffixes, covering values | Reuse persisted layouts and lifecycle writes; add explicit query eligibility. [I5] |
| Iterator and remote metrics | I: `ScanMetrics`, `ScannerMetrics` | Verify reduced storage work, not just reduced output rows. [I6] |
| Explicit placement | F: `LocationAwareScanExec`, `RemoteNodeExec` | Record ownership and identify where computation may execute. [F1] |
| Reusable physical fragments | F: `RemoteFragment`, custom leaf codec, coordinator fallback | Extend from a unary template over a raw scan to a storage access subtree. [F2] |
| Relational pushdown | F: filter/projection and partial-aggregate rules | Retain semantic eligibility checks and coordinator finishing stages. [F3] |

The index stack's earlier design already separates query normalization, key preparation,
candidate navigation, and table-specific materialization/primary lookup. Its implementation guide
and source are more current than some proposed steps in that earlier design. In particular,
live seek-assisted filtering is implemented at the stack tip, despite the earlier document
deferring it. [I3] [I4] [I7]

### Integration seams that must be resolved deliberately

1. **Protocol collision:** both stacks allocate `RemoteQueryScannerOpen` tag 9. I uses it for
   `collect_metrics`; F uses it for `expected_partition_owner` and tag 10 for `fragment`.
   Keep main's legacy messages unchanged and define a new version-gated execution message family.
   Check which experimental variants have been deployed; do not reinterpret their tags. Add old
   protocol fixtures and explicit request/response version-gate tests before integration. [W1] [P1]
2. **Static versus live predicates:** I passes the initial `access_predicate` separately from
   the mutable row predicate, so a remote update wrapper does not hide static constraints.
   Preserve this semantic distinction instead of concatenating extra expression parameters
   indefinitely. F's fragment input/output schemas introduce another boundary to map explicitly. [I8] [F2]
3. **Ordering:** I deliberately registers index SQL tables with no ordering guarantee. F
   clears remote-fragment ordering and installs fragment rewriting near the end of physical
   optimization. Ordered index access needs an end-to-end ordering contract. [I9] [F4]
4. **Completeness:** index maintenance is not backfill. `IndexesV1Feature::enable` logs that
   backfill is unimplemented; the inspection tables are explicitly not an authoritative inventory
   of old entries. Those indexes cannot silently replace a complete base-table scan. [I10]
5. **Lookup identity:** indexes use `CanonicalEntryId`, but the entry-status multi-get path
   uses `BaseEntryId` and orders requests as a set. Reusing it directly would lose incarnation
   identity and index order unless an adapter preserves and validates both. [I11]
6. **Metrics and lifecycle:** combine I's per-scanner cumulative metrics with F's fragment
    acceptance and active cancellation. Do not regress either while changing the execution unit.
    F currently uses lossy registration and one-shot Close delivery. This design consumes the
    stream-drop cleanup assumption above; its transport delivery mechanism is separate work. [I6] [F5]

## 2. Architectural boundaries

### 2.1 Query planning owns semantics and access selection

Keep the existing SQL table as the logical source. An index is an alternative physical access path
for that table, not automatically a new logical relation. The `_idx_*` tables can remain inspection
surfaces, but their existence does not establish base-table equivalence. [I9] [I10]

The planner should determine:

- The selected Restate partitions and their planned placement.
- Required output columns and additional columns needed by filters, ordering, grouping, or joins.
- Candidate access paths, including primary scans and complete applicable secondary indexes.
- Which predicates supply bounds, which run over index entries, and which require primary values.
- Whether primary lookup is necessary and whether it preserves order.
- Requested versus guaranteed output ordering, with direction and NULL placement.
- Where limits and partial computation can safely run.

DataFusion does not need to understand RocksDB key bytes. It needs truthful plan properties and
operators that respond to filter and sort requests. Use `TableProvider` for the logical table and
custom `ExecutionPlan` nodes for access choices. `try_pushdown_sort` supports exact, inexact, and
unsupported ordering responses in 55.1.0. [D1] [D2]

Start access-path selection with deterministic rules: prove eligibility first, then compare
prefix/range applicability, covering columns, useful order, and estimated lookup work. Retain the
primary-scan candidate. Unknown selectivity must not become an exact cardinality claim. A richer
cost model can follow measurements; narrow indexes with many random primary lookups are not
universally cheaper than sequential primary scans.

### 2.2 Storage owns physical compilation and execution

Retain the index stack's layering:

```text
DataFusion expressions and column lineage
    -> typed logical field constraints
    -> bind to the chosen index's schema/codecs
    -> PreparedKeyFilter + PreparedIndexPredicate / PreparedIndexRanges
    -> per-iterator KeyFilterCursor and physical bounds
    -> candidates and optional primary lookup
```

SQL types, NULL semantics, expression meaning, and unsupported-expression handling belong in the
query adapter. Physical field order, encodings, fixed prefixes, and valid seek targets belong in
partition-store. Native storage callers can continue supplying `Filter<T>` without DataFusion.
[I1] [I2] [I3] [I4]

Represent index selection, scan constraints, and lookup stages in the plan, but compile physical
prefixes and prepared byte predicates on the owning worker. Do not serialize iterators, borrowed
key views, snapshots, or `PreparedKeyFilter` buffers as the cross-version plan contract.

### 2.3 Placement and transport execute a completed local subtree

The coordinator chooses the semantic access plan. The worker validates capabilities and binds
partition-local resources. Worker-specific batching and physical seeks may vary, but execution
must satisfy the advertised schema, ordering, distribution, and row semantics.

The first storage remote unit remains **one selected storage-partition range**. Node-scoped sources
instead select one node generation, and coordinator-local sources select one local scope. All use
the same execution framework, with typed scope-specific resource binding. Local execution skips
serialization; remote execution uses a versioned codec and resource-binding context.

The Worker query context owns validated partition handles and shared read views. Each child unit
binds the applicable runtime-filter slots, memory budget, cancellation, metrics, and completeness
reporting. Independently opened subtrees with the same `QueryId` and partition share the same read
context; they must not independently create snapshots. Node-local and leader-live sources bind
their own handles rather than pretending to have a RocksDB snapshot. Custom leaf descriptors
identify the source/access path; live handles never cross the wire. The descriptor codec and
binding API are part of M2/M5.

The ownership shape is:

```text
Query Coordinator: QueryId Q
  Query Worker on a participating Restate node: context for Q
    Partition P1: shared read view S1
      index iterator / primary lookups / other table scans
    Partition P2: shared read view S2
      index iterator / primary lookups / other table scans
```

The parent acquires each partition view before its first child storage read and keeps it alive
until no remaining work for that query/partition can use it. A temporarily idle set of child
streams is not permission to recreate a newer view under the same `QueryId`. Child operations
can run concurrently on tasks/storage threads; the precise scoped-concurrency implementation is
to be chosen without holding the partition-processor loop for query execution.

This shared ownership applies even while individual dispatched fragments are single-partition.
M7 later combines multiple partition subtrees into one worker-local execution plan/task; it does
not introduce query identity or snapshot sharing for the first time.

### 2.4 One selection, placement, and fanout pipeline

The common pipeline is:

```text
Logical table + bound predicates + source declaration
    -> shared predicate normalization and typed domains
    -> source-scope selection and routing-key derivation
    -> query-owned topology/availability view
    -> concrete, disjoint work units with fixed targets
    -> access-path selection and placement-local physical subtrees
    -> shared execution-lane / fanout planning
    -> local binding or remote fragment execution
    -> common metrics, cancellation, and completeness reporting
```

This unifies query-side use of routing. It consumes the existing `PartitionRouting` authority
rather than implementing a second cluster routing system. That authority distinguishes a suitable
partition target from an observed leader/epoch; its `get_node_by_partition` can fall back to an
alive replica when no leader is known. A leader-live source must therefore declare its stronger
requirement instead of inferring leadership from any selected partition target. [U1]

#### Current mechanisms and their replacements

| Current mechanism | Evidence | Consolidated responsibility |
| --- | --- | --- |
| `FirstMatchingPartitionKeyExtractor`, `MatchingColumnExtractor`, `WhenNullExtractor` | F: `filter.rs:35-67,85-275,311-400` | Shared typed domain analysis plus declarative field-to-routing-key derivations, including conditional scope/service-key sharding. |
| `InList`, `InvocationIdFilter`, `VQueueEntryIdFilter`, `VQueueMetaFilter`, `VQueueFilter` | F: `filter.rs:403-701` | Normalize expressions once; reuse exact ID sets and finite domains for both routing and storage access. Native filter structs can remain binding targets, not independent expression parsers. |
| I's `Filter<T>` / `LivePredicate` SQL adapter | I: `filter/typed.rs:34-229` | Fold supported SQL interpretation into the same normalizer; retain typed target conversion, encoded compilation, and live-snapshot contracts. |
| `SelectPartitions` and `SelectPartitionsFromMetadata` | F: `context.rs:145-148,732-745` | Partition-universe adapter supplied to a query-owned selection context. |
| `PartitionLocator` and `MetadataAwarePartitionLocator` | F: `remote_query_scanner_manager.rs:73-117` | Shared placement resolver over the existing cluster routing authority; cache selected targets for this plan and validate at open. |
| `PointReadFanout`, the 4096-key branch in the provider, and `plan_partitions_by_location` | F: `filter.rs:53-67`; `table_providers.rs:236-283`; `partition_planning.rs:72-136` | One budgeted work planner separates logical key selection, physical access batching, and DataFusion execution lanes. |
| `extract_node_ids_from_filters`, `AllNodeLocator`, `RoleBasedNodeLocator` | F: `node_fan_out.rs:69-222` | Same domain analysis for node identity; declarative eligible-node/role policy and one topology resolver. |
| `NodeFanOutTableProvider` / `NodeFanOutExecutionPlan` | F: `node_fan_out.rs:224-489` | Thin source registration using the common planner and local/remote boundary; best-effort behavior becomes an explicit policy. |
| `ScanToScanPartitionAdapter` and node scans using `PartitionId::MIN` / `KeyRange::FULL` | F: `remote_query_scanner_manager.rs:202-209,276-294`; `node_fan_out.rs:458-472` | Tagged source scope in execution requests; no node-as-fake-partition convention in the new contract. |
| `GenericTableProvider` / `GenericExecutionPlan` | F: `table_providers.rs:353-475` | Coordinator-local source adapter in the common planning pipeline; keep metadata rows single-copy rather than fanning them out. |
| Repeated range acquisition/clamping in leader-state scanners | F: `invocation_state/table.rs:74-85,112-124`; `scheduler_status/table.rs:79-90,125-133`; `user_limits/table.rs:75-86,121-129` | Shared partition/leader scope binder passes a validated requested-range intersection to the source adapter. |
| Offline `LocalPartitions` / `AlwaysLocalPartitionLocator` and test selectors | F: `tools/restate-doctor/src/commands/snapshot/mod.rs:140-148`; `remote_query_scanner_manager.rs:120-170`; `mocks.rs:159-181` | Alternate topology/resource providers for the same pipeline, not a separate offline planner. |
| `collect_node_warnings` downcasts and node-specific error-catching streams | F: `context.rs:707-729`; `node_fan_out.rs:553-590` | Shared per-query completeness/diagnostics sink consumed by response adapters, independent of physical node types. |

Paths in the table are under `crates/storage-query-datafusion/src/` unless another root is given.
The inventory covers production query provider construction, selection traits and their workspace
implementations, plus index-stack consumers. It does not propose replacing ingestion, log routing,
or partition-controller mechanisms outside the query layer. Supporting source references are
[U1] [U2] [U3] [U4] [U5] [U6] [U7].

#### Source declarations replace bespoke routing code

Each logical source declares:

- **Scope:** partition storage, partition-leader live state, node-local state, or coordinator-local
  data. Offline execution supplies an alternate universe/resource adapter for eligible sources.
- **Field semantics:** stable identities, SQL-to-native conversion, and supported domain operations.
- **Routing derivations:** direct partition key, partition-bearing ID, scope/service-key hashing
  with explicit preconditions, or node identity constraints. Bind by table/column lineage, not by
  an unqualified column name that happens to match another input.
- **Availability and placement:** required role/capability, suitable partition target versus
  validated leader, local scanner availability, and index readiness where applicable.
- **Access capabilities:** exact ID lookup, range scan, secondary indexes, ordering, live filters,
  covering fields, and legal access batching. These capabilities do not change source scope.
- **Completeness policy:** required data or explicitly best-effort introspection, plus the source's
  consistency model. Shared error plumbing applies this policy rather than hiding it in one executor.

No new table should need its own equality/IN/OR visitor, metadata-based target loop, local/remote
branching, or fanout scheduler. Table-specific code supplies semantic derivations and row production.
Some sources can ignore storage-domain hints and rely on exact residual filtering; that is a
capability choice, not a reason to duplicate the planner.

#### Shared domain and work-selection rules

Logical-expression and physical-expression adapters feed the same domain reasoning and typed
conversions. "Normalize once" means reuse analysis for a bound expression revision, rather than
running a different parser for each consumer. Recompute or remap it after optimizer rewrites and
runtime-filter generation changes; never reuse stale column positions or predicate proofs.

1. **Distinguish unconstrained, empty, finite set, and range domains.** Empty selection yields
   `EmptyExec`/an empty stream without RPC or fake work. Unknown/unsupported analysis broadens
   selection and retains the residual; it never means empty.
2. **Combine constraints soundly.** Intersect compatible necessary constraints under AND; union
   alternatives under OR only when every arm is understood, or broaden safely. Preserve cross-field
   correlations or leave them residual. Do not stop at the first extractor and discard useful
   independent restrictions. Budget normalization to avoid unbounded expression expansion.
3. **Derive routing only from declared semantics.** An ID can supply both an exact primary locator
   and a partition key; parse it once and retain both. Hashing a field supplies only a necessary
   partition condition, not exact row equality. Hashes are not order-preserving: string ranges or
   exclusions cannot be turned into partition-key intervals/complements. Scoped versus unscoped
   routing requires the corresponding NULL/domain proof. [U2]
4. **Keep data selection separate from placement.** Secondary-key ranges do not identify storage
   partitions unless a routing derivation proves it. A node table's `plain_node_id` denotes a data
   source; it is not a request to run an arbitrary partitioned table on that node. Generational
   node predicates constrain the same identity domain without silently accepting another generation.
5. **Select work, then choose how to read it.** Several exact keys in one partition become one
   partition work description carrying the exact set. Its access planner may batch point reads,
   open disjoint ranges, or scan an envelope with residual membership checks. All three preserve
   the same selected rows; the grouping budget cannot create duplicates or lose finite-set holes.
6. **Intersect requested and available ranges once at binding.** For storage, also apply any
   locator-range validation required after split/import. A disjoint intersection is empty; never
   substitute the original request. Live-state sources receive the same scoped selection rather
   than repeatedly enumerating a whole partition for several point selections. [U3] [I4]
7. **Freeze placement for the query execution.** Capture partition/node universes in a query-owned
   context and memoize resolution using shared routing adapters. This is not an atomic cluster
   snapshot; record available topology versions and validate the source-specific contract at open.
   Runtime filters may reduce pending work, but they do not reroute an already executing subtree.
8. **Use one fanout/lane planner.** Budget target concurrency, prefix expansion, lookup batches,
   and buffering explicitly. Preserve each source's order and row semantics while assigning work;
   group by node as scheduling, not as implicit cross-partition aggregation. Deduplicate overlapping
   selections within one source scan, not across independent SQL references such as a self-join.

#### Preserve semantic differences without separate frameworks

| Source family | Common planning/execution | Source-specific contract |
| --- | --- | --- |
| Persisted partition tables and indexes | Typed selection, partition placement, access plan, remote lowering | Complete requested data by default; request-local partial-results opt-in as specified in section 5.4; partition-bound RocksDB read view for index plus lookup. |
| Partition-leader live tables | Same partition selection and remote boundary | Valid leader/live-state handle; no claim that a RocksDB snapshot captures this state. |
| Per-node introspection | Same domain analysis, work units, fanout and RPC lifecycle | Eligible node generations/roles; explicitly declared partial-result policy. |
| Coordinator-local metadata | Same predicate/residual, schema and diagnostics contracts | Execute once in the selected coordinator context, even if metadata describes the whole cluster. |
| Offline snapshot queries | Same planner with supplied local partition universe | No live routing/RPC; unavailable leader state retains documented offline behavior. |

Current node fanout converts target errors to warnings, while ordinary storage scans propagate
errors. Preserve this distinction as policy. If a result or aggregate depends on missing best-effort
inputs, carry incompleteness to the query result; never silently report a partial count as complete.
Replace physical-node downcast-based warning discovery with a shared diagnostics sink and preserve
the gRPC warning response behavior. Define handling for response formats that cannot carry warnings
before enabling best-effort execution through them. [U4]

### 2.5 Migration coverage and retirement gate

The migration manifest must include every production provider registration found in the inspected
query crate, plus I's new surfaces and the offline adapter:

| Consumers | Migration coverage |
| --- | --- |
| `sys_invocation_status`, `sys_locks`, `state`, `sys_journal`, `sys_journal_events`, `sys_inbox`, `sys_promise`, `sys_vqueue_meta`, `sys_vqueue_entry_status`, `sys_vqueues` | Persisted partition scopes; preserve each table's ID/scope/key derivations and point/range semantics. [U5] |
| `sys_invocation_state`, `sys_scheduler`, `sys_user_limits` | Leader-live adapters, shared partition-range binding, preserved offline availability/null behavior. [U3] [U6] |
| `loglet_workers`, `bifrost_read_streams`, `config` | Node scopes: currently log-server-role, worker-role, and all configured nodes respectively. Use executable declarations as the baseline: `bifrost_read_streams` has an all-nodes comment but actually constructs a worker-role locator. Resolve intended policy explicitly rather than silently broadening it. [U5] |
| `sys_deployment`, `sys_service`, `sys_rules`, `nodes`, `partitions`, `partition_replica_set`, `logs`, `partition_state` | Single coordinator-local sources; no duplicated cluster metadata through fanout. [U5] |
| I's seven `_idx_*` tables and `sys_service_stats`, `sys_deployment_stats`, `sys_virtual_object_stats` | Same partition selection/planner; reuse index access contracts and preserve aggregate-view semantics above raw stat sources. [I10] [I7] [U7] |
| `PartitionTables` / restate-doctor snapshot queries and test selectors | Same planning pipeline with fixed local universe; no production test-only routing implementation. [U6] |

M0 records this manifest and the adapter contracts. M1 supplies common expression extraction and
work selection in the new engine. M2 binds its local source adapters. M5 connects its partition and
node remote paths to the shared tagged envelope, boundary, and diagnostics lifecycle. New-engine
consolidation is complete by M5, not deferred to worker multi-partition execution in M7. Legacy
production entry points retain their prior behavior until the old network protocol range is retired.

Completion requires all of the following:

- Every manifest entry on the new path uses the common source declaration, normalizer, work
  planner, and binder. New features do not add independent parsers or schedulers.
- Legacy provider/executor implementations remain isolated behind protocol-mode selection.
  Remove them only after the protocol-retirement gate below; thin registration facades may survive
  if they delegate to the new engine without retaining independent behavior.
- `PointReadFanout` choices and hard-coded expansion thresholds are replaced by common access
  capabilities and budgets; exact row sets survive changes in batching strategy.
- `ScanToScanPartitionAdapter`/sentinel routing is absent from the new wire path. Its legacy use
  stays confined to the legacy engine and disappears when old-protocol support is removed.
- Storage, live-state, node, and metadata producers share cancellation, metrics, residual-filter
  handling, and diagnostics machinery while keeping their declared semantics.
- Adding a source in each family is demonstrated with declarations plus a producer, without a
  new routing parser or fanout executor.

**Legacy retirement gate:** the supported network-protocol minimum excludes the legacy range,
all supported Coordinators select the new query path, and in-flight legacy executions/connections
have completed or been terminated through the established cleanup interface. Then remove legacy
messages, handlers, parsers, fanout executors, and dispatch branches in a dedicated cleanup commit.
Retire protocol values/message identities without reusing them. This is a release-compatibility
decision, not a prerequisite for the new index stack.

## 3. Core contracts

### 3.1 An access path describes both eligibility and guarantees

The proposed access-path descriptor must carry enough information to explain and validate:

| Information | Purpose |
| --- | --- |
| Logical table and schema identity | Establish which SQL relation is being implemented. |
| Index identity and semantic/layout version | Bind the intended persisted representation without depending on raw byte details. |
| Indexed row population and membership condition | Prove the query does not require rows omitted by a partial or specialized index. |
| Typed source scope and readiness requirement | Carry partition/range or node/coordinator scope explicitly; validate index completeness for storage access. |
| Available fields and their SQL mappings | Decide coverage and preserve types, nullability, timestamp precision, and identity semantics. |
| Static constraints and residual obligations | Distinguish candidate pruning from exact SQL evaluation. |
| Scan direction and ordered-prefix alternatives | Permit ordered access and bounded expansion of finite leading domains. |
| Guaranteed output properties | State per-stream ordering, execution partitioning, uniqueness where proven, and boundedness. |
| Runtime-filter bindings | Identify input stage and fields to which each update applies. |
| Primary lookup and read-view requirements | Make materialization and consistency requirements explicit. |
| Conservative fallback | Define equivalent execution when the index or fragment is unavailable. |

These are semantic fields, not a finalized Rust or protobuf schema. Prefer typed states that make
an unvalidated index ineligible for authoritative access. Bind ready resources at execution rather
than holding planning-time database handles or configuration guards across awaits.

### 3.2 Candidate predicates are necessary conditions

A storage constraint must never reject a row needed by the query. Unsupported expressions remain
residual; unsupported conversion is different from a proven empty domain. Preserve exact finite
sets even when scanning their enclosing interval, and do not turn correlated cross-field ORs into
an exact Cartesian product. Retain the typed adapter's distinctions. [I1] [I4]

Separate three facts:

1. A key codec evaluates its compiled domain exactly.
2. That domain is equivalent to a particular SQL predicate, or merely necessary for it.
3. The index represents all relevant logical rows, with the correct multiplicity.

Only a proof of the relevant facts permits removing a residual filter or declaring SQL filter
pushdown exact. A key may expand to multiple rows, as with the stats tables; key acceptance alone
does not establish row-level exactness. [I7]

Track predicate lineage through projections and lookup stages. A runtime filter on aggregate
output, for example, cannot be applied to raw storage keys merely because a column name matches.
Keep exact residual evaluation until a specific rewrite proves it redundant.

When choosing among indexes, split logical constraints into those supported by the selected key
and those still owed elsewhere. Do not weaken `PreparedKeyFilter` to silently ignore missing key
fields: its existing error detects a bad binding. The access planner performs the split and retains
the residual obligation explicitly. [I2]

### 3.3 Static planning and dynamic refinement remain distinct

The static predicate selects the initial scope and access path. A runtime filter refines the
remaining work within that scope. Keep mutable updates out of immutable serialized expression
copies; use explicitly bound filter slots and snapshots.

The `LiveFilter` contract is the right foundation: **every published snapshot must be sound on
its own**, including for keys that will never be revisited after a seek. Never interpret an
incomplete join-key set as a sound negative-membership filter. Unsupported live predicates may
be ignored for pruning; failures in an exact residual must fail the query. [I4]

Reuse I's generation checks and amortized polling before decoding/materialization. Its current
64-key polling interval is an implementation starting point, not a wire guarantee. Seeks must
advance within the active physical scope; exhausting a suffix under one stage/service must not
terminate other parent groups. Stale conservative filters may admit extra candidates. [I3] [I4]

For remote execution, bind each update to `QueryId`, scan input, schema/field mapping,
and increasing generation. Once execution starts, updates refine the chosen path; switching to a
different index mid-stream requires a separate resume/deduplication design and is not an initial feature.

### 3.4 Primary lookup is an explicit unary stage

The stage consumes locators plus covered fields and returns the required primary fields. It is
not a general relational join. Its implementation may batch point reads, but it must preserve
input multiplicity and either preserve ordering or explicitly declare its loss.

Start with bounded, order-preserving lookup: attach an input ordinal, optionally sort/deduplicate
physical reads within a bounded batch, then reconstruct results in input order. Do not reorder or
deduplicate logical output merely to improve I/O. Keep canonical incarnation identity through the
conversion to a physical primary lookup key. I's existing multi-get is useful machinery, not a
complete implementation of this contract. [I11]

All storage work for one `(QueryId, PartitionId)` shares one owned local read view, including
independent table references, fragments, index iterators, and primary lookups. The agreed initial
contract is one explicit RocksDB read snapshot per query/partition, acquired by the Worker parent
before the first storage read and passed to every child iterator/Get/MultiGet. Different partitions
may observe different committed points in time. This is a stronger guarantee than sharing a
snapshot only within each index-to-lookup pipeline.

Index and primary mutations must be committed atomically for that snapshot to represent a
consistent logical state. I's entry lifecycle boundary writes the primary status and secondary
indexes through the same `PartitionStoreTransaction`. A read snapshot does not compensate for an
incomplete index; readiness remains a separate requirement. [S1]

Existing repeatable-read transactions demonstrate snapshot machinery, but the standalone index
scan and multi-get are not currently bound to one shared query snapshot. Add an owned read-only
binding suitable for background storage work; do not hold a partition-processor-owned mutable
transaction across query execution. Snapshot lifetime follows the Worker parent and its children,
not the lifetime of a single batch or Next RPC. [I11] [I12]

Validate locator incarnation, row population, and ownership/range before output. If an authoritative
ready index disagrees with its primary record in the same read view, surface an integrity failure;
silently skipping corrupt/stale entries cannot compensate for missing index entries. Normal row
filters can still reject candidates. The read view is local, not a distributed snapshot.

### 3.5 Index readiness precedes transparent substitution

Maintain separate concepts for index capability, maintenance enabled, backfill in progress, and
complete/query-eligible coverage. The exact persistent representation is a milestone decision.

- Fresh stores may become ready once creation and all relevant writes establish completeness.
- Existing stores require a backfill or verified rebuild with concurrent-write handling.
- Publish readiness atomically only after catching up to the writes covered by its proof.
- Preserve or invalidate that proof explicitly on restore, split, and format migration.
- Both planner eligibility and worker open-time validation must check it.
- If availability is unknown at planning time, carry a semantics-preserving primary fallback.

Do not equate a configuration flag, an index prefix, or a binary version with readiness. I's
maintenance path explicitly makes this distinction. Also prove population applicability: entry
indexes include multiple entry kinds, and virtual-object indexes cover only their documented
subset. An index for VQueue entries is not automatically an index for every invocation table. [I5] [I10]

### 3.6 Ordered scans, sorting, and Top-K

Ordering is a property of the **emitted SQL values in each DataFusion execution stream**, not
merely of one RocksDB iterator's bytes.

For the existing `EntryByStageServiceKey` layout, storage order is stage, service, descending
transition timestamp, then canonical identity. Fixing stage and service can expose timestamp
order. Selecting several services or stages requires separate ordered runs plus a merge, or a
retained sort/Top-K; simple concatenation is insufficient. `EntryByStageKey` avoids the service
prefix but still has a stage prefix. [I5]

Additional rules:

- Validate SQL direction, NULL placement, comparison semantics, and tie keys against each codec.
- Timestamp projection can lose precision: the index retains HLC bits while SQL exposes
  milliseconds. Ordering by milliseconds alone may be preserved, but adding a canonical-ID
  tie-breaker does not follow automatically: the hidden HLC counter orders rows within that
  millisecond first. Ordered textual ID comparisons likewise need their own proof. [I13]
- A forward scan over a descending field codec can satisfy DESC. A reverse RocksDB walk is a
  different capability: I's prepared cursor proves forward progress only. Reverse bounds and
  reverse seeks need separate implementation and tests despite the lower iterator exposing `Prev`.
- Merging individually ordered Restate-partition streams is required when their sort-key ranges
  overlap. Do not group them into one lane by unordered concatenation and keep the ordering claim.
- Primary lookup, batching, network buffering, and fallback must retain any promised order.
- Publish exact ordering only for a proved path. An inexact preferred order can help dynamic
  Top-K pruning while retaining DataFusion's sort. [I3] [F1] [D2]

For `ORDER BY ... LIMIT K`, distinguish three optimizations:

1. **Ordered index + early stop:** consume qualifying rows in the required order and stop when
   sufficient rows have been produced. Combine partition outputs with an ordered global merge.
2. **Local Top-K + global Top-K/merge:** use when exact index order is unavailable. Keep local
   candidates after all row-membership predicates; globally finish the query at the coordinator.
3. **Dynamic filtering:** feed a valid cutoff into the index cursor and pre-lookup filter to
   reduce remaining I/O. This can complement either execution model; it is not sort elimination.

Limits count qualified logical rows, never unvalidated index candidates. For OFFSET O and LIMIT K,
local candidate bounds generally need O+K; apply the offset at the global stage. Exclude WITH TIES
from the initial local truncation rule unless tie completeness is handled. When required fields
are covered and lookups cannot affect membership or rank, Top-K can precede lookup; otherwise
lookup and residual filtering must precede truncation. [D1] [D4]

These local K/O+K rules apply to disjoint inputs at the same logical row stage as the global Top-K.
Do not push truncation below a join, aggregation, deduplication, or other cardinality-changing
operator without a separate equivalence proof. Comparators, null rules, and tie handling must agree
between local and global stages.

Finite-domain predicates and SQL DISTINCT are separate. IN/prefix expansion must avoid duplicate
candidates while preserving original row multiplicity. A distinct-index walk is a later relational
rewrite, with global deduplication where needed, not a mode silently enabled on ordinary scans.

## 4. Planning and execution flow

During optimization, keep storage-local subtrees available to the rules that choose access paths
and push computation. Record their placement scope separately so an optimizer cannot accidentally
treat scans assigned to different owners as one local subtree. Freeze the opaque `RemoteNodeExec`
boundary only when lowering the chosen subtree. This evolves F's early opaque boundary; simply
adding index nodes behind its existing empty `children()` would hide them from normal traversal.
[F1]

The proposed phase dependencies are:

1. **Logical analysis and selection:** resolve fields through the source declaration, normalize
   predicates once, select partition/node/coordinator scopes in the shared work planner, preserve
   the full predicate, and compute required columns, including hidden lookup locators and
   residual/order dependencies. Reuse the query-owned topology view for all sources.
2. **Access planning:** enumerate primary and eligible index paths, extract typed conditions,
   determine covering versus lookup, and construct the local physical subtree. No row I/O here.
3. **Physical optimization:** propagate predicates and requested order; allow access-path
   reselection for useful ordering. Recompute properties when children change.
4. **Placement-aware rewrites:** move only partition-local operations and add the corresponding
   global finishing operators. Define execution lanes without losing ordering/distribution.
5. **Requirement enforcement and runtime-filter binding:** satisfy distribution/order requirements;
   bind dynamic producers to valid consumer fields/stages. Re-enforce affected requirements after
   any later structural rewrite rather than rerunning every optimizer rule blindly.
6. **Remote lowering:** encode the maximal supported storage-local subtree and replace it with an
   opaque remote boundary advertising the same externally required contract. Preserve runtime
   filter bindings through a control interface, not duplicate mutable expressions in protobuf.
7. **Final validation:** schema, ordering, distribution, fragment capability, and fallback checks;
   DataFusion's final plan sanity validation must see the lowered plan.
8. **Open/execute:** validate source-specific placement, capabilities, and readiness; bind the
   appropriate local resources, including a shared read view for storage index/lookup units.
    Acknowledge acceptance and drive batch production. Attribute child work and cancellation to `QueryId`.

Implement these dependencies against the pinned DataFusion optimizer sequence, with full-pipeline
tests. F currently places partial-aggregate and scan-fragment rules around `FilterPushdown(Post)`;
that placement is useful evidence, not a permanent ordering to preserve if a rewrite changes
properties. Avoid a rewrite that destroys ordering after the last enforcement pass. [F4] [D3]

Keep plans immutable and execution state per invocation. Prepared buffers may be reused where
their lifetime and input schema match, but iterators, runtime thresholds, snapshots, metrics,
and one-shot streams must not leak across executions of a cached or cloned plan.

## 5. Remote contract and fallback

### Protocol-gated rollout

Use the existing connection handshake as the query-infrastructure capability boundary. It chooses
a mutually supported `ProtocolVersion`, available through `Connection::protocol_version()`. The
inspected baseline supports V2 through V4; choose the next unused version when implementing this
work rather than reserving a number in this document. [P1]

- Introduce distinct new execution requests/responses, gated on the new protocol version. Existing
  `RemoteQueryScannerOpen`/`Next`/`Close` and their response behavior remain unchanged for legacy use.
- Keep `MIN_SUPPORTED_PROTOCOL_VERSION` unchanged during coexistence. Advertise the new maximum
  only when the message family and baseline new execution contract work end to end.
- Gate client send, server dispatch/decode, and response selection on the actual negotiated
  connection version. Expose that version from incoming-message context for dispatch as needed.
  Do not infer support from node binary-version strings or an ownership field.
- Add minimum-version guards for the new codecs. The generic `bilrost_wire_codec!` currently only
  requires V2 and is insufficient by itself. The reply path assumes the selected response is
  encodable and uses `expect`, so never construct a new response variant for an old-version request.
  Reject out-of-version requests through an already-compatible transport error. [P2]
- Audit all exhaustive network-version matches when adding the enum value; preserve unrelated
  services' existing payload encodings on the new version.

#### Query mode during mixed-version operation

The proposed initial policy chooses one query-wide execution mode before opening data streams.
Resolve the relevant
source universe/targets and inspect actual negotiated connections in a Coordinator preflight phase,
outside per-table scan construction. This phase reads no query rows.

| Situation | Execution mode |
| --- | --- |
| All required participating peers negotiate the new version, and the query uses supported new contracts | New engine, including its QueryId/read-view guarantees. |
| A required peer negotiates a legacy version, and the request permits legacy query semantics | Entire query uses the legacy path with its existing behavior, including on new nodes participating in that query. |
| The request requires a new-only feature or consistency contract but a required peer cannot support it | Fail explicitly before data consumption; do not silently weaken the requested contract. |
| A connection fails or placement changes | Apply the selected engine's failure policy; connection failure is not evidence of legacy protocol support. |

Show the selected mode in plan/execution diagnostics. During rolling upgrade, legacy-mode queries
retain the baseline consistency contract; do not label their independent scans as one shared
partition snapshot. Never select legacy for one child of a new query and new execution for another
child sharing its partition read context. A reconnect that negotiates a lower version must fail the
affected new execution, not switch modes after rows have escaped.

New nodes must temporarily accept unchanged legacy queries as well as new queries, since an older
Coordinator or query-wide legacy selection can target them. The retirement gate in section 2.5
removes that obligation once every supported execution selects the new path.

#### What network version does not replace

The network version establishes the execution message family and its baseline guarantees. Within
that family, retain fragment-format/custom-operator version checks, portable expression semantics,
source capability validation, index readiness, and placement validation. Supporting the new network
protocol does not prove that a specific index is complete or a particular fragment is supported.
Fragment decline/fallback within the new engine must keep the new read-view and output contract;
it is not permission to invoke legacy execution. [F2] [I10]

### 5.1 What crosses the boundary

Extend the physical-fragment codec to describe storage access leaves and primary lookup, in
addition to the supported DataFusion operators. Keep DataFusion's physical serialization rather
than introducing a second general query language. Use a versioned semantic descriptor for custom
storage nodes; prepare raw keys on the worker. F's codec currently knows only `FragmentLeafExec`. [F2]

The proposed execution envelope describes:

- Original `QueryId`, separate child scanner/stream identity, and a tagged scope: partition range,
  node generation, or coordinator-local source. Node-scoped requests do not carry a sentinel
  partition/range. Open establishes the association; subsequent batches, predicate updates,
  metrics, completion, and cancellation carry or unambiguously inherit it. `QueryId` also reaches
  Worker-side storage tasks for downstream attribution.
- Required ownership validation and index readiness/layout capabilities.
- Fragment-format compatibility, custom operator versions, and supported expression semantics.
- Expected output schema, execution-partition count, ordering/distribution, and emission/boundedness
  guarantees required upstream.
- Runtime-filter bindings and initial snapshots.
- Metrics negotiation and flow-control/resource limits.

Schema compatibility alone is insufficient: decoding a function under a different timezone,
configuration, or UDF implementation can preserve its type but change its values. Use a tested
portable allowlist initially; negotiate or capture semantic configuration before broadening it.
Encoding success is not a semantic-equivalence proof. Apply this to both fragment expressions
and access predicates that can cause storage pruning.

Keep **network-version selection**, **fragment/source validation**, and **ownership validation**
distinct. The negotiated network version selects the message family, fragment/source checks validate
the requested operation, and ownership checks validate the planned data target. Use new messages
to avoid W1's legacy-field collision; field presence and binary-version strings are not substitutes
for the negotiated protocol.

### 5.2 Fallback is an equivalent plan, not just a different schema

F can currently execute a declined unary fragment over a returned raw stream. That remains useful,
but it does not generalize unchanged to a subtree containing storage-local primary lookup. [F2]

Define fallback at the logical-table boundary:

- Use a complete primary scan with all necessary columns, then equivalent filters/projections
  and aggregation at the coordinator, or execute that alternative on a capable worker.
- If the remote boundary promised ordered output, fallback must include the required local sort
  or another ordered access path. Never expose unordered rows under an ordered boundary.
- Choose and acknowledge the executable alternative before consuming input for that query
  execution. If a mismatch is discovered after partial consumption, fail the execution; do not append
  a restarted scan and risk missing/duplicating rows.
- Do not treat corruption, ownership mismatch, or arbitrary execution errors as a fragment decline.
- A raw index inspection query keeps its inspection semantics; it is not automatically replaced
  with a complete base-table query when the index is incomplete.

Ordered fallback may be expensive. That is acceptable as a compatibility path if it remains bounded
by the query's memory/spill policy and is visible in metrics; it must not silently change results.

### 5.3 Flow control, updates, and cancellation

Batch demand and predicate updates are independent concepts. Start with bounded pulls and a
small prefetch budget; one RPC per batch is not a correctness requirement. Measure the tradeoff
between RTT overhead and stale candidate work before adding pipelining. [D4]

Updates piggybacked on `Next` are adequate for some streaming scans but cannot be the only update
path for a blocking remote subtree. While a remote Top-K/aggregate is consuming input to produce
one batch, the current scanner loop does not process another predicate update. Add a coalescing
latest-generation control path for such execution; never wait for a future threshold to make
progress. Worker-local Top-K can update its own scan directly. [F5]

Consume the assumed Coordinator-stream-drop notification at the Worker parent/subtree boundary.
Stop admitting work for that subtree, cancel its computation and storage producers, and release
its iterators and lookup buffers. Shared snapshots and partition handles remain alive until every
child using them has stopped. Cancelling one child must not invalidate a sibling's read view.
The same structured cleanup applies on normal completion. [D3]

Bind scanner operations to their owning peer and `QueryId`; bound retained requests and
lookup/reorder queues. Share worker runtime/memory accounting rather than creating an independent
pool per remote execution. Test the response to cleanup notifications; timeout/keepalive and
stream-loss detection/delivery are outside this design.

### 5.4 Optional partial results from failed partitions

**Default: disabled for partitioned queries.** Add a query-scoped option allowing classified
partition failures to produce an explicitly incomplete result while unaffected work continues.
This uses the common completeness policy and diagnostics sink, not another fanout implementation.
Existing node-introspection best-effort behavior remains a separately declared source policy; this
addition must not silently change its default during migration.

#### User-facing setting and request isolation

Proposed SQL interface (the name and syntax are a design proposal, not an existing option):

```sql
SET restate.query.allow_partial_results = true;

SELECT status, COUNT(*)
FROM sys_invocation_status
GROUP BY status;
```

The setting applies to the following query **in the same API request** and resets afterward.
An omitted setting or an explicit `false` keeps partition failures fatal. Do not require a prior
standalone SET request to mutate server-global state. Do not interpret the option as enabling
automatic retries or rerouting; restarting execution with new read views is separate work.

Implementation requirements:

- Register a typed DataFusion configuration extension with a default of `false`, validate the
  boolean, and expose the effective value in EXPLAIN/diagnostics. Give its documentation the
  appropriate `Since` release annotation when implemented.
- Parse a restricted settings preamble plus exactly one supported read query. Validate the
  entire request before execution; preserve existing DDL/DML restrictions. This extends the
  current single-statement query path, rather than passing a batch to `sql_to_statement`. [Q1]
- Build an isolated per-request planning configuration/session state over shared catalogs and
  runtime resources. DataFusion's normal SET mutates its session, and its extension contract
  explicitly requires independent cloned mutable configuration. Do not run SET against Restate's
  shared `SessionContext`. [Q2]
- Capture the effective policy in the query execution and pass it to the common execution boundary.
  Concurrent requests, subsequent queries, and reused plans must not inherit another request's
  opt-in. Unknown settings and unsupported partial-mode plans fail explicitly.

#### Failure behavior

| Event | Default (`false`) | Opt-in (`true`) |
| --- | --- | --- |
| Classified partition unavailable, lost transport, or ownership movement | Fail the query execution. | Mark the affected scan unit incomplete, stop that unit, continue unaffected units. |
| Fragment/index capability unavailable before consumption | Use the equivalent fallback or fail. | Same behavior; the option is not a substitute for capability/readiness fallback. |
| Invalid SQL, expression evaluation error, corruption, schema/codec contract violation, or unclassified internal error | Fail. | Fail; never convert arbitrary errors into missing data. |
| Coordinator failure, query cancellation, or exhausted query-wide resources | Fail/cancel. | Fail/cancel; a result cannot be finalized by a failed Coordinator. |
| Valid empty selection or successful empty scan | Complete empty input. | Complete empty input, not a failed partition. |

Add typed source failure categories to the unified new protocol; do not infer partial-result
eligibility by parsing `DataFusionError::Internal` strings. This option requires new-mode execution;
legacy mode retains its existing policies and cannot silently accept the opt-in. Unclassified
new-mode failures remain fatal. Failure policy is enforced at a source/fragment output boundary before
mixing unrelated inputs. A node-wide failure marks all affected work units, including any whose
exact progress is unknown.

The failure must be attributable to known selected work. Failure to establish the required source
universe or to validate the query plan remains fatal; it cannot be presented as an empty selection.
For an identified partition whose target cannot be resolved, partial mode may record unavailable
work during placement without inventing a target or omitting it from the completion report.

#### Streaming semantics: retain delivered prefixes

The initial proposal is **best-effort streaming**, not atomic inclusion/exclusion of whole
partitions. If a unit fails after delivering valid batches, those batches and their downstream
effects remain. The terminal report marks that unit as partially read; no retry appends a restarted
execution and no retraction of emitted rows is implied. Whole-partition atomicity would require
buffering/spilling until completion or a replay/retraction protocol, and is a separate extension.

- Counts, sums, and other aggregates reflect the input/state actually delivered. They are not
  certified whole-table values. With fragment aggregation, an interrupted unit might deliver no
  state at all; partial results can depend on execution placement and failure timing.
- Top-K is over available candidates, not a guarantee that failed partitions contain no better rows.
- Outer/anti joins can produce rows based on missing matches, so a partial result is not necessarily
  a subset of the complete query result. Keep these shapes ineligible initially unless their
  incomplete-input semantics are deliberately supported and presented to clients.
- Failure-tolerant execution needs an optimizer capability check. Runtime filters or early-stop
  decisions must not rely on evidence from a failed unit that is then discarded. Retained-prefix
  Top-K evidence may be usable; unpublished remote build/Top-K state must not silently prune other
  surviving units. Disable such pruning or keep the failure fatal until the dependency is proved.
- If all required partition work fails and no useful contribution was delivered, return an error
  with diagnostics rather than a successful empty table or misleading aggregate zero. This differs
  from a genuinely empty table. An intentional LIMIT/Top-K stop is not a partition failure.

#### Completion information is part of the result contract

On successful completion, report whether the result is complete or partial, the effective policy,
and the affected source/partition/range/node generation with failure reason and whether data had
already escaped. Distinguish successful, failed, partially read, and deliberately unneeded units;
do not equate "not fully scanned" with query incompleteness when an optimizer proves work unnecessary.

Final status is only known when execution terminates. A dropped connection or missing terminal
completion record is not evidence of a complete result. Earlier streamed batches are provisional
with respect to completeness. Even an opted-in query can end in a fatal error from another cause.

The current gRPC response already has final per-node warnings, but lacks structured partition
coverage and a completion status. The HTTP handler currently uses only the result stream, and its
JSON envelope currently writes the `rows` member. Extend the shared diagnostics contract and each
supported response format before enabling the option there. [Q3]

- gRPC: extend the terminal response with structured completion/coverage, preserving existing warnings.
- JSON: append explicit completeness and failed-work information to the final response envelope.
- Arrow-over-HTTP: define a supported terminal metadata/trailer mechanism with client support, or
  reject the opt-in for that response format until one exists. An initial header cannot describe
  failures that occur after headers are sent.
- CLI/UI: show incomplete results visibly, including aggregates; never hide the completion report.

This is an optional delivery slice, M5a. Strict execution remains available while it is developed.

## 6. Concrete end-to-end examples

The SQL below refers to existing inspection surfaces in I; automatic substitution for a base table
is a later step requiring a verified table-to-index mapping and readiness.

### 6.1 A covered ordered scan

```sql
SELECT canonical_id, next_at, seq
FROM _idx_entry_next_at_by_stage
WHERE stage = 'inbox'
ORDER BY next_at ASC, seq ASC
LIMIT 20;
```

The declared index order is stage, next time, sequence, canonical identity. With stage fixed,
the first two requested sort fields can be produced from index keys. This example already appears
in simpler form in the stack's release note. [I5] [I10]

Proposed plan:

```text
Coordinator: ordered merge, final LIMIT 20
  Partition-local streams, local or remote:
    qualified local LIMIT 20
      projection from index entries
        ordered index scan, fixed stage='inbox'
```

No primary lookup is needed for this inspection schema. Across storage partitions, merge sorted
streams rather than concatenate them. Any retained residual must run before the local limit.

### 6.2 A finite leading domain

Change the predicate to `stage IN ('inbox', 'running')`. Each stage supplies a sorted run, but the
combined index traversal is stage-major, not globally time-major. Possible plans are two prefix
scans plus an ordered merge, or a broader scan plus local Top-K. Put a budget on prefix expansion;
exceeding it broadens work without discarding constraints. The live cursor can also skip rejected
suffixes and carry into the next stage, as demonstrated at `ef8eca39d82a`. [I3] [I14]

### 6.3 A non-covering base-table query

Once a base-table/index equivalence is established, adding fields absent from the index yields:

```text
Projection
  remaining exact row filter
    ordered primary lookup using a shared read view
      index-covered filter + runtime cutoff
        chosen index scan
```

The planner may put Top-K before lookup only when index fields establish all membership and
ranking decisions. If a predicate needs primary values, lookup/filter precede local candidate
truncation. Primary lookup stays on the partition owner even when the query is coordinated elsewhere.

## 7. Implementation milestones

These are architectural outcomes, not the landing order. Start with the C00 correctness harness,
then follow the C01-C30 infrastructure sequence in the
[main-based execution plan](query-infrastructure-execution-plan.md#4-commit-sequence) for reviewable
commits. In particular, concrete index layouts, maintenance, backfill, and M3's authoritative
base-table substitution belong to the follow-on secondary-index stack. Infrastructure develops
the access contracts against existing primary readers and conformance fixtures, so it can land
without first adding the experimental `_idx_*` or stats tables.

Each milestone should be independently reviewable and retain a complete primary-scan execution
path. The evidence column identifies reusable work, not a claim that a milestone is already done.

| Milestone | Deliverable and dependencies | Completion evidence |
| --- | --- | --- |
| **M0: unify contracts and baselines** | Define roles, `QueryId`, cleanup interface, and the new network-version boundary. Freeze legacy wire/behavior fixtures; define source scopes, query-mode selection, fragment/readiness/ownership checks, and the section 2.5 migration manifest. | Old behavior is captured; new request/response gates are specified; query identity and source policies are explicit; legacy retirement depends on protocol deprecation. |
| **M1: shared selection and predicate engine** | Unify logical/physical expression analysis, routing-key derivations, node-ID selection, empty/set/range domains, work grouping, and budgets. Reuse I's typed filters, codecs, prepared cursor, live adapter, and metrics as storage bindings. Depends on M0. | Reference-equivalent results and sound selections across all source families; no independent node/partition expression parsers; exact key sets survive batching; RocksDB tests preserve bounds and live-seek savings. |
| **M2: common local sources and access subtrees** | Migrate sources to shared planning/binding. Add Worker-parent ownership, one shared read context per `(QueryId, PartitionId)`, explicit primary/index access and unary lookup, coverage analysis, bounded ordered lookup, and EXPLAIN. Use inspection tables/complete fixtures first. Depends on M1. | Every local manifest entry uses the shared pipeline; separate scans of the same query/partition share a snapshot while writes proceed; index-only path has zero primary reads; cleanup retains views until children stop; live sources retain their own consistency contract. |
| **M3: readiness and base-table substitution** | Add index-to-table population proofs, persisted readiness, backfill/rebuild plus live-write reconciliation, and conservative access-path selection. Depends on M2. | Existing-store upgrade, fresh-store activation, split/restore, concurrent writes, and absent/incomplete indexes all produce complete base-table results or an explicit error; unready paths are never authoritative. |
| **M4: ordering and local Top-K** | Implement requested-order handling and proven output properties; ordered prefix runs and cross-partition merge; safe local K/O+K; retain sorting for unsupported order. Depends on M2; base-table release also requires M3. | Full DataFusion optimizer/sanity tests; multi-partition and multi-prefix ordering; HLC/SQL tie cases; local limits after residuals; keys and lookups reduced for selective Top-K. |
| **M5: unified remote subtree execution** | Activate the new engine behind the negotiated network-version gate; bind scoped requests and `QueryId` to Worker read contexts. Preserve equivalent new-engine fallback and cleanup. Keep legacy execution isolated until protocol retirement. Depends on M2/M4; base substitution also requires M3. | New-engine sources share machinery without sentinel partitions; mixed-version requests/responses stay on the selected mode; QueryId/read views/output contracts hold; old-protocol queries retain their behavior. |
| **M5a: optional partial partition results** | Add the default-off, request-local SQL setting, typed failure classification, retained-prefix semantics, planner eligibility, and terminal completeness across supported APIs/clients. Depends on M5; independent of completing M6-M8. | Default fails on a partition error; opt-in continues unaffected units and reports incompleteness; concurrent settings are isolated; mid-stream failures, aggregates, all-failed work, and unsupported response/plan shapes follow section 5.4. |
| **M6: distributed Top-K feedback and transport** | Bind runtime slots across lowered plans; add update delivery independent of batch production for blocking subtrees; tune bounded buffering and lookup batches with measurements. Reuse M1's live cursor. Depends on M4-M5. | Thresholds tighten during a pending Next; stale/delayed updates remain correct; no progress dependency on receiving an update; bounded memory and measured network/storage savings under nonzero RTT. |
| **M7: worker-local multi-partition tasks** | Extend fragment validation/binding to multiple same-table partition leaves and local combination nodes. Dispatch a specified partition set under one worker task; merge ordered streams or reduce aggregate states before transmission; preserve per-partition read views and metrics. Depends on M5-M6. | Same results as separate tasks; ranges stay disjoint; no duplicate work; worker-level reduction/merge shown in EXPLAIN and metrics; no cluster-snapshot claim. |
| **M8: colocated multi-input subtrees** | Extend leaf bindings to multiple tables and support only joins with a proved complete local match domain. Depends on M7 or a separately scoped single-partition variant of M5. | Compare against coordinator joins, including outer/semi/anti semantics only when explicitly supported; reject non-colocated joins rather than lose cross-worker matches. |

M0-M1 preserve and integrate existing useful behavior. M2-M6 form the main indexed-scan/Top-K
delivery path. M2/M4/M5/M6 can be exercised on inspection tables before M3 enables authoritative
base substitution. M7-M8 extend the execution unit toward node-local trees. General distributed
shuffle does not gate the core milestones.

### Suggested review-sized work within the milestones

- M0: role definitions and `QueryId`; source/retirement manifest; tagged scope and topology contracts;
  stream-drop notification interface; wire fixtures; baseline tests.
- M1: shared domain normalizer; routing derivations and node selection; common work planner;
  codec/preparation integration; live reseeking and iterator metrics.
- M2: common local source adapters; access descriptors/nodes; Worker parent and shared partition
  snapshot binding; lookup node;
  coverage/projection rewrite; removal of migrated local planning paths.
- M3: lifecycle/readiness persistence; safe backfill; one verified base-table substitution.
- M4: single ordered run; multiple prefix/partition merge; Top-K and limit rules; tie semantics.
- M5: shared node/partition wire path with original `QueryId`; storage-node codecs; negotiation/fallback;
  common warnings and cleanup-notification tests; version-gated isolation of legacy implementations.
- M5a: request-local settings; typed partition failure policy; terminal completion schemas;
  client presentation; failure-tolerant optimizer and runtime-filter tests.
- M6: independent update control; bounded prefetch/lookup experiments; regression benchmarks.

Avoid coupling unrelated CLI, statistics-table, or identifier changes to the query planner unless
they are prerequisites for a selected index's primary identity or maintenance. Preserve existing
persisted encodings and `SingleDelete` lifetime invariants when extracting that dependency slice. [I5]

## 8. Verification and observability

The concrete implementation specification is [Query correctness harness](query-correctness-harness.md).
Implement its first slice on main as C00, before changing execution, and extend it with each feature.
It compares canonical logical rows queried through plain DataFusion, a broad primary-storage
reference, and optimized local/distributed paths. The legacy engine is a compatibility baseline,
not the only semantic oracle.

Compare complete typed result bags with multiplicity, exact ordered sequences when the ordering is
total, and legal tie/rank outcomes when it is not. Consume every batch and terminal status. Validate
the comparator with deliberate missing/duplicate/wrong-value/ordering mutations, and separately
assert path selection and work savings so a silent full-scan fallback cannot certify an optimization.

Use a small number of high-signal scenario suites rather than tests mirroring each helper:

1. **Semantic differential suite:** primary scan versus index access, covering/non-covering,
   accepted/declined remote execution. Include false-positive candidates, finite-set holes,
   correlated ORs, NULL/empty strings, prefix boundaries, timestamp precision, and duplicates.
2. **Ordering suite:** multiple physical partitions in fewer lanes, overlapping time ranges,
   multi-stage/service selections, ASC/DESC/NULL placement, ties, residuals, OFFSET, and empty inputs.
   Run the complete optimizer pipeline and final sanity validation, not only one custom rule.
3. **Lifecycle suite:** incomplete/backfilled stores, transactional membership changes, incarnation
   changes, partition movement/split, and snapshot-coherent index plus lookup reads. Include two
   independently opened scans of different tables sharing one `(QueryId, PartitionId)` while the
   processor commits updates, and distinct queries receiving independent read contexts.
4. **Runtime suite:** delayed and coalesced dynamic updates, live seeks between parent groups,
   and injected stream-drop notifications during active storage/lookup/blocking operators. Verify
   subtree teardown, sibling safety, and snapshot release only after child work has stopped.
   Detecting stream loss and testing timeout/keepalive policies belongs to the separate transport work.
5. **Compatibility suite:** old raw scan, metrics negotiation, fragment/operator version mismatch,
   missing index readiness, and fallback that preserves both schema and order. Exercise old/new
   Coordinator and Worker combinations, actual negotiated version gates in both message directions,
   query-wide legacy fallback, required-new rejection, reconnect, and unchanged legacy semantics.
6. **Selection/routing suite:** the same equality/IN/AND/OR/NULL cases across routing, key filters,
   and node selection; contradictory/empty domains; scope-gated hashing; canonical/base IDs;
   plain versus generational node IDs; role restrictions; many keys sharing one partition;
   disjoint requested/owned ranges; offline universes; target changes; and zero RPCs for no work.
   Test both under- and over-pruning and row multiplicity, not just final plan formatting.
7. **Consolidation suite:** drive representative persisted, live, node, and metadata sources
   through one planner/binder and the real local/remote transport. Check single-copy metadata,
   partial-result warnings, strict source failures, and diagnostics after physical rewrites.
   Audit the new-engine migration manifest and the isolated, version-gated legacy allowlist before
   declaring M5 complete. Remove the allowlist and its implementations only at protocol retirement.
   Verify original `QueryId` propagation through Open, child streams, runtime updates, storage tasks,
   metrics, and completion, including local execution on the Coordinator's node.
8. **Partial-results suite:** default-off behavior; explicit true/false and invalid settings;
   parallel and subsequent request isolation; failure before first batch versus after a delivered
   prefix; scalar/grouped aggregates and Top-K; all-failed versus genuinely empty scans; retained
   filter evidence; fatal corruption/expression errors; final diagnostics and disconnects on each
   supported API. Verify unsupported partial execution shapes cannot silently swallow errors.

Retain I's actual-work assertions. Returning the right rows after a full scan must not pass a
test intended to prove bounded index access. Its Top-K regression checks both candidate completeness
and the number of keys/seeks, and is a good pattern to keep. [I14]

Query diagnostics should carry the original `QueryId` and child source/stream identities.
EXPLAIN should show source scope, derived selection, frozen targets, fanout/lane decisions,
completeness policy, selected access path and fallback, index identity, covered/missing columns,
static constraints, residual location, requested/guaranteed ordering, prefix expansion strategy,
runtime-filter consumers, and where local versus global limits/aggregation run. Worker-bound raw
prefix bytes belong in trace diagnostics, not claims that EXPLAIN opened an iterator.

Reuse existing `storage_keys_visited`, `storage_seek_count`, `storage_next_count`,
`storage_prev_count`, `storage_iterator_bytes`, `storage_records_emitted`, and completion/reporting
semantics. Add measurements for primary lookups, avoided materialization, update freshness,
buffered work, and fallback only when their accounting is well defined. Missing remote metrics
mean unavailable, not zero; iterator bytes are not physical disk bytes. [I6]

Benchmark full scans against index paths across selectivity, covering ratio, K, number of parent
prefixes/partitions, lookup batch sizes, RTT, and memory limits. Measure first-result latency,
total time, keys visited, lookup work, network bytes, and peak memory. Exact ordering can remove
a sort's implicit buffering, so bounded parallel prefetch should be measured rather than removed
in the name of filter freshness. [D2]

At implementation time use `cargo nextest run`, targeted real-storage and transport suites,
`cargo check`, formatting, and Clippy. Run the workspace's required all-feature checks before
committing code. This design-only change does not claim new runtime test or benchmark results.

## 9. Decisions made here and decisions still open

### Proposed defaults

- Public base tables retain their logical identity; indexes are physical access alternatives.
- Query Coordinator and Query Worker are per-query execution roles; a Restate node may fulfill both.
- One original `QueryId` identifies the execution and is preserved for downstream attribution.
- One source-declaration, predicate-analysis, work-selection, placement, and execution framework
  serves persisted partitions, leader-live state, per-node introspection, metadata, and offline scans.
- Consolidation covers all new-engine sources by M5. Keep the old engine unchanged behind the
  legacy protocol path until protocol deprecation triggers one explicit cleanup commit.
- Explicit scan and lookup stages; standard DataFusion relational operators where possible.
- Physical byte predicates are prepared locally from semantic constraints.
- Same execution implementation for local and remote subtrees.
- Typed execution scopes: initial storage units select one partition range, and all units sharing
  `(QueryId, PartitionId)` reuse the Worker parent's read view; per-node data selects node generations
  and metadata selects one coordinator-local scope. Cross-partition snapshots may differ.
- Coordinator-side stream drop is assumed to notify the Worker to clean up its subtree. Timeout,
  keepalive, lease, and notification-delivery mechanisms are out of scope.
- Conservative residuals and a primary fallback until eligibility/exactness are proved.
- True ordered index access and Top-K are first-class milestones, not deferred behind joins.
- Capability negotiation, readiness, and ownership are separate validations.
- Bounded asynchronous flow control; no lockstep filter-freshness requirement.
- Optional partial partition results are disabled by default and enabled only in an isolated query
  request. Retained-prefix streaming and terminal completeness are explicit parts of that contract.

### Open decisions, with an explicit resolution gate

| Decision | What is known / what must be decided | Gate |
| --- | --- | --- |
| Network protocol allocation and retirement | Mechanism is decided: negotiated version gates new messages/responses, legacy remains until protocol deprecation. Choose the next unused version, audit deployed experiments, and schedule the minimum-version bump/cleanup. | M0-M5; retirement later |
| Mixed-version query selection | Proposed initial policy is whole-query new or legacy mode before reading data. Finalize when legacy semantics are permitted and how callers require new-only guarantees; never downgrade individual running scans. | M5 / C14 |
| Query identity plumbing | Representation is decided: Coordinator-allocated ULID-backed `ResourceId`, prefix `qry`, using the shared versioned encoding. Finalize wire placement and explicit versus stream-bound propagation, preserving separation from child scanner IDs. | M0-M5 |
| Source policies and diagnostics | Preserve declared role eligibility, suitable-target versus leader requirements, and strict/best-effort semantics. Resolve contradictory comments from executable behavior; define incomplete-result handling for each query response API. | M0-M2 |
| Partial-results query interface | Finalize the proposed setting name and restricted SET preamble, failure categories, initial eligible plan shapes, and Arrow/HTTP terminal reporting. No shared-session mutation; no silent incomplete success. | M5a |
| First authoritative base-table/index mapping | Existing index populations and canonical locators are known; choose one mapping and prove row/value equivalence rather than assuming every invocation table matches. | M2-M3 |
| Read-view ownership API | Scope is agreed: Worker parent owns one read context per `(QueryId, PartitionId)` across all child scans. Choose the owned handle and scoped task/thread implementation without borrowing the processor's mutable transaction or running the query on its loop. | M2 |
| Backfill protocol | Maintenance exists, backfill does not. Choose a snapshot + catch-up or generation/rebuild procedure compatible with lifecycle writes and `SingleDelete`; prove publication correctness. | M3 |
| First exact SQL orderings | Natural key orders exist; certify SQL mappings individually, especially timestamp truncation and formatted IDs. Reverse prepared navigation is additional work. | M4 |
| Remote expression portability | Decide the initial portable function/option set and semantic context negotiation; schema equality is not sufficient. | M5 |
| Worker readiness visibility | Choose how planning learns capabilities and how cached information is revalidated at open; keep contract-preserving fallback for stale information. | M3-M5 |
| Runtime update transport and budgets | `Next` piggybacking is available; select the independent control mechanism and measured buffering budget for blocking work. | M6 |
| Larger execution units | Decide per-node partition batching, merge concurrency, and failure granularity from measurements; colocated join eligibility is a separate semantic proof. | M7-M8 |

Release notes should accompany implemented user-visible milestones: index activation/backfill,
automatic access selection, ordering/Top-K behavior, and changed compatibility or retry behavior.
M5a also needs release notes explaining the default-off option, retained partial partitions,
incomplete aggregate/Top-K meaning, supported clients, and how completion is reported.
Follow `release-notes/README.md:16-46,109-125`; do not publish this proposed design as shipped behavior.

## Source map

All line ranges below were checked in the named revision. The design uses conceptual operator
labels intentionally; `QueryId` is an agreed addition, and only identifiers attributed to sources
claim to exist in code.

- **[I1]** I: `crates/storage-api/src/filter.rs:28-168`; `crates/storage-query-datafusion/src/filter/typed.rs:34-47,138-229,274-331` — logical constraints and SQL translation.
- **[I2]** I: `crates/partition-store/src/keys/predicate.rs:17-87,141-168,171-230`; `crates/partition-store/src/keys/filter.rs:322-397` — prepared literals, interval unions, schema binding.
- **[I3]** I: `crates/partition-store/src/keys/filter.rs:400-495,520-609`; `crates/rocksdb/src/iterator.rs:28-38,74-90` — bounds, forward cursor, live seeks, lower iterator actions.
- **[I4]** I: `crates/storage-api/src/filter.rs:124-132`; `crates/storage-query-datafusion/src/filter/typed.rs:50-135,175-192`; `crates/storage-query-datafusion/src/index/table.rs:29-53`; `crates/partition-store/src/index/scan.rs:39-41,180-218` — live filter semantics and polling.
- **[I5]** I: `crates/partition-store/src/index.rs:39-77`; `crates/partition-store/src/index/entry.rs:22-84`; `crates/partition-store/src/index/virtual_object.rs:21-58`; `crates/partition-store/src/index/maintenance.rs:16-80`; `crates/partition-store/src/vqueue_table/index.rs:84-160` — layouts, membership, and maintenance.
- **[I6]** I: `crates/storage-query-datafusion/src/scan_metrics.rs:29-147`; `crates/types/src/net/remote_query_scanner.rs:96-168` — metric meanings, cumulative reports, completion.
- **[I7]** I: `docs/dev/datafusion-index-filtering.md:61-106,259-280,304-326`; `docs/dev/ordered-key-filtering.md:109-157`; `docs/dev/journey-of-stat-query.md:296-311` — prior design and stats row expansion. Consult implementation for later live-seek behavior.
- **[I8]** I: `crates/storage-query-datafusion/src/table_providers.rs:44-63`; `crates/storage-query-datafusion/src/scanner_task.rs:76-101` — access predicate versus mutable remote wrapper.
- **[I9]** I: `crates/storage-query-datafusion/src/index/table.rs:56-82`; `crates/storage-query-datafusion/src/index/entry_by_stage/table.rs:29-55,88-114` — inspection registration and lazy projection.
- **[I10]** I: `crates/partition-store/src/features/indexes_v1.rs:23-56`; `crates/partition-store/src/vqueue_table/index.rs:84-87`; `release-notes/unreleased/stage-index-tables.md:7-38,40-55` — readiness gap, populations, inspection SQL.
- **[I11]** I: `crates/partition-store/src/vqueue_table/mod.rs:720-768,771-850`; `crates/storage-api/src/vqueue_table/entry_status.rs:109-127,182-216` — base-key multi-get and canonical identity reconstruction.
- **[I12]** I: `crates/partition-store/src/partition_store.rs:604-632,1016-1023`; `crates/partition-store/src/index/scan.rs:180-219`; `crates/partition-store/src/vqueue_table/mod.rs:807-834` — existing snapshot support and separate scan/lookup read options.
- **[S1]** I: `crates/partition-store/src/vqueue_table/mod.rs:433-473`; `crates/partition-store/src/index/maintenance.rs:16-80` — primary entry status and secondary-index updates through the same transaction. [RocksDB snapshots](https://github.com/facebook/rocksdb/wiki/Snapshot) and [iterator read views](https://github.com/facebook/rocksdb/wiki/Iterator#consistent-view) explain why all related reads must use one explicit snapshot.
- **[I13]** I: `crates/partition-store/src/keys/filter/codec/timestamp.rs:35-62,100-168`; `crates/partition-store/src/keys/index_key_codec.rs:50-74,141-160`; `crates/storage-query-datafusion/src/index/entry_by_stage/table.rs:93-111`; `release-notes/unreleased/stage-index-tables.md:30-33` — timestamp and ID representations.
- **[I14]** I: `crates/storage-query-datafusion/src/index/tests.rs:1153-1264` — live Top-K correctness and actual seek/visit assertions.
- **[F1]** F: `crates/storage-query-datafusion/src/partition_planning.rs:41-136`; `crates/storage-query-datafusion/src/partitioned_scan.rs:82-164,423-486,500-528,580-582` — placement, lanes, per-partition remote scans, opaque boundary.
- **[F2]** F: `crates/storage-query-datafusion/src/remote_fragment.rs:124-195,238-278,425-462,492-578` — single-input validation, codec, binding, recoverable setup.
- **[F3]** F: `crates/storage-query-datafusion/src/scan_fragment.rs:67-126`; `crates/storage-query-datafusion/src/partial_aggregation.rs:59-193,220-265` — supported relational rewrites and reduction.
- **[F4]** F: `crates/storage-query-datafusion/src/partitioned_scan.rs:531-554`; `crates/storage-query-datafusion/src/context.rs:682-698,752-768` — cleared ordering and optimizer placement.
- **[F5]** F: `crates/storage-query-datafusion/src/scanner_task.rs:182-241`; `crates/storage-query-datafusion/src/remote_query_scanner_server.rs:40-45,84-94`; `crates/storage-query-datafusion/src/remote_query_scanner_client.rs:154-182,258-330,412-432` — cancellation, bounded stream, and update delivery.
- **[W1]** I: `crates/types/src/net/remote_query_scanner.rs:60-63`; F: `crates/types/src/net/remote_query_scanner.rs:60-75` — incompatible independent use of Open tag 9.
- **[U1]** F: `crates/core/src/partitions.rs:17-55,57-97`; `crates/storage-query-datafusion/src/remote_query_scanner_manager.rs:73-129,215-249`; `crates/storage-query-datafusion/src/context.rs:145-148,732-745` — routing authority, leader distinction, partition universes.
- **[U2]** F: `crates/storage-query-datafusion/src/filter.rs:35-400,403-701`; `crates/storage-query-datafusion/src/table_providers.rs:217-283` — routing derivations, duplicated expression interpretation, point/grouped fanout.
- **[U3]** F: `crates/storage-query-datafusion/src/invocation_state/table.rs:74-85,95-143`; `crates/storage-query-datafusion/src/scheduler_status/table.rs:79-90,109-159`; `crates/storage-query-datafusion/src/user_limits/table.rs:75-86,105-142` — live source range binding and execution.
- **[U4]** F: `crates/storage-query-datafusion/src/node_fan_out.rs:56-80,153-222,271-315,319-337,421-489,553-590`; `crates/storage-query-datafusion/src/remote_query_scanner_manager.rs:202-209,276-294`; `crates/storage-query-datafusion/src/context.rs:707-729`; `crates/admin/src/cluster_controller/grpc_svc_handler.rs:592-652` — node selection, sentinel RPC, partial-result policy and warning plumbing.
- **[U5]** F: `crates/storage-query-datafusion/src/context.rs:232-325,360-395` — user/cluster registration inventory. Node policies: `crates/storage-query-datafusion/src/loglet_worker/table.rs:23-42`, `bifrost_read_stream/table.rs:23-48`, `config/table.rs:34-51`. Coordinator-local registrations under `crates/storage-query-datafusion/src/`: `deployment/table.rs:39-44`, `service/table.rs:38-44`, `rules/table.rs:36-43`, `node/table.rs:36-43`, `partition/table.rs:36-43`, `partition_replica_set/table.rs:38-46`, `log/table.rs:33-35`, `partition_state/table.rs:34-38`.
- **[U6]** F: `crates/storage-query-datafusion/src/context.rs:405-418`; `crates/storage-query-datafusion/src/remote_query_scanner_manager.rs:120-170`; `tools/restate-doctor/src/commands/snapshot/mod.rs:140-148,329-335`; `crates/storage-query-datafusion/src/mocks.rs:159-181` — offline/test universes and preserved live-state behavior.
- **[U7]** I: `crates/storage-query-datafusion/src/stats/service_stats/table.rs:34-62`; `stats/deployment_stats/table.rs:36-65`; `stats/virtual_object_stats/table.rs:33-62`; `crates/storage-query-datafusion/src/index/entry_by_stage/table.rs:44-55`; `crates/storage-query-datafusion/src/index/table.rs:56-82`; `release-notes/unreleased/stage-index-tables.md:7-28` — stats/index selection and consumer inventory. Relative stats paths share `crates/storage-query-datafusion/src/`.
- **[Q4]** F: `crates/types/src/identifiers.rs:1059-1186`; `crates/types/src/id_util.rs:26-63` — existing ULID-backed `ResourceId` generation, parsing/formatting, and versioned resource-prefix schema; `qry` is the agreed new prefix to register.
- **[P1]** F/main baseline: `crates/types/protobuf/restate/common.proto:14-28`; `crates/types/src/net/mod.rs:24-28`; `crates/core/src/network/handshake.rs:59-84`; `crates/core/src/network/connection.rs:368-375` — supported range, mutual version selection, and per-connection version access.
- **[P2]** F/main baseline: `crates/types/src/net/mod.rs:97-138`; `crates/core/src/network/incoming.rs:37-59,745-759` — generic codec minimum, incoming version context, and response-encoding assumption.
- **[Q1]** F: `crates/storage-query-datafusion/src/context.rs:641-670`; `crates/admin-rest-model/src/query.rs:14-20`; `crates/core/protobuf/cluster_ctrl_svc.proto:187-190` — current SQL-string requests and single-statement execution path.
- **[Q2]** DataFusion 55.1.0: [single-statement parser](https://github.com/apache/datafusion/blob/55.1.0/datafusion/core/src/execution/session_state.rs#L458-L481); [SET mutates session configuration](https://github.com/apache/datafusion/blob/55.1.0/datafusion/core/src/execution/context/mod.rs#L1109-L1139); [configuration extension and independent cloning](https://github.com/apache/datafusion/blob/55.1.0/datafusion/common/src/config.rs#L2245-L2276) — basis for request-local option handling.
- **[Q3]** F: `crates/core/protobuf/cluster_ctrl_svc.proto:192-205`; `crates/admin/src/cluster_controller/grpc_svc_handler.rs:424-459,592-652`; `crates/admin/src/rest_api/query.rs:135-167,197-229` — existing gRPC warnings and HTTP/JSON streaming envelope.
- **[D1]** [DataFusion custom table providers](https://datafusion.apache.org/library-user-guide/custom-table-providers.html); [55.1.0 TableProvider scan contract](https://docs.rs/datafusion-session/55.1.0/datafusion_session/table/trait.TableProvider.html#tymethod.scan); [external index integration](https://datafusion.apache.org/blog/2025/08/15/external-parquet-indexes/) — planning/execution separation and index-based access.
- **[D2]** [55.1.0 try_pushdown_sort](https://docs.rs/datafusion-physical-plan/55.1.0/datafusion_physical_plan/execution_plan/trait.ExecutionPlan.html#method.try_pushdown_sort); [55.1.0 PushdownSort implementation](https://github.com/apache/datafusion/blob/55.1.0/datafusion/physical-optimizer/src/pushdown_sort.rs#L18-L53); [sort pushdown and buffering](https://datafusion.apache.org/blog/2026/07/20/sort-pushdown/) — exact/inexact ordering and streaming merge.
- **[D3]** [55.1.0 ExecutionPlan](https://docs.rs/datafusion-physical-plan/55.1.0/datafusion_physical_plan/execution_plan/trait.ExecutionPlan.html); [55.1.0 sanity checker](https://github.com/apache/datafusion/blob/55.1.0/datafusion/physical-optimizer/src/sanity_checker.rs#L143-L189) — properties, execution lifecycle, and final validation.
- **[D4]** [Dynamic filters](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters); [55.1.0 AggregateMode](https://docs.rs/datafusion-physical-plan/55.1.0/datafusion_physical_plan/aggregates/enum.AggregateMode.html); [Ballista architecture](https://datafusion.apache.org/ballista/contributors-guide/architecture.html#distributed-query-scheduling) — runtime feedback, aggregate state, and later distributed-stage extensions.
