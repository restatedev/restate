# Journey of a stat query

This walkthrough follows a `sys_service_stats` SQL query from planning to RocksDB
iteration and back to the result stream. It also records an error-propagation issue
identified during a source-level review.

For the relationships between table markers, logical schemas, key codecs, and
generated types, see [Ordered-key filtering: types and macros](ordered-key-filtering.md).

## Example query

```sql
SELECT service_name, handler, kind, stage, status, num_entries
FROM sys_service_stats
WHERE service_name = 'LargeState'
  AND handler LIKE 'get%'
  AND kind = 'invocation'
  AND stage = 'running';
```

The main representation changes are:

```text
SQL
 → DataFusion logical plan
 → partitioned physical execution plan + PhysicalExpr
 → Filter<ServiceLoad>
  → PreparedKeyFilter<ServiceLoadKey>
  → KeyFilterCursor<ServiceLoadKey>
 → PhysicalScan<Bytes> + RocksDB ReadOptions
 → RocksIterator
 → DBRawIteratorWithThreadMode
```

## 1. Parse and plan the SQL

`QueryContext::execute` parses SQL using the PostgreSQL dialect, creates and verifies
the logical plan, creates a physical plan, and returns its execution stream.

Source: [context.rs:693–715](../../crates/storage-query-datafusion/src/context.rs#L693-L715).

`sys_service_stats` is an aggregate view over a partitioned raw provider. The view
groups by every dimension and sums `num_entries`, so matching partition-local rows
are combined into cluster-wide results above the scan. Dimension filters can be
pushed through this aggregate; predicates on aggregate results must retain their
SQL-level meaning.

Sources: [registration:52–62](../../crates/storage-query-datafusion/src/stats/service_stats/table.rs#L52-L62),
[aggregate view:19–46](../../crates/storage-query-datafusion/src/stats/aggregated_stat_table.rs#L19-L46).

`sys_virtual_object_stats` instead exposes partition-local counts directly and includes
`partition_key`. Its registration does not wrap the provider in the aggregate view,
so a `LIMIT` need not wait for cross-partition aggregation of all matching objects.
Each stored VO stats key produces one row with `num_inbox`, `num_running`,
`num_suspended`, `num_paused`, and `num_finished` counters. Absent stages are zero,
not NULL; this table has no `stage` or `num_entries` column.

Explicit `partition_key` selections use one scan per physical partition. The typed
filter applies the entire predicate, so per-key fanout would repeat matching rows
when multiple selected keys share a physical partition.

Sources: [VO registration](../../crates/storage-query-datafusion/src/stats/virtual_object_stats/table.rs#L52),
[VO schema](../../crates/storage-query-datafusion/src/stats/virtual_object_stats/schema.rs#L15), and
[VO row construction](../../crates/storage-query-datafusion/src/stats/virtual_object_stats/row.rs#L16).

## 2. Push predicates into the partitioned provider

`PartitionedTableProvider::scan`:

1. Constructs the projected schema.
2. Converts pushed logical expressions into physical expressions.
3. Reassigns column indices against that schema.
4. Combines the expressions into one predicate.
5. Selects physical partitions and groups them into DataFusion execution partitions.

The provider reports pushdown as `Inexact`. DataFusion retains responsibility for
exact filtering, and columns needed by filters remain available in the projection.

Sources: [table_providers.rs:153–246](../../crates/storage-query-datafusion/src/table_providers.rs#L153-L246),
[pushdown declaration:280–293](../../crates/storage-query-datafusion/src/table_providers.rs#L280-L293).

For this table, `service_name` narrows storage scans inside each selected physical
partition. It does not identify a single partition: registration uses the default
partition-key extractor, which recognizes `partition_key`, not `service_name`.

Sources: [registration:57](../../crates/storage-query-datafusion/src/stats/service_stats/table.rs#L57),
[default extractor:75–85](../../crates/storage-query-datafusion/src/filter.rs#L75-L85).

## 3. Route each physical scan locally or remotely

`PartitionedExecutionPlan::execute` invokes the distributed scanner for each physical
partition. Initially it passes the same expression as both:

- `access_predicate`: input for constructing storage constraints;
- `predicate`: live row filtering.

Source: [table_providers.rs:363–386](../../crates/storage-query-datafusion/src/table_providers.rs#L363-L386).

The scanner resolves the owning node. A local scan calls the registered local
scanner. A remote request carries the physical expression and projected schema,
rather than encoded RocksDB bounds. The receiving worker constructs its storage
filter and bounds locally.

Sources: [routing:277–333](../../crates/storage-query-datafusion/src/remote_query_scanner_manager.rs#L277-L333),
[remote request:167–191](../../crates/storage-query-datafusion/src/remote_query_scanner_client.rs#L167-L191).

On the remote worker, the original decoded expression remains available for access
planning, while a `DynamicFilterPhysicalExpr` wrapper receives subsequent row-filter
updates. This separation lets static constraints survive remote wrapping: the
storage translator can inspect the original conjuncts without mistaking the entire
expression for dynamic state.

Sources: [scanner_task.rs:74–98](../../crates/storage-query-datafusion/src/scanner_task.rs#L74-L98),
[predicate updates:154–170](../../crates/storage-query-datafusion/src/scanner_task.rs#L154-L170).

## 4. Construct the logical storage filter

`LocalPartitionsScanner` calls `S::Filter::new`. For service stats, that associated
type is `Filter<ServiceLoad>`.

Sources: [partition_store_scanner.rs:100](../../crates/storage-query-datafusion/src/partition_store_scanner.rs#L100),
[service_stats/table.rs:68–72](../../crates/storage-query-datafusion/src/stats/service_stats/table.rs#L68-L72).

The generic translator extracts supported static conjuncts. For the example query,
the logical clauses are:

```text
ServiceName(Equal("LargeState"))
HandlerStartsWith("get")
Kind(Equal(Invocation))
```

DataFusion can rewrite a short literal IN list into an OR of equalities. The
translator normalizes same-column literal disjunctions back into a finite set, so
an OR in `EXPLAIN` can still produce bounded scans and IN-gap seeks. An OR with
different columns or unsupported arms remains entirely residual.
See [the shared normalizer](../../crates/storage-query-datafusion/src/filter.rs#L407)
and [typed extraction](../../crates/storage-query-datafusion/src/filter/typed.rs#L49).

`stage = 'running'` remains in the complete row predicate. Dynamic conjuncts are
excluded from this one-time storage preparation. The complete predicate is retained
for row filtering; extraction does not remove the supported conjuncts from it.

Sources: [translation:34–101](../../crates/storage-query-datafusion/src/filter/typed.rs#L34-L101),
[static-conjunct selection:482–488](../../crates/storage-query-datafusion/src/filter.rs#L482-L488),
[row-filter construction:117–118](../../crates/storage-query-datafusion/src/partition_store_scanner.rs#L117-L118).

## 5. Bind logical clauses to physical codecs

`ServiceStatsScanner` calls `scan_service_load`, which invokes the generated
`ServiceLoadKey::prepare_filter`.

The generated binding:

- inspects prefix clauses through `FilterTarget::starts_with_prefix`;
- dispatches by logical field to the physical codec;
- validates and encodes ordinary value predicates;
- constructs `PreparedKeyFilter<ServiceLoadKey>`.

Sources: [scanner:84–86](../../crates/storage-query-datafusion/src/stats/service_stats/table.rs#L84-L86),
[storage entry point:41–50](../../crates/partition-store/src/stats/aggregated/scan.rs#L41-L50),
[generated binding:250–279](../../crates/partition-store/src/keys/macros.rs#L250-L279).

Preparation arranges predicates in physical field order and checks intersections
for contradictions. Equality, IN, and
range literals share an owned byte buffer. Prefix predicates retain their encoded
prefix matcher and half-open byte bounds, presence-tagged for nullable fields.

Sources: [preparation](../../crates/partition-store/src/keys/filter.rs#L310),
[prefix preparation:74–87](../../crates/partition-store/src/keys/filter.rs#L74-L87).

## 6. Derive the iterator bounds

The physical key order is:

```text
StatKeyPrefix | service_name | handler | kind
```

Source: [service-load key declaration:24–31](../../crates/partition-store/src/stats/aggregated/service_load.rs#L24-L31).

For the example query, let `encode` denote the relevant order-preserving field
encoding, and `||` denote byte concatenation:

```text
H = encoded StatKeyPrefix for this partition and statistic
P = H || encode("LargeState")

lower, inclusive = P || 0x01 || encode("get") || encode(Invocation)
upper, exclusive = P || 0x01 || bytes("geu")
```

`0x01` is the nullable handler's presence tag. The service equality fixes the parent
prefix. The handler prefix supplies a complete inclusive minimum, so the lower
bound can also append the kind minimum. The upper envelope stops at the handler's
exclusive string-prefix boundary. The codec removes terminal padding and the final
marker, retains earlier continuation markers, then increments the last payload byte.
The result is a byte boundary, not a complete encoded field or necessarily valid UTF-8.

The lower endpoint uses the complete string encoding of `"get"`. Prefix matching
itself is codec-aware; the complete encoding of `"get"` is not simply a byte prefix
of every encoded string beginning with those characters.

Sources: [nullable prefix encoding:74–87](../../crates/partition-store/src/keys/filter.rs#L74-L87),
[bound construction](../../crates/partition-store/src/keys/filter.rs#L365),
[encoded string bounds](../../util/string/src/mem_comparable_string.rs#L229).

### An ordered-comparison variation

For `service_name >= 'LargeState'`, the lower bound is instead
`H || encode("LargeState")`, and the upper bound is the end of `H`'s region.
If later fields also have lower bounds, their minima can extend the initial lower
bound. Exhausting a handler prefix carries into the service field: the cursor seeks
past the current service, discovers the next stored service, then refines its handler.

Sources: [bounds](../../crates/partition-store/src/keys/filter.rs#L365),
[cursor carry](../../crates/partition-store/src/keys/filter.rs#L509).

On the SQL path, `PreparedKeyFilter::into_cursor` invokes `scan` to construct bounds.
The generated prefix builder serves explicit prefix-construction callers. Both use
the field codecs; the SQL scanner supplies the fixed `StatKeyPrefix` directly.

Source: [aggregated/scan.rs:95–108](../../crates/partition-store/src/stats/aggregated/scan.rs#L95-L108).

## 7. Configure and schedule the RocksDB iterator

`iterator_controlled_physical` consumes the already-encoded scan.
`run_iterator_internal` selects prefix/range read options and starts at
`IterAction::Seek(lower)`.

The iterator uses the physical partition's column family. `TableKind` validates the
key-kind region rather than selecting a separate stat column family.

Source: [partition_store.rs:508–561](../../crates/partition-store/src/partition_store.rs#L508-L561).

For a range, `ScanMode::from_range` compares the endpoints' fixed prefixes. Matching
fixed prefixes allow `WithinPrefix`; otherwise it selects `TotalOrder`. Read options
receive the lower and exclusive upper bounds. Prefix scans use `PrefixRange`, and
prefixes shorter than the configured fixed-prefix length require total-order seeking.

Sources: [ScanMode:949–964](../../crates/partition-store/src/partition_store.rs#L949-L964),
[read options:57–81](../../crates/partition-store/src/lib.rs#L57-L81).

The stat scanner requests low priority and enables async I/O. The background iterator
allows blocking I/O with `ReadTier::All` and runs on the low-priority storage thread
pool. That worker creates the underlying iterator with:

```rust
raw_iterator_cf_opt(&cf, read_options)
```

Sources: [stat iterator options:100–108](../../crates/partition-store/src/stats/aggregated/scan.rs#L100-L108),
[iterator creation:345–397](../../crates/rocksdb/src/lib.rs#L345-L397),
[thread-pool dispatch:381–388](../../crates/rocksdb/src/db_manager.rs#L381-L388).

## 8. Drive the iterator and evaluate candidates

`RocksIterator::step` executes `seek`, `next`, or `stop`, then supplies borrowed
key/value slices to the callback. Iterator errors are delivered through that callback.

Source: [rocksdb/iterator.rs:67–112](../../crates/rocksdb/src/iterator.rs#L67-L112).

The stat callback checks the encoded key before deserializing the value:

| Result | Action |
| --- | --- |
| `Match` | Deserialize the aggregate and invoke the row callback if the aggregation accepts it. |
| `Seek(target)` | Seek to an absolute, forward, in-bounds target supplied by the cursor. |
| `Done` | Stop the iterator. |

Source: [aggregated/scan.rs](../../crates/partition-store/src/stats/aggregated/scan.rs#L129).

For the example's fixed service, the iterator's upper bound excludes handlers beyond
the `get` prefix. With varying preceding fields, prefix exhaustion carries into the
parent so later groups remain reachable. A failed kind predicate on a handler
that matches `get%` advances the handler rather than exhausting the entire scan.

Source: [evaluation and carry](../../crates/partition-store/src/keys/filter.rs#L480).

For a finite domain at any depth, the filter uses binary search to find later
candidates and checks all constraints on that field before selecting one. A
complete candidate lets the cursor append the same lower suffix used for initial
positioning. If no candidate remains, it carries left and resets the suffix.

Continuous domains supply a boundary after a whole value group instead of guessing
the next stored value. No suffix may be appended behind that boundary; the next
iterator visit supplies the actual value to refine. Field-local byte overflow also
carries left, never into the fixed partition/stat identity. Both the cursor and
controlled iterator adapter validate forward progress and scan bounds. Ordinary
for-each callers continue to use `ControlFlow` with unit continuation values.

Sources: [controlled scan adapter](../../crates/partition-store/src/partition_store.rs#L524),
[field advancement](../../crates/partition-store/src/keys/filter.rs#L224),
[iterator actions:28–36](../../crates/rocksdb/src/iterator.rs#L28-L36).

## 9. Return rows and enforce the complete predicate

A matching stored aggregate can expand into multiple stage/status rows. `BatchSender`
applies the complete live predicate to the Arrow batch, then coalesces and sends it.
Its limit handling occurs after that filtering. A bounded batch channel provides
backpressure to the storage-thread producer.

Sources: [row expansion:21–33](../../crates/storage-query-datafusion/src/stats/service_stats/row.rs#L21-L33),
[batch filtering and sending:123–175](../../crates/storage-query-datafusion/src/table_util.rs#L123-L175),
[channel construction:102–103](../../crates/storage-query-datafusion/src/partition_store_scanner.rs#L102-L103).

These rows feed the public aggregate view, which computes the final cross-partition
sums. The storage filter narrows candidate keys; it does not replace the complete
SQL predicate or the public aggregation.

Source: [aggregate view:25–46](../../crates/storage-query-datafusion/src/stats/aggregated_stat_table.rs#L25-L46).

## Earlier review finding: fallible row conversion was unwrapped

This finding has been fixed: stats callbacks now preserve `ControlFlow<Result<()>>`,
and each query adapter maps conversion errors into `StorageError`. The history below
explains why the fallible callback contract matters.

Sources: [stats callback contract](../../crates/partition-store/src/stats/aggregated/scan.rs#L93),
[service adapter](../../crates/storage-query-datafusion/src/stats/service_stats/table.rs#L85),
[deployment adapter](../../crates/storage-query-datafusion/src/stats/deployment_stats/table.rs#L88),
[virtual-object adapter](../../crates/storage-query-datafusion/src/stats/virtual_object_stats/table.rs#L85).

At the time of this review, all three stats scanners adapt their row callback with:

```rust
f((key_decoder, value)).map_break(Result::unwrap)
```

Their `append_row` methods can return a `ConversionError` when decoding a stored key,
so the unwrap can panic on the storage thread. For example, an unfiltered scan can
encounter an invalid encoded entry-kind value, which the owned decoder rejects.

Sources: [service scanner:84–95](../../crates/storage-query-datafusion/src/stats/service_stats/table.rs#L84-L95),
[deployment scanner:87–98](../../crates/storage-query-datafusion/src/stats/deployment_stats/table.rs#L87-L98),
[VO scanner:84–95](../../crates/storage-query-datafusion/src/stats/virtual_object_stats/table.rs#L84-L95),
[entry-kind decoder](../../crates/partition-store/src/keys/index_key_codec.rs#L100).

The consequence can extend beyond a panic: iterator completion uses a oneshot error
channel, and a dropped sender is interpreted as success. Under unwinding, the
conversion error can therefore be lost and the scan appear successfully completed
with partial results. The native storage task runs separately from the DataFusion
task that awaits that channel.

Sources: [completion handling:516–525](../../crates/partition-store/src/partition_store.rs#L516-L525),
[storage-thread dispatch:381–388](../../crates/rocksdb/src/db_manager.rs#L381-L388).

The recommended fix is to preserve a fallible callback result through the stats
scan API and use the existing iterator error channel. It already handles
`ControlFlow::Break(Err(...))`. A regression test should verify that a malformed
matching key produces a query error rather than a successful partial result.

Source: [iterator error handling:435–459](../../crates/partition-store/src/partition_store.rs#L435-L459).

## Responsibility summary

- SQL translation defines logical conditions.
- Key preparation defines physical bounds and raw-key matching.
- The iterator executes the scan.
- Batch filtering and the aggregate view preserve the full SQL semantics.
