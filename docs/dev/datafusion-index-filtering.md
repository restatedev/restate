# DataFusion index filtering

## Status and goal

This document records the agreed design direction and a proposed implementation sequence. It is
not a description of an already implemented generic index executor; API names and exact trait
shapes remain to be decided. See [DataFusion Query Engine](dev/datafusion.md) for the surrounding
query architecture.

Stats are the first consumer of ordered-key filtering infrastructure that should also serve:

- secondary indexes exposed directly as SQL tables;
- secondary indexes used to accelerate queries on existing tables; and
- native storage callers that supply constraints without DataFusion expressions.

The reusable abstraction is an ordered key schema plus a compiled key predicate. Initial iterator
bounds, raw-key filtering, and safe seeks must derive from the same constraints. Table-level code
chooses the access path and handles matching entries.

```text
SQL predicates or native constraints
                 |
       constraints identified by field tag
                 |
           bind to a key schema
                 |
       compiled key predicate + access plan
          /                       \
initial iterator bounds       raw-key evaluation/navigation
          \                       /
             candidate entries
                    |
       table-specific materialization or primary lookup
                    |
          remaining row/value predicates
```

## Existing building blocks

- `define_aggregated_stat` knows field order, names, codecs, and borrowed representations. It
  generates sequential prefix builders and progressive decoders with field tags
  ([stats/macros.rs:64](../crates/partition-store/src/stats/macros.rs#L64),
  [stats/macros.rs:259](../crates/partition-store/src/stats/macros.rs#L259)).
- `IndexFieldEncode` provides order-preserving field encoding; `IndexFieldDecode` and
  `FieldDecoder` allow encoded fields to be inspected without constructing an owned key
  ([keys/index.rs:24](../crates/partition-store/src/keys/index.rs#L24),
  [keys/index.rs:33](../crates/partition-store/src/keys/index.rs#L33)).
- `define_table_key` has optional-field builders that serialize through the first unset field.
  Its `BuilderRef` uses the existing table-key codecs rather than the statistic field codecs
  ([keys.rs:418](../crates/partition-store/src/keys.rs#L418),
  [keys.rs:459](../crates/partition-store/src/keys.rs#L459)).
- `PhysicalScan` represents prefix scans and exclusive-upper-bound ranges
  ([scan.rs:29](../crates/partition-store/src/scan.rs#L29)).
- The RocksDB iterator callback already supports `IterAction::Seek(Bytes)`. The partition-store
  `iterator_for_each` adapter currently exposes continue/stop/error instead
  ([iterator.rs:28](../crates/rocksdb/src/iterator.rs#L28),
  [partition_store.rs:421](../crates/partition-store/src/partition_store.rs#L421)).

The design should build on these components, preserving their persisted encodings.

## Responsibilities

### Query adapter

The query layer normalizes supported predicates into storage-neutral constraints and retains the
remaining SQL predicate. It owns SQL typing, NULL semantics, pattern escaping, and the mapping
from SQL columns to key-field tags. Unknown mappings or unsupported expressions are residuals,
not evidence that the result is empty.

Conjuncts may be intersected. Same-field alternatives may form a finite set or interval union.
Correlated alternatives across fields must not be mistaken for an exact Cartesian product: any
broader extracted constraint remains a necessary condition, with the original predicate retained
for correctness.

New stats/index access planning initially uses static constraints. Live dynamic predicates can
still filter rows. Enabling dynamic storage reseeking would require a separate correctness
contract; adoption of this mechanism must preserve existing tables' dynamic point-lookup behavior.

### Key schema and compiler

The storage layer owns field ordering, codecs, physical identity, legal boundaries, and raw-key
interpretation. The shared schema contract should support:

- field tags and positions in declared key order;
- field types, nullability, and appropriate borrowed input representations;
- locating complete encoded fields and comparing their values;
- encoding complete fields into prefixes; and
- constructing validated bounds around complete prefixes.

The macros should generate implementations of this contract. Shared algorithms should perform
domain binding, bound construction, and key evaluation. Adding another key composed of supported
field codecs should not require another handwritten `service_stat_scan`-style traversal.

Generate typed steps or a typed visitor: merely exposing a tag does not solve the changing input
types and builder states between fields. Start with `define_aggregated_stat`; make it an adapter
to the shared contract rather than the owner of a stats-specific algorithm. Adapt
`define_table_key` when a concrete consumer needs it, without changing its codec semantics.

### Table-level planning and execution

The table selects an applicable key schema/access path, supplies physical partition or index
identity, and chooses an execution strategy supported by that path. It also decides whether a
candidate entry can become a row directly or requires a primary lookup.

Partition routing and local access planning are separate: narrowing a secondary-key interval
does not by itself establish which Restate partitions contain matching rows.

## Constraints and binding

Preserve the exact supported field constraints until access planning:

| Constraint | Meaning |
| --- | --- |
| Unconstrained | No usable restriction on the field |
| Empty | No admissible value can satisfy the extracted constraint |
| Exact value / finite set | Preserve membership, including holes in an `IN` list |
| Ordered interval | Independent inclusive, exclusive, or unbounded endpoints |
| String prefix | Case-sensitive prefix of a string value |

Equality is a singleton set. SQL NULL is a value only where the field permits it: an exact NULL
constraint must remain distinct from both an absent constraint and an empty domain. Ordinary
comparisons with NULL cannot be treated as `IS NULL`. An empty string is an ordinary string value.

At binding time, resolve tags to field positions, convert values using field-specific semantics,
and encode/normalize constraints in the field codec's order. After binding, raw-key evaluation
should not repeat tag lookups, parsing, or literal encoding for every entry.

`FromStr` alone does not establish ordering or SQL equivalence. For example, textual ordering of
`"10"` and `"2"` differs from numeric ordering. A conversion failure may eliminate a value only
when it proves that value cannot match the indexed field; unsupported conversions require a
broader scan and residual evaluation.

Retain exact sets even when execution uses their enclosing range. Otherwise the evaluator loses
the information needed to filter holes, choose point reads, or seek to the next allowed value.
Keep the normalized predicate immutable and any navigation state local to each iterator.

## Initial bound construction

Walk the key schema in declared order, looking up the constraint for each tag. Do not build keys
by iterating the constraint map. Advance lower and upper endpoints together through legal field
boundaries.

| Field constraint | Action |
| --- | --- |
| Exact value, including exact NULL | Append the complete field and continue |
| Finite set expanded into exact prefixes | Continue separately under each selected prefix |
| Finite set represented by one enclosing range | Bound this field and stop |
| Ordered interval | Bound this field and stop |
| String-prefix constraint | Bound this field and stop |
| Unconstrained | Finish at the preceding prefix |
| Empty | Produce an empty scan |

Thus, initial bound construction stops at the first non-exact field. Constraints on later fields
remain in the key predicate. Preserve whole-predicate contradiction detection even if a preceding
gap prevents a later field from contributing iterator bounds.

Let `P` be a fixed preceding key prefix and `E(v)` the complete encoding of one field value.
`successor(Q)` denotes an exclusive boundary beyond all keys extending a complete key prefix `Q`.
For non-null ordered values, the conceptual intervals are:

| Constraint on next field | Inclusive lower | Exclusive upper |
| --- | --- | --- |
| `= v` | `P · E(v)` | `successor(P · E(v))` |
| `>= v` | `P · E(v)` | `successor(P)` |
| `> v` | `successor(P · E(v))` | `successor(P)` |
| `< v` | Start of admissible field values under `P` | `P · E(v)` |
| `<= v` | Start of admissible field values under `P` | `successor(P · E(v))` |

Nullable-field compilation must additionally exclude NULL where the operator requires it. Bound
bytes need not decode as complete keys or values; expose them as validated execution artifacts.
All intervals remain within the fixed physical identity. For stats, the enclosing identity
includes the partition and statistic, not just `KeyKind`
([stats/codec.rs:78](../crates/partition-store/src/stats/codec.rs#L78)).

## String-prefix strategy

Only case-sensitive string matching is supported. A literal prefix followed by `%`, with SQL
escaping interpreted by the query adapter, can supply a string-prefix constraint. More complex
patterns may supply a necessary prefix restriction, but their remaining conditions stay residual.
The SQL comparison semantics must agree with the field codec's ordering.

### Initial implementation: lower bound plus enclosing upper bound

For prefix `s` under an exact preceding prefix `P`, use:

```text
[P · E(s), successor(P))
```

The string-prefix field is terminal for bound construction. Do not append constraints from later
tags to either endpoint. This is a range over variable-length field values, not an equality prefix
under one complete string value.

The physical upper bound is still set: it is the end of the preceding fixed prefix. At the first
filterable field, use the full physical identity prefix (at least `KeyKind`, plus any fixed
partition/statistic/index discriminators).

Within one fixed `P`, the raw-key evaluator distinguishes:

1. Current string is below `s`: skip or seek to the lower bound.
2. Current string starts with `s`: continue evaluating later fields.
3. Current string is greater than `s` and does not start with it: this prefix interval is exhausted.

Exhaustion is scoped to `P`. For example, with `scope IN ('tenant-a', 'tenant-b')` and
`service_name LIKE 'alp%'`, exhausting matching services under `tenant-a` must still allow
visiting `tenant-b`. Stop a separate interval iterator, or advance to the next applicable parent
prefix when using a broader iterator. An unconstrained earlier field likewise requires discovering
subsequent parent groups rather than ending the whole scan.

For `LIKE '%'`, the string prefix is empty: cover the applicable non-NULL string domain under the
parent prefix. A rejected suffix constraint does not exhaust the string-prefix interval.

The comparison must be codec-aware. A complete mem-comparable encoding of `"alp"` is not a byte
prefix of the encoding of `"alpha"`: the codec groups bytes and records a terminal marker
([mem_comparable_string.rs:172](../util/string/src/mem_comparable_string.rs#L172)).

### Future refinement: tighter physical upper bound

Keep the string-prefix constraint independent of the chosen physical upper bound. A future
codec-backed compiler may tighten the interval, for example from the enclosing-prefix bound to
the encoding of `"alq"` for `"alp%"`. This must require no change to the logical predicate or
matching semantics. Retaining the evaluator's prefix check is safe with either plan.

Do not require an upper endpoint to be a valid string. Incrementing the input UTF-8 bytes may
produce invalid UTF-8 even when it gives a valid bytewise boundary. Conversely, incrementing the
completed encoding of `"alp"` bounds the exact encoded value, not every string starting with it.
Any future tightening belongs in the codec and should produce opaque bounds without weakening
string validity guarantees. The current byte-group writer is a possible internal building block
([mem_comparable_string.rs:679](../util/string/src/mem_comparable_string.rs#L679)); the existing
fixed-length increment helper is not itself a complete string-prefix-bound API
([lib.rs:85](../crates/partition-store/src/lib.rs#L85)).

## Raw-key evaluation and navigation

The proposed evaluator has four conceptual outcomes; these are not finalized Rust API names:

| Outcome | Contract |
| --- | --- |
| Candidate | Key satisfies the compiled key constraints; hand it to the table |
| Skip | Current key cannot match; advance normally |
| Seek | No possible match lies between the current key and the validated forward target |
| Done | No possible match remains in the current execution scope |

Skipping rejects one key. Seeking rejects an entire interval and therefore needs a stronger proof.
When a constrained suffix is exhausted, advance the appropriate enclosing prefix rather than
ending the whole scan. An observed complete parent value can support local navigation even if
that field was not an equality in the original predicate.

Seek targets must advance strictly, remain inside the active iterator's physical bounds, and
preserve index/partition ownership. If the next selected interval is outside those bounds, open
the next interval rather than seeking outside the iterator's contract. Preserve cancellation,
backpressure, and storage-error propagation in the adapter. Malformed keys must not be silently
classified as ordinary predicate mismatches.

Keep prefix-extractor mode consistent with every region visited by an iterator. Reuse the
centralized range-mode decision rather than assuming all generated seeks share the fixed RocksDB
prefix ([scan.rs:58](../crates/partition-store/src/scan.rs#L58)).

## Execution strategies and secondary-index integration

The access planner may choose multi-get for a finite set of complete physical keys, separate scans
for a finite set of leading prefixes, or a broader interval with key filtering and seeks. An exact
secondary value is not necessarily a complete key: a non-unique index can have many primary-locator
suffixes under it.

Budget combined expansion across fields, not just the length of each `IN` list. Exceeding the budget
must broaden access without dropping combinations or discarding the exact predicate. Deduplicate
point selections and normalize overlapping intervals so an entry is not emitted twice. Strategy
thresholds and batching can evolve independently from the predicate representation.

For a non-covering secondary index, candidate entries feed a bounded primary-lookup pipeline.
The table must validate ownership, primary existence, and any required index/primary consistency
before emitting rows. Index iteration and primary lookup need a consistent local read view.
Advertised result ordering must match the actual access path, and limits apply after the remaining
row predicate, not to unvalidated candidate counts.

Stats also illustrate why key acceptance is only candidate selection: one stored key expands into
multiple bucket rows, whose stage, status, and count come from the value
([service_stats/row.rs:21](../crates/storage-query-datafusion/src/stats/service_stats/row.rs#L21)).
Filters on cross-partition aggregate results must remain at their appropriate SQL level.

## Observability and verification

Expose the selected access strategy, which fields contributed bounds, the tag where construction
stopped, and whether a string-prefix upper bound is enclosing or tight. Worker-side trace output
should make physical bounds and identity inspectable; distinguish emitted rows from keys visited,
rejected keys, and seeks. Plan-time `EXPLAIN` information must not imply that worker-specific
encoded bounds have already been instantiated. The current scan display only reports scanner,
partitions, projection, predicate, and limit
([table_providers.rs:454](../crates/storage-query-datafusion/src/table_providers.rs#L454)).

Use a small number of high-signal tests:

- Compare optimized execution with a broad reference scan plus the exact predicate, including
  finite-set holes, intersections, NULL, and inclusive/exclusive range endpoints.
- Observe visited keys before residual filtering, so forcing full scans fails optimization tests.
- Cover string prefixes at empty, short, 8-byte group-boundary, embedded-NUL, and Unicode inputs;
  include exact matches, extensions, and the first nonmatching group.
- Exercise parent-prefix transitions, especially multiple selected parents and unconstrained
  earlier fields; prove seeks do not omit or duplicate candidates.
- Check agreement across point reads, prefix expansion, and enclosing-range fallback.
- Verify static constraint binding survives local/remote predicate transport and live updates.

## Proposed implementation sequence

1. **Define the shared contract.** Agree field traversal, constraint ownership, nullable values,
   codec-order comparisons, and the distinction between immutable predicate and iterator cursor.
   Generate the schema adapter from `define_aggregated_stat` using existing field codecs.
2. **Compile constraints and evaluate raw keys.** Implement finite sets and ordered intervals,
   legal initial bounds, and candidate/skip evaluation. Keep SQL normalization in the query crate
   and encode literals once during binding.
3. **Add case-sensitive string prefixes.** Implement the conservative lower/enclosing-upper plan,
   terminal-tag rule, and scoped exhaustion. Allow a later tighter upper bound without changing
   the constraint representation.
4. **Adopt the mechanism for stats.** Replace the handwritten per-key scan construction, keep
   remaining row predicates, and add production-path tests and targeted access-plan diagnostics.
5. **Add seek-assisted execution.** Expose the existing RocksDB seek capability through a
   correctness-preserving partition-store adapter. Start with finite-set gaps and exhausted
   prefixes; retain ordinary advancement as a valid fallback.
6. **Integrate a concrete secondary-index consumer.** Add its schema adapter, ownership/read-view
   requirements, and table-specific materialization. Introduce multi-get or prefix-expansion
   choices where complete-key information and measurements justify them.

Steps 1-4 form the first vertical slice. The contract must permit later execution strategies,
but a general cost model, exhaustive SQL-expression support, and a migration of every existing
table-key builder are not prerequisites.
