# Ordered-key filtering: types and macros

This is a guide to the current implementation, using `ServiceLoad` as the example.

## One table marker, two schemas

`ServiceLoad` is an empty marker declared by `define_table!` in storage-api.
`define_filter!` implements its logical filter schema. Partition-store implements
the storage traits on that same marker; it does not declare another table type.
See [the logical declaration](../../crates/storage-api/src/stats/service_load.rs#L14)
and [the storage adapter](../../crates/partition-store/src/stats/macros.rs#L30).

```text
ServiceLoad                          logical table identity (storage-api)
  |
  +-- FilterTarget                   logical fields and typed clauses
  |     +-- ServiceLoadField
  |     +-- ServiceLoadClause
  |     +-- Filter<ServiceLoad>       conditions on logical records
  |
  +-- Stat                           persisted identity, key, and value types
  |     +-- ServiceLoadKey            physical field order and codecs
  |     +-- StageStatusCounts         stored value
  |
  +-- AggregatedStat
        +-- StageStatusGauge         aggregation implementation
```

`FilterTarget` belongs to storage-api and contains no RocksDB encoding or access
strategy. Its field enum identifies logical fields; enum order is not physical
key order. `Filter<T>` ANDs clauses, including repeated constraints on one field.
See [FilterTarget and Filter](../../crates/storage-api/src/filter.rs#L28).

The current storage declaration is:

```rust
define_aggregated_stat!(
    table: ServiceLoad,
    value: StageStatusGauge,
    key: ServiceLoadKey(
        service_name: ServiceName => str,
        handler: Option<ReString> => str,
        kind: EntryKind,
    ),
);
```

Here `value` selects the aggregation algorithm, whose `Output` is the stored value
type. `StageStatusGauge` and `StageStatusCounts` are the aggregation and value aliases
for stage/status buckets. See [the declaration](../../crates/partition-store/src/stats/aggregated/service_load.rs#L24),
[the adapter](../../crates/partition-store/src/stats/macros.rs#L30), and
[the aliases](../../crates/partition-store/src/stats/aggregated/bucketed_gauge.rs#L303).

## What each macro supplies

| Macro | Responsibility |
| --- | --- |
| `define_table!` | Declare the logical marker. |
| `define_filter!` | Generate field/clause enums and implement `FilterTarget` for the marker. |
| `define_aggregated_stat!` | Implement `Stat` and `AggregatedStat`, supply the stat header writer, and select the table marker as the filter target. |
| `define_index_key!` | Generate the owned/borrowed key payloads and physical schema; invoke the shared prefix, decoder, and filter generators. |
| `define_index_key_prefix!` | Generate field-order-checked writes to a caller-owned `BufMut`. |
| `define_index_key_decoder!` | Generate progressive owned decoding and borrowed encoded-field access. |
| `define_index_key_filter!` | Bind the logical clause variants to the declared physical codecs. |

Sources: [table macro](../../crates/storage-api/src/table.rs#L26),
[filter macro](../../crates/storage-api/src/filter/macros.rs),
[stat macro](../../crates/partition-store/src/stats/macros.rs#L11), and
[shared key macros](../../crates/partition-store/src/keys/macros.rs#L11).

## The generated key types

`ServiceLoadKey` owns the decoded fields. `ServiceLoadKeyRef<'a>` uses borrowed
representations where the declaration has `=> str`. `IndexFieldView` preserves
wrappers such as `Option`, and `IntoIndexFieldRef` converts the input to that view.
The borrowed-view annotation is independent of prefix matching capability.
See [key generation](../../crates/partition-store/src/keys/macros.rs#L30) and
[field views](../../crates/partition-store/src/keys/index.rs#L219).

`EncodeIndexKey` and `DecodeIndexKey` handle the payload after the fixed header.
The stat adapter supplies `EncodeStatKey<ServiceLoad>` and `DecodeStatKey<ServiceLoad>`
implementations that delegate to those payload codecs. The stat-specific traits
keep the payload associated with its statistic at stat API boundaries.
See [payload traits](../../crates/partition-store/src/keys/index.rs#L128) and
[stat adapters](../../crates/partition-store/src/stats/macros.rs#L53).

`StatKeyPrefix` is the persisted ten-byte header: key kind, partition padding,
partition ID, statistic ID, aggregation kind, and reserved byte. It identifies the
physical region in which a stat scan is allowed to run.
See [StatKeyPrefix](../../crates/partition-store/src/stats/codec.rs#L21).

`ServiceLoadKeyPrefix<B, FIELD>` is the generated field-sequence builder. Its
constructor writes the fixed header; each method appends one complete field.
It borrows the caller's buffer, has no `finish()`, and does not clear the buffer:

```rust
let mut scratch = Vec::new();
ServiceLoadKey::prefix(partition_id, &mut scratch)
    .service_name("Counter")
    .handler(Some("get"));
// scratch now contains the header and two encoded fields.
```

The const `FIELD` tracks which method is available next. `KeyDecoder<'a, K, FIELD>`
tracks the corresponding decoding position. A `take_*` method returns a
`FieldDecoder` borrowing encoded bytes; a `decode_*` method materializes the value.
See [prefix and decoder generation](../../crates/partition-store/src/keys/macros.rs#L120).

## A query from SQL to RocksDB

Consider this predicate on `sys_service_stats`:

```sql
WHERE service_name = 'Counter'
  AND handler LIKE 'get%'
  AND kind = 'invocation'
  AND stage = 'running'
```

1. The generic `ScanLocalPartitionFilter` implementation translates supported static
   conjuncts into `Filter<ServiceLoad>`. Here it constructs `ServiceName(Equal(...))`,
   `HandlerStartsWith(...)`, and `Kind(Equal(...))` clauses. `stage` is not a field in
   this logical filter schema. SQL NULL semantics, operand reversal, and LIKE escaping
   are handled by the query adapter. See [translation](../../crates/storage-query-datafusion/src/filter/typed.rs#L34).
2. `ServiceLoadKey::prepare_filter` uses the generated binding and `IndexFilterCodec`
   to validate and encode literals. For example, a physical `ServiceName` uses
   `ReString` filter literals, and an optional string prefix includes a presence tag.
   See [binding generation](../../crates/partition-store/src/keys/macros.rs#L248) and
   [codec preparation](../../crates/partition-store/src/keys/filter/codec.rs#L22).
3. `PreparedKeyFilter<ServiceLoadKey>` arranges constraints in physical order using
   `IndexKeySchema::FIELDS`. Value literals share one owned byte buffer; prefix
   predicates retain their prepared prefix matcher. Missing key fields are errors,
    not silently discarded constraints. See [preparation](../../crates/partition-store/src/keys/filter.rs#L310).
4. The stat scan supplies the encoded `StatKeyPrefix`. `PreparedKeyFilter::into_cursor`
   binds the filter to a `PhysicalScan<Bytes>` and creates its navigation state.
   Included lower endpoints are complete values, so the lower bound combines the
   service, handler-prefix minimum, and kind. The upper envelope stops at the
   handler's exclusive prefix boundary: the service encoding, presence tag, and
   bytes `geu` for `get%`.
   See [bounds and cursor construction](../../crates/partition-store/src/keys/filter.rs#L365).
5. `KeyFilterCursor` evaluates encoded key fields and returns `Match`, `Seek`, or `Done`.
   At the first rejected field it advances within that field's domain, or carries
   into an earlier field when the domain is exhausted. A complete next value allows
   lower bounds for later fields to be appended; a boundary after a value does not.
   Seek targets are absolute owned bytes. The cursor and iterator adapter check
   forward progress and scan bounds; carry never changes the fixed physical identity.
   Values are deserialized only for matching keys, then the aggregation decides
   whether to return them. See [evaluation](../../crates/partition-store/src/keys/filter.rs#L480)
   and [execution](../../crates/partition-store/src/stats/aggregated/scan.rs#L95).
6. One stored `StageStatusCounts` value can become several SQL rows. `BatchSender`
   retains the complete live predicate, including the stage restriction, for row
   filtering. See [bucket expansion](../../crates/storage-query-datafusion/src/stats/service_stats/row.rs#L21)
   and [batch filtering](../../crates/storage-query-datafusion/src/partition_store_scanner.rs#L100).

The prefix builder is a way to construct a particular field-boundary prefix.
The prepared filter is the component that combines multiple predicates and chooses
scan bounds. They share codecs, but have different jobs.

### Advancing a compound predicate

For `service_name IN ('A', 'LargeState') AND key > '9999'` on virtual-object stats,
initial positioning combines service `A` with the exclusive key boundary. A jump
to the next selected service uses the same lower-suffix construction, so it starts
past that service's `9999` key group rather than at its first key. String comparison
is lexicographic, so `10000` is below `9999`.

When a parent has a continuous or unconstrained domain, the cursor cannot invent
its next stored value. It seeks past the current parent group, lets RocksDB discover
the next actual parent, then refines the suffix. Exhausting a suffix range or prefix
therefore carries left instead of prematurely ending the scan. Finite domains at
any depth use sorted candidates and retain every same-field condition.

Sources: [field advancement](../../crates/partition-store/src/keys/filter.rs#L224),
[cursor carry](../../crates/partition-store/src/keys/filter.rs#L509), and
[real iterator regression](../../crates/partition-store/src/stats/aggregated/virtual_object_load/filter_tests.rs#L33).

### String-prefix upper bounds

`MemCmpPrefix::encode_upper_bound` constructs an exclusive boundary from the encoded
prefix: retain earlier continuation markers, omit terminal padding and the final
marker, then increment the last payload byte. UTF-8 payload bytes cannot be `0xff`,
so the increment does not carry into an earlier group. The boundary need not be
valid UTF-8 or a complete encoded field, and later fields must not be appended.

For `service_name LIKE 'Large%' AND key > '9'`, the iterator upper bound is the
fixed statistic identity followed by bytes `Largf`. The lower bound includes the
complete encoding of `Large` and the exclusive boundary after key `9`. RocksDB
receives both bounds, independently of cursor filtering. Prefix upper bounds also
intersect with other constraints on the same field. An empty prefix is unbounded
for ordinary strings; nullable strings use `[1]`-tagged bounds and `[2]` as the end
of the entire non-NULL domain.

Sources: [codec boundary](../../util/string/src/mem_comparable_string.rs#L237),
[prepared prefix bounds](../../crates/partition-store/src/keys/filter.rs#L75), and
[raw iterator regression](../../crates/partition-store/src/stats/aggregated/virtual_object_load/filter_tests.rs#L202).

## Prefix capability ownership

There are two semantic decisions:

- `define_filter!` declares whether a logical field has a StartsWith clause.
- `IndexFilterCodec` determines whether the physical representation supports prefix
  matching and whether it needs a NULL tag.

Physical declarations contain only the field codecs and borrowed representations.
They do not repeat the logical prefix capability. `FilterTarget` exposes a borrowed
inspection method, generated by `define_filter!`:

```rust
fn starts_with_prefix(clause: &Self::Clause) -> Option<&str>;
```

It returns `Some(prefix)` for a StartsWith clause, including `Some("")` for
an empty prefix, and `None` for value clauses. `field(clause)` identifies the field.
The accessor borrows the clause's literal; it does not clone or re-encode it.
See [the contract](../../crates/storage-api/src/filter.rs#L64) and
[the logical generator](../../crates/storage-api/src/filter/macros.rs).

The physical binding:

1. Inspects whether the clause is a prefix predicate.
2. Dispatches on the generated logical field enum to the declared codec's `prepare_prefix`.
3. Otherwise uses the typed value-clause match and `prepare_value`.

For `ServiceLoad`, the generated prefix branch is equivalent to this, with
`FilterTarget` and `IndexFilterCodec` in scope:

```rust
if let Some(prefix) = ServiceLoad::starts_with_prefix(clause) {
    return match ServiceLoad::field(clause) {
        ServiceLoadField::ServiceName => ServiceName::prepare_prefix(prefix),
        ServiceLoadField::Handler => Option::<ReString>::prepare_prefix(prefix),
        ServiceLoadField::Kind => EntryKind::prepare_prefix(prefix),
    };
}
// The ordinary typed value-clause match follows.
```

The physical generator uses ordinary field repetition for both dispatches. It never
names a `*StartsWith` clause variant, so it also works with logical schemas that have
no prefix clauses. Prefix classification happens during preparation, not per row.
See [binding generation](../../crates/partition-store/src/keys/macros.rs#L248).

A logical prefix clause is supported whenever the selected key contains the field
and its codec supports prefixes. There is no separate per-key opt-out. For example,
`NameKey` contains only the name field but automatically supports its logical prefix
clause. `EntryKind` also uses static mem-comparable display names and supports
prefixes when the logical schema declares them. A logical prefix on a numeric
codec such as `u64` is rejected, as is a clause for a field absent from the selected
key. Those errors must not become silently dropped constraints.
See [binding tests](../../crates/partition-store/src/keys/macros/tests.rs)
and [missing-field validation](../../crates/partition-store/src/keys/filter.rs#L330).

`Stage`, `Status`, and `EntryKind` ordered fields encode their static display names,
so adding enum variants does not require assigning or renumbering lexical ranks.
This concerns ordered keys; stage/status counter buckets inside aggregate values
retain their compact numeric encoding.

Sources: [ordered enum codecs](../../crates/partition-store/src/keys/index_key_codec.rs#L84),
[static entry-kind names](../../crates/types/src/vqueues/entry_id.rs#L192), and
[aggregate bucket codecs](../../crates/partition-store/src/stats/aggregated/bucketed_gauge.rs#L53).
