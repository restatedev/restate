# restate-partition-store

## What the partition store is

Restate splits its work into partitions. The **partition store** is one
partition's local state: a RocksDB column family holding everything the
partition processor applies from the log, such as invocation status, journals,
user state, promises, locks, timers, and the virtual queues with their entries.
It also holds the partition's own bookkeeping, like which storage features and
migrations have been applied. Every key starts with a key kind, and keys of
partition-keyed tables continue with the partition key, so, within each key
kind, one partition's data is one contiguous range of keys.

This crate also owns the store's **derived data**: secondary indexes and
statistics computed from the stored tables, and the machinery that builds them
while the partition keeps running. Read the rest before adding an index, a
statistic, a data migration or a backfill here or in the worker.

## The concepts, in the order you need them

**Source tables.** Derived data is computed from a *source table*: one of the
store's tables whose rows are read by the mechanisms below. Today the only
source is the virtual queues' `entry_status` table: the walk records name it,
and the definitions apply to its rows. Supporting other source tables is the
design direction; this document uses `entry_status` as its running example.
Its rows describe each queued unit of work (an *entry*, for example an
invocation): its stage (inbox, running, suspended, finished, …), its status, its
timing statistics, and more. Where this document says "entry", read "a row of
the source table"; where it names a stage or a service, it is speaking about
that example.

**Index definition.** A piece of code with a stable, versioned name, such as
`"entry_by_stage_service/1"`, that says which entries belong to the index and
what to store for each of them. There are two kinds:
- a **secondary index** stores one key per member row, ordered by chosen
  fields, so that queries can scan, for example, "entries of service X in stage
  Y, newest first" without reading every row;
- an **aggregate index**, also called a statistic, stores counters, such as the
  number of entries per service and stage. Counters are updated by sending
  RocksDB a signed delta, which RocksDB adds to the stored value later; this
  avoids reading the counter on every write.

**Index.** One concrete build of a definition inside one partition store. The
store gives it an **index ID**, a number that prefixes all of its keys, so a
build's keys are one key range. Rebuilding a definition creates a new index with
a new ID; IDs are never reused.

**Keeping an index up to date.** Two mechanisms do this.
- **Writes.** Whenever the processor changes a row of the source table, it also
  updates every **maintained** index over that table, the ones being built or
  Ready: remove what the row's old state
  contributed, add what the new state contributes. If the index doesn't contain
  the old state yet, because the index is newer than the row, the write only
  adds. We call that *adopting* the row.
- **A backfill walk.** A new index doesn't yet contain the rows that existed
  before it. A **walk** visits those rows in key order, adds each one and records
  the index's token on it (below). It runs in small **slices**, each inside one
  of the processor's own transactions, between the processor's regular work, and
  remembers how far it got in a **cursor** so it can resume after a restart. The
  cursor is only a bookmark: no write consults it.

**Status.** An index is `Backfilling` while its walk hasn't finished: writes keep
it up to date, but it is incomplete, so queries must not read it. It becomes
`Ready` when the walk has added every row; queries may read it. It is
`Dropped` when its keys have been deleted; its ID stays reserved forever.

**Token.** For a write to know whether an index already contains a row's old
state, the source row keeps a small list, `touched_by`: the IDs of the indexes
still being built that already contain this row. (The list names the index's
*build job*, which for now has the same ID as the index.) Writes and walks are
the only ways a row gets a token, and the token is the only proof that an index
being built contains the row. A row carries a token until that index is Ready or
dropped; the next write then drops it. Each source table stores its tokens in
its own rows; for the example, that is the `entry_status` header.

**Index registry.** The per-store record of which indexes exist, with their
definition name, ID and status (`src/registry.rs`). An index being built points
to its **walk**, recorded next to the registry (`src/walk.rs`): the walk has its
own ID and holds the traversal's progress, and several indexes built together
share one walk. Both records are local to the store, not replicated: two
replicas of one partition can be at different points, which is normal.

**Definition families.** Definitions released together are grouped in a
*family*, switched on by the same configuration options.

**Migration job.** The general unit of maintenance work, today building an
index. Each index has a build job with the same ID, recorded next to the
registry (`src/job.rs`): its status, when it started and finished, and what
its slices did. The records are for operators only: the registry decides
whether an index is Ready, and the walk where its work resumes. Finished jobs
are kept up to `worker.storage.migration-job-history-limit`.

## The rules

Breaking one corrupts data silently; the symptom appears much later as a wrong
count or a stale key. The rules state the contract that index maintenance on
writes, background slices and query admission follow; the records in this crate
are their foundation.

1. **A definition's meaning never changes under its name.** Different membership,
   key fields, value encoding or key derivation means a new versioned name. At
   open, a store retires indexes whose definition the binary doesn't know.
2. **Index IDs are never reused**, so a stale token or a not-yet-deleted key can
   never be mistaken for a new build.
3. **One decision per index per row change.** The index contains the row's old
   state if the index is Ready or the row holds the index's token. Then remove
   old and add new; otherwise only add. Never apply a change twice; never skip an
   index that is being maintained.
4. **Read the stored row in the same transaction before changing it.** A row
   remembered from an earlier transaction can be stale: a later write may have
   changed its state and tokens, or a later slice may have filled it and added a
   token, so rule 3 would decide wrongly and add the row twice or skip it. For
   the `entry_status` example, `StoredEntryRef` carries the stored tokens
   together with the row's old state for this reason; it does not prove the row
   is current.
5. **A slice commits its index writes, the tokens it adds to the rows it
   filled, its cursor and, at the end, `Ready` together.** A crash loses them
   together, which is why the store needs no write-ahead log for this. The same
   holds for a write: the row change, its index effects and its token change are
   one commit. A fill keeps the tokens the row already holds.
6. **Only a full pass with no gap makes an index Ready.** A walk's indexes join
   it when it starts, at the beginning of its source, and every definition today
   can fill a row, so each row the walk passes holds the token or gets it before
   the cursor moves on. An index that joins a walk late, or a definition that
   can't fill, needs its own full pass before it is Ready.
7. **Queries read only Ready indexes, resolved in their own snapshot.** A read
   finds the Ready index of a definition in the registry as it was when the read
   was admitted, or fails with `IndexNotReady`.
8. **Persisted formats are append-only.** Bilrost tags, enum numbers and the keys
   under which the registry and walks are stored never change meaning. Rename
   in code freely; never renumber.
9. **The `index-registry` storage feature fences older binaries.** It is set with
   the first registration and records a minimum Restate version, so an older
   binary, which doesn't know tokens and index keys, refuses to open the store
   because of that version floor rather than ignoring them.

## Don'ts

Don't read a Backfilling index, or a newer registry than your snapshot. Don't
reuse a row read in an earlier transaction as an old state. Don't renumber tags,
enums or storage keys. Don't reuse an ID, or change a definition without
renaming it. Don't add an option, feature or definition without a
`Since vX.Y.Z` note.
