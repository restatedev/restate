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
store's tables whose rows are read by the mechanisms below. Index backfills use
the virtual queues' `entry_status` table; orphan cleanup uses the journal
completion-ID index. This document uses `entry_status` as its running example.
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

**Status.** An index is `Backfilling` until a walk finds it complete: writes keep
it up to date, but it is incomplete, so queries must not read it. It becomes
`Ready` once a full pass of a walk finds every row covered; queries may read it.
A definition that can't fill rows from the row alone (a *passive* one) relies on
writes and deletions to cover them: between passes it stays `Backfilling`
without a walk, and its job waits for the next pass. It is `Dropped` when its
keys have been deleted; its ID stays reserved forever.

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

**Migration job.** The general unit of maintenance work: building an index or
cleaning up obsolete data. Each index has a build job with the same ID, recorded next to the
registry (`src/job.rs`): its status, when it started and finished, and what
its slices did, and when a waiting job may start its next pass. The registry
decides whether an index is Ready, and the walk holds where its work resumes and
the evidence for completing it; a job record can be rebuilt from the registry.
Finished jobs are kept up to `worker.storage.migration-job-history-limit`.

### Reference online migration: orphaned journal completion IDs

`src/walk/jc_orphan_cleanup.rs` is the worked example of a non-index job.
`jc_orphan_cleanup/1` removes completion-ID index (`jc`) rows left behind by an
old journal deletion bug. It does not create an index-registry entry.

**Authority and lifecycle.** The `jc-orphan-cleanup-v1` storage feature means the
cleanup has completed; a supported cleanup walk means a sweep is in
progress. Opening the store resumes that walk, reconstructing missing diagnostic
job records without inventing counters or timestamps. Otherwise, if the marker
is absent and the source is nonempty, open registers a walk with its captured
last key. Empty sources need no job and cost only a last-key seek on reopening.
Job ID 8 is reserved for this one-time migration; index jobs start at 64. Never
reuse this identity for another definition or use history as a completion marker.
The historical `JC_ORPHAN_CLEANUP_DONE` marker is also honored, but new jobs
record completion only in the storage-feature ledger. This feature is completion
evidence, not a foreground routing switch. Completion publishes the committed
feature bits to all store handles and the processor's status; diagnostic records
are reconciled against completion evidence even if an unfinished record survived.

**Deletion proof.** The job checks the invocation's `j2[0]` ownership sentinel
through the current processor transaction. It deletes a `jc` row only if that
sentinel is absent. No ownership decision is retained across slices. A slice
can end inside an invocation, so a large journal cannot defeat the entry budget.
Ownership lookups bypass the block cache and charge their key/value bytes to
the slice's read budget. Lookup sizes are known after reading: an oversized
candidate yields to the next slice, whose first row always makes progress.
Foreground journal creation establishes entry zero, writes journal entries and
their indexes transactionally, and journal deletion removes both. Consequently
foreground operations cannot introduce orphans behind the cursor or beyond the
captured endpoint. A full pass is sufficient; the cursor alone is not proof.

**Atomicity and recovery.** The shared walk engine owns traversal, read/write
budgets, savepoints, cursor advancement, accounting, commit and publication.
The definition owns decoding, the deletion predicate and the completion marker.
A failed staging attempt clears all its writes, including earlier deletions in
the slice. An uncertain commit stops the processor and requires reopening.
The final slice commits its completion marker, job history and walk removal
together. If adding completion metadata would exceed a data slice's write budget,
that slice commits its deletions and cursor first, and an empty slice finishes
the job. The ledger is read and encoded only when finishing, not for every slice.
Trimming history cannot restart the cleanup.

**Compatibility.** This walk uses `OnUnknown::Discard`: an older binary may
discard it, because orphan deletion leaves the live data format unchanged and a
later sweep can safely restart. The existing registry fence protects the walk
records from binaries predating that infrastructure. Do not copy this policy
for a layout conversion: choose `Refuse` and fence the required version before
changing how foreground operations interpret data.

**Operations.** The normal background-migration controls govern this job too.
It appears in `restate.cluster.migration_jobs` as kind `cleanup`, mode `active`;
`rows_examined` counts visited index keys and `rows_filled` counts deletions.
Progress and ETA are approximate partition-key-range estimates, not row counts.

When adding the next online migration:

1. Specify the authoritative records and reserve a collision-free job identity.
2. Write the safety argument for interleaved foreground writes, including keys
   behind the cursor and beyond the captured endpoint.
3. Define a versioned walk kind/source and validate their persisted positions.
4. Stage each mutation and its predicate in the processor's transaction.
5. Persist completion with the last slice, independently of job diagnostics.
6. Choose the unknown-kind policy deliberately; preserve existing wire tags.
7. Test restart, missing diagnostics, split slices, foreground mutations and
   failure rollback. The tests beside this cleanup demonstrate these cases.

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
10. **Every definition declares the Restate version that introduced it**
    (`introduced:` next to `definition:`). A node of an older version can't
    maintain or read the definition's indexes, so a query coordinator must
    choose an index only where every node serving its partitions runs at least
    that version. The V2 planner checks this, against the versions the worker
    nodes recorded, before it reads an index in place of its primary. The version is code,
    not stored data: it is never persisted.

## Don'ts

Don't read a Backfilling index, or a newer registry than your snapshot. Don't
reuse a row read in an earlier transaction as an old state. Don't renumber tags,
enums or storage keys. Don't reuse an ID, or change a definition without
renaming it. Don't add an option, feature or definition without a
`Since vX.Y.Z` note.
