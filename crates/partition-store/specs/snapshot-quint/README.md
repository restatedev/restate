# SnapshotStore model (Quint) — snapshot leases and SST pruning

A small Quint model of Restate's snapshot path for **one partition**: the metadata-backed
snapshot lease, `latest.json` publication with retention, incremental SST dedup/sharing, eviction
cleanup and the orphan sweep. It was written to check the *interleavings* that the Rust unit tests
cannot easily hit: lease lapse, clock skew, stalls, commit-but-error and late deletes.

Ground truth: `crates/partition-store/src/snapshots/{leases.rs,repository.rs,snapshot_task.rs}`
(read-only; every action in `snapshot_store.qnt` cites the `path:line` it models —
`L:` = leases.rs, `P:` = repository.rs, `T:` = snapshot_task.rs).

## What it covers

* `Provider.acquire` / takeover only at true expiry by the acquirer's clock (`L:439`), the lease
  value as a versioned cell (`holder, lease_id, expires_at, version`).
* `is_valid()` = `now + LEASE_SAFETY_MARGIN < expires_at` on the guard's cached expiry (`L:398`).
* Renewal (`L:265-315`) including "the stored lease id must be ours".
* `fenced`: (a) local `is_valid`, (b) `confirm_lease` re-read of the stored lease id (`L:270-299`,
  `L:470-505`), (c) a second local check plus the operation deadline `expires_at - MARGIN`
  (`L:288-296`, `L:634-641`). Shared-SST deletes start only through it and land within `LAT`
  ticks; own-prefix objects of an unpublished snapshot are deleted unfenced (`P:727-742`).
* Upload with fingerprint dedup (`P:795-880`): an existing `ssts/<fp>.sst` is reused **without**
  refreshing its mtime, which is what makes the sweep/delete races possible.
* `build_latest_v2` (`P:1085-1160`): retention cut at `num_retained`, eviction list, "superseded"
  for equal/lower LSN.
* `resolve_failed_pointer_update` (`P:992-1052`) after a failed conditional put: re-read lists us →
  published and files kept (cleanup as on success), known not to list us → discard, re-read fails →
  keep the files.
* Eviction cleanup with a referenced-SST set, skip-if-referenced, metadata deleted last
  (`P:1490-1520`), orphan sweep with `MIN_AGE` (`P:1932-2010`).

## What it abstracts away

Bytes/sizes/multipart/RocksDB export/the log; per-node clocks are one global `now` plus a fixed
`skew[n]`; lease CAS versions are incremented but never fail; no node generations, adopt branch or
same-node wait; renewal cadence and retry policies; metadata store read errors (except the lease
re-read failing, which is a modelled outcome); one key per delete step instead of batches of
`DELETE_BATCH_SIZE`; only incremental snapshots (`ssts/<fp>.sst` + `metadata.json`); the
`file://` `PutMode::Overwrite` fallback (`P:1200-1215`) is not modelled, so a pointer regression is
not expressible.

## Invariants

* **I1** every snapshot in `latest.json.retained` has its `metadata.json` and all its SSTs present.
* **I2** `retained` is strictly descending in LSN and the pointer LSN never decreases.
* **I3** no shared-SST (`ssts/`) delete *lands* while the store's lease belongs to another lease id.
* **I4** a snapshot that was published and is still retained is never discarded.
* **I5** deleting an evicted snapshot's metadata implies its unreferenced files are already gone.

`checkpoint(cond)` is a test-only stuttering step used by the directed runs.

## Configurations (`cfg_*.qnt`)

`DURATION=10`, `MARGIN=3`, `MIN_AGE=6`, `F=4`, 2 nodes, node 1 skewed by `MAX_SKEW`.

| cfg | MODE | MAX_SKEW | stall | LAT | r | notes |
|---|---|---|---|---|---|---|
| C1 `cfg_perfect` | PERFECT | 0 | bounded 1 | 1 | 2 | mutex baseline |
| C2 `cfg_perfect_commit_err` | PERFECT | 0 | bounded 1 | 1 | 2 | commit-but-error |
| C3 `cfg_lapsing_bskew0` | LAPSING | 0 | bounded 1 | 1 | 2 | safe case |
| C4 `cfg_lapsing_bskew3` | LAPSING | 3 = MARGIN | bounded 1 | 1 | 2 | skew assumption broken |
| C5 `cfg_lapsing_unb_skew0` | LAPSING | 0 | unbounded | 1 | 2 | safe case |
| C6 `cfg_lapsing_unb_skew3` | LAPSING | 3 | unbounded | 1 | 2 | |
| C7 `cfg_lapsing_r1` | LAPSING | 0 | unbounded | 1 | 1 | Crash enabled |
| C8 `cfg_lapsing_lat4` | LAPSING | 0 | unbounded | **4 ≥ MARGIN** | 2 | deadline assumption broken |


## What the model does not cover

- The publish-time lease check is taken atomically with the conditional write of `latest.json`. In the code they are two steps (`put_snapshot_inner`); the gap is the known lapsed-lease publish race, which the conditional write makes harmless unless clock skew reaches the safety margin. Configurations with `MAX_SKEW > MARGIN` still reach a publish under another node's lease.
- Random simulation only (`quint run`), not exhaustive checking: Apalache was not available. The directed `quint test` runs pin the interleavings that matter.
- Bytes, sizes, multipart uploads, the RocksDB export and the log are abstracted away; see RESULTS.md for every spec-vs-code discrepancy.

## How to run

Quint 0.33.1 (`quint verify` is *not* available here: no JVM/Apalache — everything is
simulation). From this directory:

```bash
Q=quint-tool/node_modules/.bin/quint
$Q typecheck snapshot_store.qnt                      # and each cfg_*.qnt
$Q run cfg_lapsing_bskew0.qnt --init=M::init --step=M::step \
   --invariant=M::I3 --max-steps=60 --max-samples=100000 --seed=11 --verbosity=1
$Q run cfg_perfect.qnt --init=M::init --step=M::step \
   --witnesses=M::W_publishes M::W_evictions M::W_sweeps --max-steps=200 \
   --max-samples=300 --seed=11 --verbosity=1
$Q test cfg_lapsing_r1.qnt                           # directed interleavings
./run.sh                                             # everything, one PASS/FAIL/VIOLATION per line
```

`--invariants=M::I1 M::I2 ...` checks several at once; `--invariant=M::I1` is one. A run stops at
the first violation, so the per-invariant flags are what the table below uses
(100k samples for I1/I3, 20k for I2/I4/I5, seed 11).

## Current results (after `fenced`)

| cfg | I1 | I2 | I3 | I4 | I5 |
|---|---|---|---|---|---|
| C1 | PASS | PASS | PASS | PASS | PASS |
| C2 | PASS | PASS | PASS | PASS | PASS |
| C3 | PASS | PASS | PASS | PASS | PASS |
| C4 | **VIOL** (directed) | PASS | **VIOL** (directed) | PASS | PASS |
| C5 | PASS | PASS | PASS | PASS | PASS |
| C6 | PASS | PASS | PASS | PASS | PASS |
| C7 | PASS | PASS | PASS | PASS | PASS |
| C8 | **VIOL** (directed) | n/c | **VIOL** (random) | n/c | n/c |

* I1/I3: 100k samples, seed 11, `--max-steps=60`; I2/I4/I5: 20k. `(directed)` = only the
  `quint test` interleaving reaches it, the random search did not within the budget.
* The interesting reading: with `LAT < MARGIN` and `skew <= MARGIN` the fenced deletes are safe
  (C3, C5, C6, C7 and the two baselines). Breaking either assumption brings the violations back:
  `skew = MARGIN` (C4) leaves a one-tick window at `exp - MARGIN`, and `LAT >= MARGIN` (C8) lets a
  delete outlive the operation deadline.
* `n/c` = not checked for C8 (only I1/I3 were asked for); the C2/C1 rows' I2/I4/I5 are from before
  the fencing change, which does not touch those paths.

See `RESULTS.md` for the exact numbers, counterexamples, the history before fencing and the
spec/code discrepancies.
