# Model A — SnapshotStore: results

Quint 0.33.1 (`quint typecheck`, `quint run`, `quint test`; `quint verify` unavailable — no
JVM/Apalache), model in `snapshot_store.qnt`, one instance module per configuration
(`cfg_*.qnt`). All numbers below are **random simulation**, not exhaustive verification; sample
budgets are stated per check (100k for I1/I3, 20k for I2/I4/I5, seed 11, `--max-steps=60`).

**Update 2 (delete fencing).** The deletes the model flagged as unfenced are now fenced:
`SnapshotLeaseGuard::fenced` (`L:270-299`) does a local `is_valid` (a), a `confirm_lease` metadata
read that the stored lease id is still ours (b, `L:470-505`), then starts the operation only after
a *second* local check, bounded by the operation deadline `expires_at - LEASE_SAFETY_MARGIN`
(c, `L:288-296`, `L:634-641`). `delete_under_lease` (`P:748-790`) sends shared-SST deletes in
batches through it, and it is used by the discard of created shared SSTs (`P:740-741`, own-prefix
objects still unfenced at `P:734-738`), by eviction cleanup (`P:1506`, metadata too `P:1515`) and by
the orphan sweep (`P:2039`). The model now starts every shared-SST delete through
`fenceConfirm` + a locally re-checked `start*Delete` and lets it land within `LAT` ticks (a new
constant; `Tick` refuses to advance past `start+LAT`, so a started delete cannot be late). The
codes above refer to `crates/partition-store/src/snapshots/{leases,repository}.rs` at the stack tip they were written against
(the `lease-fix` tree from the previous update still has the same resolve path; line numbers below
are from `model-tree` where they moved).

**Update 1 (commit-but-error).** The code was re-read in the `lease-fix` worktree: the
commit-but-error path now re-reads the pointer (`resolve_failed_pointer_update`, P:992-1052) and
the orphan sweep is gated on incremental snapshots (P:577-580). `PublishCommitButError` in the
model now enters a `resolving` phase with three non-deterministic outcomes
(`resolveListed` / `resolveNotListed` / `resolveReadFails`). C2 was re-run (all of I1-I5) and C1
re-run for I1/I3 as a regression; the pre-fix C2 numbers are marked "before the fix".

Everything is grounded in the code, read-only, branch `incremental-snapshot-chaos`
(the current stack):
`P = crates/partition-store/src/snapshots/repository.rs`,
`L = crates/partition-store/src/snapshots/leases.rs`,
`T = crates/partition-store/src/snapshots/snapshot_task.rs`.

## Configurations

| id | module | MODE | MAX_SKEW | stall | NUM_RETAINED | commit-err | crash |
|----|--------|------|----------|-------|--------------|------------|-------|
| C1 | `cfg_perfect` | PERFECT | 0 | bounded (MAX_STALL=1) | 2 | no | no |
| C2 | `cfg_perfect_commit_err` | PERFECT | 0 | bounded | 2 | **yes** | no |
| C3 | `cfg_lapsing_bskew0` | LAPSING | 0 | bounded | 2 | no | no |
| C4 | `cfg_lapsing_bskew3` | LAPSING | **3 = MARGIN** | bounded | 2 | no | no |
| C5 | `cfg_lapsing_unb_skew0` | LAPSING | 0 | **unbounded** | 2 | no | no |
| C6 | `cfg_lapsing_unb_skew3` | LAPSING | 3 | unbounded | 2 | no | no |
| C7 | `cfg_lapsing_r1` | LAPSING | 0 | unbounded | **1** | no | **yes** |

Protocol constants (spec values / 30 s): `DURATION=10`, `MARGIN=3`, `MIN_AGE=6`, `F=4`
fingerprints, ≤4 snapshots, 2 nodes, node 1 skewed by `MAX_SKEW`.

## Configuration × invariant

`PASS` = no violation in the full sample budget; `VIOL` = violated.

| cfg | I1 | I2 | I3 | I4 | I5 |
|-----|----|----|----|----|----|
| C1 perfect | PASS 100k | PASS 20k | PASS 100k | PASS 20k | PASS 20k |
| C2 perfect+commit-err — **before the fix** | **VIOL** | PASS 20k | PASS 100k | **VIOL** | PASS 20k |
| C2 perfect+commit-err — **after the fix** (`lease-fix`) | PASS 100k | PASS 20k | PASS 100k | PASS 20k | PASS 20k |
| C3 lapsing, skew 0, bounded, LAT=1 | PASS 100k | PASS 20k | PASS 100k | PASS 20k | PASS 20k |
| C4 lapsing, skew 3 = MARGIN, LAT=1 | **VIOL** (directed) / PASS 100k random | PASS 20k | **VIOL** (directed) / PASS 100k random | PASS 20k | PASS 20k |
| C5 lapsing, unbounded, LAT=1 | PASS 100k | PASS 20k | PASS 100k | PASS 20k | PASS 20k |
| C6 lapsing, unbounded, skew 3, LAT=1 | PASS 100k | PASS 20k | PASS 100k | PASS 20k | PASS 20k |
| C7 lapsing, unbounded, r=1, crash, LAT=1 | PASS 100k | PASS 20k | PASS 100k | PASS 20k | PASS 20k |
| C8 = C5 with **LAT=4 >= MARGIN** | **VIOL** (directed) / PASS 100k random | — | **VIOL** (random, ≤100k) | — | — |

Rows C3–C8 are after fencing (fresh numbers, seed 11, 100k samples). C1/C2 were also re-run after
fencing for I1/I3 and stayed PASS; their I2/I4/I5 numbers are the pre-fencing ones (those invariants
do not involve deletes) and the C2 directed runs were re-run.

I2 is never violated anywhere (the conditional write protects the pointer — see D2 for the
`file://` caveat that the model does not express). I4/I5 are only violated by the
commit-but-error path.

## Calibration (spec's expected results)

| spec expectation | outcome |
|---|---|
| PERFECT, no crash, no commit-err → I1–I5 hold | **as expected**: C1 all PASS (2 seeds for I1/I3) |
| PERFECT + PublishCommitButError → I1 and I4 fail | **as expected before the fix**: C2 I1 & I4 VIOL, I2/I3/I5 PASS. **After the fix the expectation changes**: the re-read lists the snapshot, so nothing is discarded and all of I1-I5 hold (C2 re-run: all PASS; `resolveListed` also runs eviction cleanup, P:968-974) |
| LAPSING, bounded stall, pairwise skew < MARGIN → I1, I3 hold | **as expected**: C3 I1/I3 PASS (100k, 2 seeds) |
| LAPSING, unbounded stall **or** MAX_SKEW ≥ MARGIN → I3 and I1 fail | **as expected**: C4 (skew = MARGIN, 1-tick stall) I1+I3 VIOL; C5/C6 I3 VIOL and I1 VIOL (directed; see note) |
| Same assumption → I2 still holds | **as expected**: I2 PASS in C3–C7 |
| Lapsed-lease publish race reachable | **as expected**: `W_foreignPublish` witnessed in 17/300 traces (C6), i.e. `PublishOk` landed while the stored lease id was another node's |
| **After fencing**: LAPSING with LAT < MARGIN and skew <= MARGIN → I1/I3 hold | **as expected**: C3, C5, C6, C7 PASS I1 and I3 (100k) |
| **After fencing**: LAT >= MARGIN → violations still reachable | **as expected**: C8 (LAT=4) I3 VIOL by random search (≤100k samples) and I1+I3 by the directed run |
| **After fencing**: skew = MARGIN → violations still reachable (assumption broken) | **as expected**: C4 I1+I3 reachable only by the directed run (`LAT=1`, one-tick window at `exp - MARGIN`) |
| Random simulation rarely finds the stall trace unaided | **as expected**: the 1-tick and 4-tick stalls were only reached by the directed runs (or needed 100k samples) |

Honest note on C5/C6/C7 I1: at 100k samples the random walk finds the *delete under a foreign
lease* (I3) but not the full I1 (it needs the deleted SST to be re-referenced by a snapshot that
the other node published in between). The directed runs below reach exactly that state, so the
invariant is not vacuously satisfied — the model reaches it, the random search just needs the
specific stake-out. C7's I3 likewise was not found in 100k (seed 11) or 300k (seed 21); the two
directed runs in `cfg_lapsing_r1.qnt` reach it.

## Counterexamples

**C2 `cfg_perfect_commit_err` I1 + I4 — BEFORE THE FIX: commit-but-error discarded a published
snapshot** (43 steps, seed 11, from the `bridge-cse` branch; no longer reproducible):

> After the fix this trace stops being a violation: `put_snapshot_inner` re-reads the pointer
> (P:946-951, P:992-1052), sees its own snapshot id in the result, and keeps the files; see the
> new directed runs below.

**C2 (after the fix) — commit-but-error keeps the published snapshot** (directed, 14 and 10 steps):
1. n0 acquires, exports file {2} (lsn 1), uploads `ssts/2`, writes `metadata.json`, reads
   latest.json (etag 0), revalidates (P:933).
2. `publishCommitButError`: the conditional put is committed (retained = [s1]) but returns `Err`,
   so the node enters `resolving` with its upload progress intact (P:946-951).
3. `resolveListed`: the re-read pointer lists s1 and s1 is still the top-level id, so the snapshot
   counts as published, the files are kept, and eviction cleanup runs as on success
   (P:992-1038, P:952-958, P:968-974).
4. `checkpoint(I1 ∧ I4 ∧ I5)` holds; `cleanupRead` sees `refSet={2}` and the sweep lists nothing.
5. Second run: `resolveReadFails` (P:1006-1016) keeps the files and discards nothing →
   `checkpoint(I1 ∧ I4)`.

**C2 — pre-fix shape (kept for reference)**:
1. n1 acquires, exports one file (lsn 1), uploads `ssts/0` (`creates_shared`, P:826-838), writes
   `metadata.json` (P:1052-1065), reads latest.json (etag 0, branch `newer`), revalidates (P:932).
2. `publishCommitButError` (P:936-947): latest.json is committed (etag 1, retained = [s1]) but
   `put_opts` returns `Err`; `put` takes the error path (P:717-720).
3. n1 revalidates (P:734) and `discard_uploaded_files` deletes `metadata.json` **and** `ssts/0`
   of the snapshot it just published.
4. State: `retained=[s1]`, `metaObjs={}`, `sstPresent={}` → I1 violated; ghost
   `discardedRetained={s1}` → I4 violated.

**C4 `cfg_lapsing_bskew3` I3 + I1 — discard race with a stale local check** (31 steps, seed 11):
1. n0 acquires at now 0, exports two files, uploads both (`ssts/{0,1}`, mtime 0), revalidates at
   now 7 (still valid).
2. n1 (skew +3) takes the lease over at now 9 and publishes s2 (etag 1, retained [s2]) which
   deduplicated against `ssts/0` (head() hit, mtime not refreshed, P:795-825).
3. n0's own publish loses its CAS (`seenEtag 0 ≠ 1`, P:941-945), so it enters the discard path
   and deletes its created shared SSTs with the check taken at now 7 (P:731-740).
4. `retained=[s2]`, its metadata present but its files gone → I1 violated; the stored lease id is
   n1's → I3 violated (ghost `i3Deletes=1`, `discardSsts=2`).

**BEFORE FENCING — C5 `cfg_lapsing_unb_skew0` I3, delete under a foreign lease** (40 steps, seed 11):
1. n0 acquires (exp 10), exports and uploads two SSTs, revalidates at now 5, intends to publish.
2. n0's check at now 9 is invalid; it revalidates again at now 14 (after a renewal, exp 20 — still
   valid by its own clock: 14 + 0 + 3 < 20).
3. n0 stalls; n1 takes the lease over at now 20 (`lease=(h1,id2,exp30)`), i.e. 6 ticks after n0's
   last valid check.
4. n0 discards at now 24 with that stale check → deletes its created SSTs while the store's lease
   belongs to n1 → I3 violated (nothing is retained, so I1 is not violated in this trace).

**BEFORE FENCING — C6 `cfg_lapsing_unb_skew3` I3** (28 steps, seed 11): same shape, compressed — n0 revalidates at
now 14 (exp 21), n1 takes over at now 19 (`exp 32`), n0 deletes one created SST at now 19 →
`i3Deletes=1`.


**AFTER FENCING — C8 `cfg_lapsing_lat4` I3 — a late landing (random, ≤100k samples, seed 11)**:
1. n0 acquires at now 3 (exp 13), exports files {1,2}, uploads both (created), and later renews
   (exp 26); its publish fails, so it enters the discard path at now 15.
2. n0 revalidates at now 22 (`22 + 3 < 26` ✓), `fenceConfirm` confirms the stored lease id is
   still its own, and starts the fenced delete of one created SST — with `LAT = 4` that request may
   stay in flight until now 26.
3. n1 acquires at now 26 (`26 >= 26`, `L:439`) while the delete is still in flight.
4. The delete lands at now 26, i.e. 4 ticks after it started and while the store's lease belongs to
   lease id 2 → `i3Deletes = 1` (I3). With `LAT = 1` this cannot happen: the landing would be at
   `start + 1 <= exp - MARGIN < exp`.

**AFTER FENCING — C4 `cfg_lapsing_bskew3` I1 + I3 (directed, 47 steps)** — `skew = MARGIN` with the
stall bound honoured: n0 sweeps, revalidates at now 13 (a), `fenceConfirm` (b), starts the delete at
13 (c), stalls exactly one tick (MAX_STALL = 1), n1's clock (`+3`) reaches the expiry at now 14 —
the very tick the delete lands — publishes a snapshot that reused `ssts/2`, and the delete lands at
14 → I1 + I3. This is the case the `LAT < MARGIN` bound does *not* cover, because the takeover
clock differs by `MARGIN`.

**AFTER FENCING — C7 / C5 / C3: the old counterexamples are now refused.** The directed runs
`sweepRaceIsRefusedByFencingTest`, `discardRaceIsRefusedByFencingTest`
(`cfg_lapsing_r1`) and `unboundedStallIsRefusedByFencingTest` (`cfg_lapsing_unb_skew0`) drive the
exact interleavings that used to delete a referenced SST; now `fenceAbort` fires (the local check
lapsed and `confirm_lease` sees another node's lease id, `L:274`/`L:476`) and the invariant holds at
the checkpoint: `I1 ∧ I3`.

## Directed runs (`quint test`, exact interleavings) — all PASS

Each run asserts that the violation is reached, so a PASS means the bad state was produced.

* `cfg_perfect_commit_err::commitButErrorKeepsPublishedSnapshotTest` — C2 after the fix: the
  re-read lists the snapshot, cleanup runs, ends with `checkpoint(I1 ∧ I4 ∧ I5)`.
* `cfg_perfect_commit_err::commitButErrorUnreadablePointerKeepsFilesTest` — re-read fails, files
  kept, ends with `checkpoint(I1 ∧ I4)`.
  (Before the fix this file held `commitButErrorDiscardsPublishedSnapshotTest`, asserting
  `¬I1 ∧ ¬I4`; that run no longer represents the code.)
* `cfg_lapsing_bskew3::skewEqualsMarginBoundedStallTest` — asserts `¬I1 ∧ ¬I3`;
  **skew = MARGIN with the stall bound honoured**: n0 (skew 0) acquires at 0, abandons a first attempt (`loseLease` → discard with an
  invalid check keeps `ssts/2`, P:734), re-acquires at 7 (exp 17), publishes s2 (retained [s2]),
  lists `ssts/2` as an orphan (age 7 ≥ MIN_AGE, unreferenced, P:1868-1912), revalidates at
  now 13, stalls **exactly one tick** (MAX_STALL = 1), n1 (skew +3) takes over at now 14
  (14 + 3 ≥ 17), publishes s3 which reuses `ssts/2` (mtime stays 0), n0 deletes `ssts/2` with
  the check from now 13 → I1 + I3.
* `cfg_lapsing_lat4::lateLandingDeletesRetainedSstTest` — asserts `¬I1 ∧ ¬I3`: `LAT=4`, delete
  started at now 13 while valid, lands at now 17 exactly when node 1 takes over and after it
  published a snapshot reusing that SST.
* `cfg_lapsing_unb_skew0::unboundedStallIsRefusedByFencingTest` — asserts `I1 ∧ I3` after the
  old unbounded-stall interleaving (revalidate at 13, stall 4 ticks, takeover at 17, delete
  attempted): fencing refuses it.
* `cfg_lapsing_r1::sweepRaceIsRefusedByFencingTest` / `::discardRaceIsRefusedByFencingTest` —
  `NUM_RETAINED=1` with `Crash`, assert `I1 ∧ I3`; `fenceAbort` is the modelled refusal
  (`P:770-775` leaves the rest to the sweep).

## Reachability witnesses (`quint run --witnesses`, seed 11, 300 traces × 200 steps)

A witness is counted when it is true in some state of the trace, so a non-zero count proves the
state is reachable. Trace length is 200 here (the 60-step budget of the invariant runs is too
short for three publishes); stated as a deliberate choice.

| witness | C1 perfect | C6 unb_skew3 | C7 r1 | C2 commit-err |
|---|---|---|---|---|
| `W_publishes` (≥3 publishes) | 293 | 23 | 0 | 293 |
| `W_evictions` (≥1 eviction delete) | 68 | 4 | 0 | 65 |
| `W_discardSsts` (≥1 created-SST discard) | 0 | 26 | 1 | 0 (141 before the fix) |
| `W_foreignPublish` (publish under a foreign lease) | 0 | 17 | 0 | 0 |
| `W_sweeps` (≥1 sweep delete) | 10 | 8 | 0 | 23 |
| `W_discardRetained` (discard of a retained id) | 0 | 0 | 0 | 0 (267 before the fix) |

After fencing, `W_foreignPublish` is 0 in every configuration with `skew <= MARGIN`: the publish
check at `P:933` is modelled as a fresh local read taken atomically with the CAS, so a lapsed writer
cannot land a publish before the takeover (see D12). The witness stays in the model for
`MAX_SKEW > MARGIN` configurations.
C7 (r1) needs longer traces/targeted seeds for ≥3 publishes: its unbounded stall plus `Crash`
produces runs that spend most of their steps waiting for a crashed holder's lease to expire; the
two directed runs are the evidence for C7 instead. C1 never discards a created shared SST
(no lapse, no CAS loss) — that witness belongs to the tearing configurations.

## Spec vs code discrepancies (modelled per the code)

0a. **Shared-SST deletes are fenced; the model bounds the in-flight time with `LAT`.** `fenced`
   (`L:270-299`) is the only way a shared-SST delete starts, and `delete_under_lease`
   (`P:748-790`) awaits the delete inside the fenced future, so the guard is held until it
   completes. The model expresses this as `fenceConfirm` + a re-checked `start*Delete` + a pending
   delete that lands within `LAT` ticks; the code has no explicit "LAT" — the real bound is the
   object-store request time, and the deadline that backs it is `expires_at - LEASE_SAFETY_MARGIN`
   (`L:634-641`). `LAT` is therefore a *modelling knob* for the assumption "a delete request
   completes within less than the safety margin", not a constant in the code. The model also
   forbids `release`/`fenceAbort`/`cleanupDone`/`discard` while that node's delete is in flight,
   because the code awaits it; `Crash` is allowed (the request can outlive the process).
0b. **Own-prefix objects are not fenced** (`P:734-738` deletes `own_objects` before any lease
   check), only the created shared SSTs are (`P:740-741`). Modelled as such: `discard` removes the
   snapshot's own `metadata.json` unfenced and abandons un-started shared SSTs to the sweep.
0c. **Batching.** `DELETE_BATCH_SIZE = 1000` (`P:126`): one `fenced` call per batch, so the real
   window is "up to 1000 keys per check". The model starts one key per fenced step (stricter).
0d. **Commit-but-error is now resolved by a re-read (fixed in `lease-fix`).** `put_snapshot_inner`
   no longer discards on a failed conditional put: it calls `resolve_failed_pointer_update`
   (P:946-951, P:992-1052), which re-reads latest.json. If the pointer lists this snapshot
   (top-level or in `retained_snapshots`, P:1017-1038) the snapshot is published, the files are
   kept and — if it is still the newest — cleanup runs exactly as on success (P:952-958 for the
   newer-snapshot case, P:968-974 for the cleanup spawn); if the pointer is known not to list it
   the files are discarded (P:1039-1051); if the re-read fails, the files are kept for the sweep
   (P:1006-1016). Modelled as `resolving` + three outcomes. The pre-fix code (P:717-720 of the
   `bridge-cse` branch) discarded unconditionally, which is why the old C2 row shows I1/I4.
   A related change: the orphan sweep now runs only for incremental snapshots
   (`orphan_cleanup: matches!(…, SnapshotType::Incremental)`, P:577-580), which is the only type
   this model represents — the "always sweep-eligible" over-approximation in item 4 now matches
   the code for incremental snapshots, and the `ORPHAN_SCAN_INTERVAL` filter (P:1186-1191) remains
   an over-approximation.
1. **Pointer CAS is not available for `file://` repositories.** `conditional_put_options` falls
   back to `PutMode::Overwrite` when the destination scheme is `file` (P:1105-1114, especially
   P:1108 and P:1112). I2 ("conditional write protects the pointer") is therefore only true for
   object stores that support `UpdateVersion`; the model always assumes the CAS, so it cannot
   exhibit a pointer regression. Not modelled.
2. **Discard re-reads the clock per object instead of using the last check.** The code deletes
   `own_objects` unconditionally and then `created_shared_ssts` under
   `take_while(|_| lease_guard.is_valid())` (P:731-740, P:734). SPEC.md says "delete created only
   if the last check said valid"; the model collapses the per-object reads into one decision taken
   from the last `CheckLease`, which is the weaker (more permissive) reading — a real discard is
   *less* likely to delete than the model's `Discard`.
3. **Deletion batching.** `ORPHAN_DELETE_BATCH_SIZE = 1000` (P:138) with one `is_valid()` per
   batch (P:1983-1990). The model requires a check per key, i.e. it is stricter than the code
   (the code's check→delete window spans up to 1000 keys, the spec's `BATCH=2` knob is not
   modelled).
4. **Sweep frequency.** `ORPHAN_SCAN_INTERVAL = 24h` and `claim_orphan_scan` (P:135, P:1122-1139,
   P:1235) allow the orphan sweep at most once per interval per partition; the model treats every
   post-publish cleanup as sweep-eligible (over-approximation, conservative for finding bugs).
5. **`ORPHAN_MIN_AGE = 24h`** (P:132) is scaled to 6 model ticks and evaluated against `now`
   instead of `WallClock::now_ms()` (P:1874).
6. **Lease adopt and same-node waiting are not modelled.** `try_acquire` adopts an existing value
   when the stored `lease_id` equals the caller's (retry after a committed-but-errored write,
   L:426-435), and `acquire_lease` polls for up to `SAME_NODE_LEASE_WAIT` when the lease is held by
   the same node id (P:606-638, P:125-126). Model A has no lease-write error injection and no node
   generations, so Acquire is: slot free / released, or expired by the caller's clock (L:439).
7. **Renewal cadence.** `LEASE_RENEWAL_INTERVAL = 2 min` (L:56) and the renewal retry policy are
   not modelled; `Renew` is a nondeterministic step. The modelled part is faithful: `renew()`
   checks the *stored* lease id (not its own validity) and computes the new expiry from its own
   clock (L:265-315).
8. **PERFECT mode is a spec abstraction, not a code path.** The code has one implementation with
   the safety margin (L:353-354). In `MODE=0` the model makes `is_valid` always true and allows a
   takeover only after a release marker (`exp = 0`, L:462) — an atomic mutex.
9a. **The publish path is not fenced, and its check→CAS window is not modelled.** `put_snapshot_inner`
   re-reads the clock right before the conditional put (`P:933`) and the code's own comment says the
   window to the CAS landing cannot be closed (`P:920-925`); the model takes that local check
   atomically with the CAS, so a *lapsed writer publishing* cannot be expressed. Because of that,
   `W_foreignPublish` is unreachable for `skew <= MARGIN`; with `MAX_SKEW > MARGIN` it is reachable
   (the local check passes while another node's takeover condition already holds).
9b. **`checkedAt` is cleared on release/crash.** SPEC.md's bounded-stall guard "no node has
   `now - checkedAt > MAX_STALL`" is applied to nodes that have an outstanding local check
   (`checkedAt ≥ 0`); otherwise no time could ever advance in bounded mode.
10. **Non-shared snapshot files are not modelled.** Only incremental snapshots are considered:
    `metadata.json` + `ssts/{fingerprint}.sst` keys (P:772-779). Bytes/sizes, multipart uploads,
    the RocksDB export and the log are out of scope as the spec allows.
11. **`everPublished` is recorded as a ghost but no invariant consumes it**: I4 uses the stronger
    "discarded while retained" set (`discardedRetained`), which is what the commit-but-error
    calibration needs (`retained=[s1]` at the moment of the discard).

## Files

* `snapshot_store.qnt` — the model (state, 23 actions, I1–I5, 6 witnesses, `checkpoint`).
* `cfg_*.qnt` — one instance module per configuration; `cfg_lapsing_r1.qnt`,
  `cfg_lapsing_bskew3.qnt` and `cfg_lapsing_unb_skew0.qnt` also hold the directed runs.
* `run.sh` — every configuration × invariant with fixed seeds, witnesses, directed runs; prints one
  `PASS`/`FAIL`/`VIOLATION` line per check (`S_BIG`, `S_SMALL`, `S_WIT`, `SEED`, `STEPS` are
  overridable; the table above is `./run.sh` with the defaults).
* `summarise.py` — helper that prints a compact per-state view of an ITF trace (used for the
  counterexample summaries).

Run everything:

```bash
./run.sh                       # ~20 min, prints the PASS/FAIL/VIOLATION lines
# single check, e.g.:
Q=quint-tool/node_modules/.bin/quint
$Q typecheck snapshot_store.qnt
$Q run cfg_lapsing_bskew3.qnt --init=M::init --step=M::step --invariant=M::I3 \
   --max-steps=60 --max-samples=100000 --seed=11 --verbosity=1
$Q test cfg_lapsing_r1.qnt          # directed runs
```
