# Model A: SnapshotStore (spec from the formal-methods consultant; build this)

Scope: one partition, lease as a switchable mode. In: object store as key -> (present, mtime); latest.json as (etag, retained list); SST identity as a small fingerprint set; a global integer clock. Out: bytes, sizes, multipart, RocksDB, the log; an LSN is an increasing integer per export.
Lease modes: PERFECT (atomic mutex, baseline) and LAPSING (holder caches exp; "valid" means now+skew[n]+MARGIN < exp; takeover allowed when now+skew[m] >= exp).

State. Store: objs, pointer, lease {holder, id, exp, ver}, now, skew[n]. Per node: pc[n], myExp[n], snap[n] (id, lsn, files), created[n] (shared keys this attempt uploaded), seenEtag[n], refSet[n], toDelete[n], checkedAt[n]. Ghost: everPublished (set of snapshot ids), plus ghost flags for action-level invariants.

Actions (each one atomic step; a stall is Tick firing between them):
- Tick: now+1. In bounded mode guard: no node has now - checkedAt > MAX_STALL.
- Acquire(n): guard lease absent or expired by n's clock; CAS on ver; set myExp.
- Renew(n): guard stored id is ours; CAS; extend exp. LoseLease(n): n notices invalid, goes to Abandon.
- Export(n): pick an LSN >= the pointer's and a nondeterministic file subset (overlap makes sharing).
- HeadAndUpload(n,f): if objs[f] present, reuse (mtime unchanged); else put and add to created.
- PutMetadata(n); ReadPointer(n): record seenEtag; branch on newer, equal or lower LSN.
- CheckLease(n): local validity; set checkedAt. Required before Publish and before each delete batch.
- PublishOk(n): guard etag == seenEtag; prepend, split at NUM_RETAINED, record evicted list.
- PublishCommitButError(n): same store effect, but n takes the error path (Discard), as the code does today.
- PublishRejected(n): etag mismatch -> Discard.
- Discard(n): delete own metadata; delete created only if the last check said valid.
- CleanupRead(n): read pointer and retained metadata; abort if any missing; refSet = union of retained files.
- EvictDelete(n,k): guard k in evicted files minus refSet. EvictDeleteMeta(n): only after all its SSTs gone.
- SweepList(n): toDelete = keys with now - mtime >= MIN_AGE, not in refSet, plus directories not retained.
- SweepDeleteBatch(n): delete up to BATCH keys since the last CheckLease.
- Crash(n): any pc; drops local state; the lease stays until it expires.

Invariants:
- I1 (main): every snapshot s in pointer.retained has its metadata and every key in files(s) present in objs.
- I2: pointer.retained strictly descending in LSN; pointer LSN never decreases.
- I3: no delete of an ssts/ key while the store's lease belongs to another id (ghost flag).
- I4: a snapshot id in everPublished and still retained is never the target of Discard.
- I5: EvictDeleteMeta(s) implies no file unique to s remains.

Expected results (calibration; the model is too weak if these do not show):
- PERFECT, no Crash, no CommitButError: I1-I5 hold.
- PERFECT + PublishCommitButError: I4 and I1 FAIL (verified in code: repository.rs put(), Err(put_error) => discard_uploaded_files, with no re-read of latest.json).
- LAPSING with MAX_STALL + pairwise skew < MARGIN: I1 and I3 hold.
- LAPSING with unbounded stall or MAX_SKEW >= MARGIN: I3 and I1 FAIL via: A does CleanupRead or SweepList and CheckLease, then stalls; B acquires, reuses old unreferenced X (mtime not refreshed so MIN_AGE gives no cover) and publishes; A then deletes X. Same shape for Discard of created keys.
- Under the same assumption I2 still holds (conditional write protects the pointer). Also show the lapsed-lease publish race is reachable: a PublishOk lands while the store's lease belongs to another id.

Knobs: 2 nodes; 4 fingerprints; <= 4 snapshots; NUM_RETAINED in {1,2}; DURATION=10, MARGIN=3, RENEW=4, MIN_AGE=6, BATCH=2; MAX_SKEW in {0,1,3}; MAX_STALL in {1, unbounded}; 60 steps; >= 100k samples (lower if runs take too long; report what you used).
Simulation caveats: random simulation rarely finds the stall trace unaided; also write it as a directed Quint run (a `run` that executes the exact interleaving and asserts the violation). Add reachability witnesses (e.g. "never 3 publishes", "never an eviction") that must be violated, to prove the model reaches interesting states.

# Model B: LeaseCore (second priority; only if Model A is done and time remains)
Metadata store as one versioned cell with linearizable CAS plus commit-but-error responses; nodes with generations; held[n] {id, exp}; pendingRelease[n]; attemptId[n].
Actions: AcquireAbsent, AcquireTakeover (expired, CAS on ver), AcquireAdopt (stored id == attemptId), AcquireCommitErr (write lands, retry same id), RenewRead + RenewCas, Drop (id -> pendingRelease), ReleaseRead + ReleaseCas (exp=0), Restart(n) (gen+1, forget held), Tick.
Invariants: L1 with MAX_SKEW < MARGIN at most one (n,id) holds a valid lease by its own clock; L2 ReleaseCas never changes a cell whose id differs from the released id; L3 held[n].exp <= cell.exp whenever the cell's id is ours; L4 no takeover of an unexpired lease, including from an earlier generation of the same node.
Expected: L1 fails once MAX_SKEW >= MARGIN (calibration).
