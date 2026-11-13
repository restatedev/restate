// Model A: SnapshotStore for Restate incremental snapshots (see SPEC.md).
//
// Grounded in (read-only) code, branch `incremental-snapshot-chaos`:
//   P = crates/partition-store/src/snapshots/repository.rs
//   L = crates/partition-store/src/snapshots/leases.rs
//   T = crates/partition-store/src/snapshots/snapshot_task.rs
//
// Object store is modelled as
//   sstPresent : Set[fingerprint]          the `ssts/{fingerprint}.sst` keys that exist
//   sstMtime   : fingerprint -> int        their last-modified time (only refreshed by a put)
//   metaObjs   : Set[snapshot id]          `lsn_*/metadata.json` keys that exist
//   metaFiles  : snapshot id -> Set[fp]    what the written metadata.json records as its files
// A "delete" removes a key from sstPresent / metaObjs; sstMtime and metaFiles are ghosts that keep
// the last written content (exactly what I1/I5 must reason about).
//
// Every action carries the path:line it models. Deviations from SPEC.md are listed in RESULTS.md.

module snapshot_store {

  // ---------------------------------------------------------------- knobs (SPEC.md)
  const N: int                 // nodes (2)
  const F: int                 // SST fingerprints (4)
  const NUM_RETAINED: int      // snapshot.retained {1,2}
  const MODE: int              // 0 = PERFECT (mutex), 1 = LAPSING
  const MAX_SKEW: int          // skew of node 1 vs node 0 (pairwise clock skew)
  const BOUNDED: bool          // bounded-stall mode
  const MAX_STALL: int         // max ticks between a local check and the action it guards
  const ALLOW_COMMIT_ERR: bool // enable PublishCommitButError
  const ALLOW_CRASH: bool      // enable Crash
  const LAT: int               // upper bound (ticks) on how late a started delete may land

  // Scaled protocol constants (SPEC.md knobs, spec values divided by 30s).
  pure val DURATION = 10       // L:44 LEASE_DURATION
  pure val MARGIN = 3          // L:47 LEASE_SAFETY_MARGIN
  pure val MIN_AGE = 6         // P:132 ORPHAN_MIN_AGE
  pure val MAX_SNAPS = 4       // <= 4 snapshots

  type Lease = { has: bool, holder: int, id: int, exp: int, ver: int }
  type Pointer = { has: bool, etag: int, latestLsn: int, latestId: int, retained: List[int] }

  type Local = {
    pc: str,                  // "idle" | "held" | "exported" | "meta" | "ready" | "cleanup"
                              // | "sweep" | "discarding" | "releasing"
    myId: int,                // lease id of our guard
    myExp: int,               // guard's cached expires_at (L:296, L:325)
    snapId: int,
    snapLsn: int,
    snapFiles: Set[int],
    uploaded: Set[int],
    created: Set[int],        // created_shared_ssts (P:2021)
    seenEtag: int,            // version read by get_latest_snapshot_metadata_for_update (P:1661)
    branch: str,              // build_latest_v2 verdict: "newer" | "equal" | "lower" | "none"
    refSet: Set[int],         // referenced SSTs read by cleanup (P:1211)
    toDelete: Set[int],       // sweep candidate list (P:1884)
    evicted: Set[int],
    chkValid: bool,           // result of the LAST local is_valid() (L:398)
    checkedAt: int,           // -1 = no outstanding local check
    fenced: bool,             // confirm_lease() confirmed the stored lease id is ours (L:280)
    refReady: bool,
  }

  type Ghost = {
    everPublished: Set[int],
    publishCount: int,
    foreignPublishes: int,      // PublishOk while the stored lease id is another id
    evictDeletes: int,
    sweepDeletes: int,
    discardSsts: int,
    i3Deletes: int,             // deletes of ssts/ keys while the lease belongs to another id
    discardedRetained: Set[int],// I4: ids discarded while still retained
    metaDeleted: Set[int],      // snapshot ids whose metadata was deleted by eviction
    maxPointerLsn: int,
    nextLease: int,
    nextSnap: int,
  }

  var now: int
  var lease: Lease
  var pointer: Pointer
  var sstPresent: Set[int]
  var sstMtime: int -> int
  var metaObjs: Set[int]
  var metaFiles: int -> Set[int]
  var lsnOf: int -> int
  // deletes that have been started through `fenced` and are still in flight: the shared SST keys
  // plus the start time and originating node of each (d) "lands within at most LAT ticks"
  var pendingSst: Set[int]
  var pendingStart: int -> int
  var pendingOwner: int -> int
  var pendingId: int -> int      // lease id under which the delete was started
  var pendingKind: int -> str     // "discard" | "evict" | "sweep"
  var loc: int -> Local
  var ghost: Ghost

  // ---------------------------------------------------------------- helpers
  pure def imax(a: int, b: int): int = if (a > b) a else b
  pure def imin(a: int, b: int): int = if (a < b) a else b

  pure def skew(n: int): int = if (n == 0) 0 else MAX_SKEW

  def L(n: int): Local = loc.get(n)

  // node n's wall clock (SPEC.md: skew[n]); the model's `now` is the true time
  def localNow(n: int): int = now + skew(n)

  // L:353-354  is_valid(): now + MARGIN < expires_at, against the guard's cached expiry.
  // PERFECT mode is the atomic-mutex baseline: the holder never observes a lapse.
  def localValid(n: int): bool =
    if (MODE == 0) true else localNow(n) + MARGIN < L(n).myExp

  // L:384 `existing_lease.is_expired(&self.clock)`: takeover only at true expiry, by the
  // acquirer's clock. PERFECT mode: a mutex, only acquirable when the slot is empty.
  def canAcquire(n: int): bool =
    if (not(lease.has)) true
    // PERFECT mode is a mutex: only the release marker (L:462 expires_at = UNIX_EPOCH, our
    // exp == 0) frees the slot; the mutex itself never lapses.
    else if (MODE == 0) lease.exp == 0
    else localNow(n) >= lease.exp

  def filesOf(s: int): Set[int] =
    if (metaFiles.keys().contains(s)) metaFiles.get(s) else Set()

  pure def listToSet(l: List[int]): Set[int] =
    l.foldl(Set(), (acc, e) => acc.union(Set(e)))

  pure def prepend(l: List[int], x: int): List[int] =
    l.foldl(List(x), (acc, e) => acc.append(e))

  def retainedSet: Set[int] = listToSet(pointer.retained)

  def retains(x: int): bool = pointer.retained.indices().exists(i => pointer.retained[i] == x)

  def retainedFiles: Set[int] =
    pointer.retained.foldl(Set(), (acc, s) => acc.union(filesOf(s)))

  def evictedFilesOf(n: int): Set[int] =
    L(n).evicted.fold(Set(), (acc, s) => acc.union(filesOf(s)))

  // P:1066-1073: newest LSN = max(pointer.min_applied_lsn, max over retained)
  def newestLsn: int =
    pointer.retained.foldl(pointer.latestLsn, (acc, s) => imax(acc, lsnOf.get(s)))

  // P:1083-1091 + P:106-110: retained list kept in strictly descending LSN order
  def descLsn(ids: List[int]): bool =
    ids.foldl({ prev: 0, ok: true },
      (acc, s) => { prev: lsnOf.get(s), ok: acc.ok and (acc.prev == 0 or acc.prev > lsnOf.get(s)) }
    ).ok

  // the stored slot belongs to some lease id that is not ours (SPEC.md I3 ghost shape)
  def leaseForeign(n: int): bool = lease.has and lease.id != L(n).myId

  def sstExists(f: int): bool = sstPresent.contains(f)

  // no shared-SST delete that this node started is still in flight
  def noPending(n: int): bool = pendingSst.forall(f => pendingOwner.get(f) != n)

  def allRetainedMetaPresent: bool =
    pointer.retained.indices().forall(i => metaObjs.contains(pointer.retained[i]))

  // ---------------------------------------------------------------- initial state
  action init = all {
    now' = 0,
    lease' = { has: false, holder: -1, id: 0, exp: 0, ver: 0 },
    pointer' = { has: false, etag: 0, latestLsn: 0, latestId: 0, retained: [] },
    sstPresent' = Set(),
    sstMtime' = Map(),
    metaObjs' = Set(),
    metaFiles' = Map(),
    lsnOf' = Map(),
    pendingSst' = Set(),
    pendingStart' = Map(),
    pendingOwner' = Map(),
    pendingId' = Map(),
    pendingKind' = Map(),
    loc' = 0.to(N - 1).mapBy(n => {
      pc: "idle", myId: 0, myExp: 0, snapId: 0, snapLsn: 0, snapFiles: Set(), uploaded: Set(),
      created: Set(), seenEtag: -1, branch: "none", refSet: Set(), toDelete: Set(), evicted: Set(),
      chkValid: false, checkedAt: -1, fenced: false, refReady: false }),
    ghost' = {
      everPublished: Set(), publishCount: 0, foreignPublishes: 0, evictDeletes: 0, sweepDeletes: 0,
      discardSsts: 0, i3Deletes: 0, discardedRetained: Set(), metaDeleted: Set(),
      maxPointerLsn: 0, nextLease: 1, nextSnap: 1 },
  }

  // ---------------------------------------------------------------- Tick (SPEC.md)
  // Bounded mode: no node may hold an outstanding local check older than MAX_STALL.
  // checkedAt == -1 means "no outstanding check" (node idle, or it just acted).
  action tick = all {
    if (BOUNDED)
      (0.to(N - 1).forall(n => L(n).checkedAt < 0 or now - L(n).checkedAt <= MAX_STALL))
    else true,
    // an in-flight delete must land within LAT ticks, so the clock may not advance past start+LAT
    pendingSst.forall(f => now - pendingStart.get(f) < LAT),
    now' = now + 1,
    lease' = lease,
    pointer' = pointer,
    sstPresent' = sstPresent,
    sstMtime' = sstMtime,
    pendingSst' = pendingSst,
    pendingStart' = pendingStart,
    pendingOwner' = pendingOwner,
    pendingId' = pendingId,
    pendingKind' = pendingKind,
    metaObjs' = metaObjs,
    metaFiles' = metaFiles,
    lsnOf' = lsnOf,
    loc' = loc,
    ghost' = ghost,
  }

  // ---------------------------------------------------------------- Acquire
  // L:395-435 try_acquire: read the slot, CAS on version, take over only if the stored
  // value is expired *by our* clock (L:439). T:88 acquires before the export.
  action acquire(n: int): bool = all {
    L(n).pc == "idle",
    canAcquire(n),
    val id = ghost.nextLease
    val exp = localNow(n) + DURATION
    all {
      lease' = { has: true, holder: n, id: id, exp: exp, ver: lease.ver + 1 },
      loc' = loc.set(n, { ...L(n), pc: "held", myId: id, myExp: exp, snapId: 0, snapLsn: 0,
        snapFiles: Set(), uploaded: Set(), created: Set(), seenEtag: -1, branch: "none",
        refSet: Set(), toDelete: Set(), evicted: Set(), chkValid: false, checkedAt: -1, fenced: false,
        refReady: false }),
      ghost' = { ...ghost, nextLease: id + 1 },
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Renew
  // L:265-315 renew(): read the slot, if the stored lease_id is ours CAS a new expiry that is
  // computed from *our* clock. Note it never checks its own expiry.
  action renew(n: int): bool = all {
    L(n).pc != "idle",
    lease.has,
    lease.id == L(n).myId,
    val exp = localNow(n) + DURATION
    all {
      lease' = { ...lease, exp: exp, ver: lease.ver + 1 },
      loc' = loc.set(n, { ...L(n), myExp: exp }),
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- LoseLease
  // L:353 is_valid() false -> the work is aborted: run_under_lease (L:200-240) cancels and
  // put's pre-publish check (P:932) errors -> Discard.
  action loseLease(n: int): bool = all {
    L(n).pc != "idle",
    lease.has,
    lease.id == L(n).myId,
    not(localValid(n)),
    all {
      // noticing the lapse is itself a local validity check: the last check now says invalid,
      // so the discard path will keep the shared SSTs it created (P:734).
      loc' = loc.set(n, { ...L(n), pc: "discarding", chkValid: false }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Export
  // P:887-920 put_snapshot_inner: the local export happens after the lease is taken
  // (T:88-107). An LSN and a file subset are chosen here, which is what makes sharing appear.
  action exportWith(n: int, d: int, mask: int): bool = all {
    L(n).pc == "held",
    ghost.nextSnap <= MAX_SNAPS,
    d >= 1,
    val lsn = (if (pointer.has) pointer.latestLsn else 0) + d
    val files = 0.to(F - 1).filter(f => (mask / (2 ^ f)) % 2 == 1)
    all {
      loc' = loc.set(n, { ...L(n), pc: "exported", snapId: ghost.nextSnap, snapLsn: lsn,
        snapFiles: files, uploaded: Set(), created: Set() }),
      lsnOf' = lsnOf.put(ghost.nextSnap, lsn),
      ghost' = { ...ghost, nextSnap: ghost.nextSnap + 1 },
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      now' = now,
    },
  }

  action exportAttempt(n: int): bool = all {
    L(n).pc == "held",
    ghost.nextSnap <= MAX_SNAPS,
    nondet d = 1.to(2).oneOf()
    nondet mask = 1.to(2 ^ F - 1).oneOf()
    exportWith(n, d, mask),
  }

  // ---------------------------------------------------------------- HeadAndUpload
  // P:790-838 upload_snapshot_with_dedup: head(); if present skip the upload *without*
  // refreshing the object's mtime, and only a key that did not exist is `creates_shared`.
  action headAndUploadF(n: int, f: int): bool = all {
    L(n).pc == "exported",
    L(n).snapFiles.contains(f),
    not(L(n).uploaded.contains(f)),
    val wasPresent = sstExists(f)
    all {
      sstPresent' = sstPresent.union(Set(f)),
      sstMtime' = if (wasPresent) sstMtime else sstMtime.put(f, now),
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      loc' = loc.set(n, { ...L(n), uploaded: L(n).uploaded.union(Set(f)),
        created: if (wasPresent) L(n).created else L(n).created.union(Set(f)) }),
      lease' = lease,
      pointer' = pointer,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  action headAndUpload(n: int): bool = all {
    L(n).pc == "exported",
    L(n).snapFiles.exclude(L(n).uploaded).size() > 0,
    nondet f = L(n).snapFiles.exclude(L(n).uploaded).oneOf()
    headAndUploadF(n, f),
  }

  // ---------------------------------------------------------------- PutMetadata
  // P:1052-1065: write metadata.json (recorded in own_objects, P:1066).
  action putMetadata(n: int): bool = all {
    L(n).pc == "exported",
    L(n).uploaded == L(n).snapFiles,
    val id = L(n).snapId
    all {
      metaObjs' = metaObjs.union(Set(id)),
      metaFiles' = metaFiles.put(id, L(n).snapFiles),
      loc' = loc.set(n, { ...L(n), pc: "meta" }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- ReadPointer
  // P:900-905 get_latest_snapshot_metadata_for_update: read latest.json and remember its
  // version (our `seenEtag`) plus the build_latest_v2 verdict (P:1066-1093).
  action readPointer(n: int): bool = all {
    L(n).pc == "meta",
    all {
      loc' = loc.set(n, { ...L(n), pc: "ready", seenEtag: pointer.etag,
        branch: if (not(pointer.has)) "newer"
                else if (L(n).snapLsn == newestLsn) "equal"
                else if (L(n).snapLsn < newestLsn) "lower"
                else "newer" }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- CheckLease
  // L:353 is_valid(): pure local clock test, recorded in checkedAt. Required before Publish
  // (P:932) and before each delete batch (P:734, P:1219, P:1235, P:1926, P:1986).
  action checkLease(n: int): bool = all {
    L(n).pc != "idle",
    all {
      loc' = loc.set(n, { ...L(n), chkValid: localValid(n), checkedAt: now }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // shared body of the three publish outcomes
  action publishWith(n: int, takeErrorPath: bool): bool = all {
    L(n).pc == "ready",
    L(n).branch == "newer",
    L(n).seenEtag == pointer.etag,   // the conditional put's MatchesVersion precondition
    localValid(n),                   // P:933 re-check of the lease right before the CAS
                                     // (modelled atomically with the CAS: the remaining
                                     // check->CAS window is not expressed, see RESULTS.md)
    val id = L(n).snapId
    val ids = prepend(if (pointer.has) pointer.retained else [], id)
    val kept = ids.slice(0, imin(NUM_RETAINED, ids.length()))
    val dropped = ids.slice(kept.length(), ids.length())
    all {
      pointer' = { has: true, etag: pointer.etag + 1, latestLsn: L(n).snapLsn,
                   latestId: id, retained: kept },
      loc' = loc.set(n,
        if (takeErrorPath)
          // P:946-951: the conditional put returned an error, but the write may have landed.
          // put_snapshot_inner re-reads latest.json before discarding anything, so the node
          // enters a "resolving" phase that keeps the upload progress.
          { ...L(n), pc: "resolving", evicted: listToSet(dropped), chkValid: false }
        else
          // P:958-971: spawn_cleanup_task with the evicted list, same lease guard.
          { ...L(n), pc: "cleanup", evicted: listToSet(dropped), snapId: 0, snapFiles: Set(),
            uploaded: Set(), created: Set(), chkValid: false, refSet: Set(),
            toDelete: Set(), refReady: false }),
      ghost' = { ...ghost,
        everPublished: ghost.everPublished.union(Set(id)),
        publishCount: ghost.publishCount + 1,
        foreignPublishes: ghost.foreignPublishes + (if (leaseForeign(n)) 1 else 0),
        maxPointerLsn: imax(ghost.maxPointerLsn, L(n).snapLsn) },
      lease' = lease,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- PublishOk
  // P:931-957: conditional put of latest.json, then the cleanup task.
  action publishOk(n: int): bool = all {
    L(n).pc == "ready",
    publishWith(n, false),
  }

  // ---------------------------------------------------------------- PublishCommitButError
  // P:938-951: `put_opts` returns Err even though the write landed; instead of discarding, the
  // writer re-reads latest.json (resolve_failed_pointer_update, P:992-1052). The store effect of
  // the committed write is applied here; the re-read is the separate step below.
  action publishCommitButError(n: int): bool = all {
    L(n).pc == "ready",
    ALLOW_COMMIT_ERR,
    publishWith(n, true),
  }

  // ---------------------------------------------------------------- Resolve: pointer lists us
  // P:1017-1038: the re-read pointer carries this snapshot id (top-level or retained) -> the
  // snapshot counts as published and its files are kept. If it is still the newest, cleanup runs
  // exactly as on success (P:968-974); if a newer snapshot took the top, that writer owns eviction
  // and ours returns Published without spawning cleanup (P:952-958).
  action resolveListed(n: int): bool = all {
    L(n).pc == "resolving",
    pointer.has,
    retains(L(n).snapId),
    all {
      loc' = loc.set(n,
        if (pointer.latestId == L(n).snapId)
          { ...L(n), pc: "cleanup", snapId: 0, snapFiles: Set(), uploaded: Set(),
            created: Set(), chkValid: false, refSet: Set(), toDelete: Set(), refReady: false }
        else
          { ...L(n), pc: "releasing", snapId: 0, created: Set() }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Resolve: pointer lost us
  // P:1039-1051: the pointer is known to exist and not to list this snapshot, so the write is
  // treated as a failure and the files are discarded as before (P:717-720).
  action resolveNotListed(n: int): bool = all {
    L(n).pc == "resolving",
    pointer.has,
    not(retains(L(n).snapId)),
    all {
      loc' = loc.set(n, { ...L(n), pc: "discarding" }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Resolve: re-read failed
  // P:1006-1016: the pointer cannot be re-read, so nothing is known and the uploaded files are
  // kept for the orphan sweep (an empty progress is returned, so put() discards nothing).
  action resolveReadFails(n: int): bool = all {
    L(n).pc == "resolving",
    all {
      loc' = loc.set(n, { ...L(n), pc: "releasing", snapId: 0, created: Set() }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- PublishRejected
  // P:941-945 + P:1105-1120: PutMode::Update(version) fails with FailedPrecondition ->
  // error path -> Discard (the store is unchanged).
  action publishRejected(n: int): bool = all {
    L(n).pc == "ready",
    L(n).branch == "newer",
    L(n).seenEtag != pointer.etag,
    all {
      loc' = loc.set(n, { ...L(n), pc: "discarding" }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Superseded
  // P:906-915: build_latest_v2 returns None for equal/lower LSN -> Ok(Superseded) ->
  // discard_uploaded_files (P:709-716).
  action superseded(n: int): bool = all {
    L(n).pc == "ready",
    L(n).branch != "newer",
    all {
      loc' = loc.set(n, { ...L(n), pc: "discarding" }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Fencing (a)(b)(c)
  // L:270-299 SnapshotLeaseGuard::fenced:
  //   (a) L:274-276 if !self.is_valid() { return None }   -- the local check
  //   (b) L:278-283 g.confirm_lease() (L:470-505) reads the lease back from the metadata store and
  //       compares the stored lease_id with ours; fails closed on a mismatch, a missing value or
  //       a read error.
  //   (c) L:288-296 run_under_lease(|| { if self.is_valid() { op } else { None } }): the
  //       operation starts only after a *second* local check and is bounded by the operation
  //       deadline expires_at - LEASE_SAFETY_MARGIN (L:634-641, run_under_lease L:219-258).
  // A stall may fall between (a), (b) and (c); the model makes each of them its own step.
  action fenceConfirm(n: int): bool = all {
    L(n).pc != "idle",
    L(n).chkValid,                     // (a) passed: the last local check said valid (L:274)
    lease.has,
    lease.id == L(n).myId,             // (b) the stored lease id is still ours (L:476)
    all {
      loc' = loc.set(n, { ...L(n), fenced: true }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // The fenced operation is refused (local check failed, confirm failed, or the deadline in (c)
  // has passed): delete_under_lease stops and leaves the rest to the orphan sweep
  // (P:770-775). The pending phase ends.
  action fenceAbort(n: int): bool = all {
    noPending(n),
    L(n).pc != "idle",
    or { not(L(n).chkValid), leaseForeign(n), not(lease.has), now + MARGIN >= L(n).myExp },
    all {
      loc' = loc.set(n,
        if (L(n).pc == "discarding") { ...L(n), pc: "releasing", fenced: false, created: Set() }
        else if (L(n).pc == "sweep") { ...L(n), pc: "releasing", fenced: false }
        else if (L(n).pc == "cleanup") { ...L(n), pc: "releasing", fenced: false }
        else { ...L(n), fenced: false }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // (c) start the delete of one shared SST: both checks must still hold and the operation must
  // still be inside the lease's operation deadline, so this re-evaluates the local predicate.
  action startSharedDelete(n: int, f: int, kind: str): bool = all {
    L(n).fenced,                       // (b) succeeded earlier
    localValid(n),                     // (c) second local check at the start of the operation
    now + MARGIN < L(n).myExp,         // (c) operation deadline expires_at - MARGIN
    not(pendingSst.contains(f)),
    sstExists(f),
    all {
      pendingSst' = pendingSst.union(Set(f)),
      pendingStart' = pendingStart.put(f, now),
      pendingOwner' = pendingOwner.put(f, n),
      pendingId' = pendingId.put(f, lease.id),
      pendingKind' = pendingKind.put(f, kind),
      loc' = loc.set(n, { ...L(n), fenced: false,
        created: if (kind == "discard") L(n).created.exclude(Set(f)) else L(n).created,
        toDelete: if (kind == "sweep") L(n).toDelete.exclude(Set(f)) else L(n).toDelete }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // discard of created shared SSTs (P:727-742: own objects unfenced, created shared SSTs through
  // delete_under_lease, i.e. fenced, P:740-741)
  action startDiscardDeleteF(n: int, f: int): bool = all {
    L(n).pc == "discarding",
    L(n).created.contains(f),
    startSharedDelete(n, f, "discard"),
  }

  action startDiscardDelete(n: int): bool = all {
    L(n).pc == "discarding",
    all {
      val cands = L(n).created.filter(f => sstExists(f) and not(pendingSst.contains(f)))
      all {
        cands.size() > 0,
        nondet f = cands.oneOf()
        startDiscardDeleteF(n, f),
      },
    },
  }

  // eviction cleanup (P:1500-1520: the unreferenced files of an evicted snapshot go through
  // delete_under_lease)
  action startEvictDeleteF(n: int, f: int): bool = all {
    L(n).pc == "cleanup",
    L(n).refReady,
    L(n).evicted.size() > 0,
    evictedFilesOf(n).contains(f),
    not(L(n).refSet.contains(f)),
    startSharedDelete(n, f, "evict"),
  }

  action startEvictDelete(n: int): bool = all {
    L(n).pc == "cleanup",
    L(n).refReady,
    all {
      val cands = evictedFilesOf(n).exclude(L(n).refSet)
        .filter(f => sstExists(f) and not(pendingSst.contains(f)))
      all {
        cands.size() > 0,
        nondet f = cands.oneOf()
        startEvictDeleteF(n, f),
      },
    },
  }

  // orphan sweep (P:2039 delete_under_lease(orphans.ssts, lease_guard))
  action startSweepDeleteF(n: int, f: int): bool = all {
    L(n).pc == "sweep",
    L(n).toDelete.contains(f),
    startSharedDelete(n, f, "sweep"),
  }

  action startSweepDelete(n: int): bool = all {
    L(n).pc == "sweep",
    all {
      val cands = L(n).toDelete.filter(f => sstExists(f) and not(pendingSst.contains(f)))
      all {
        cands.size() > 0,
        nondet f = cands.oneOf()
        startSweepDeleteF(n, f),
      },
    },
  }

  // ---------------------------------------------------------------- LandDelete
  // The request is in flight: it lands non-deterministically at any time in [start, start+LAT].
  // Tick refuses to advance past start+LAT, so it always lands within LAT ticks. The node that
  // started it may by now have lost the lease; that is exactly what I3 measures.
  action landDeleteF(f: int): bool = all {
    pendingSst.contains(f),
    now - pendingStart.get(f) <= LAT,
    val n = pendingOwner.get(f)
    val kind = pendingKind.get(f)
    all {
      pendingSst' = pendingSst.exclude(Set(f)),
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      sstPresent' = sstPresent.exclude(Set(f)),
      ghost' = { ...ghost,
        i3Deletes: ghost.i3Deletes + (if (lease.has and lease.id != pendingId.get(f)) 1 else 0),
        evictDeletes: ghost.evictDeletes + (if (kind == "evict") 1 else 0),
        sweepDeletes: ghost.sweepDeletes + (if (kind == "sweep") 1 else 0),
        discardSsts: ghost.discardSsts + (if (kind == "discard") 1 else 0) },
      loc' = loc,
      lease' = lease,
      pointer' = pointer,
      sstMtime' = sstMtime,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      now' = now,
    },
  }

  action landDelete: bool = all {
    pendingSst.size() > 0,
    nondet f = pendingSst.oneOf()
    landDeleteF(f),
  }

  // ---------------------------------------------------------------- CleanupDone
  // P:770-775: a fenced batch that is refused (or a finished pass) ends the cleanup; whatever is
  // left is the orphan sweep's business.
  action cleanupDone(n: int): bool = all {
    noPending(n),
    or { L(n).pc == "cleanup", L(n).pc == "sweep" },
    all {
      loc' = loc.set(n, { ...L(n), pc: "releasing", fenced: false }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Discard
  // P:726-743 discard_uploaded_files: own_objects are always deleted; created shared SSTs are
  // deleted only while is_valid() (P:734 take_while). The validity used here is the result of
  // the *last* local check, which is what a stalled node still believes.
  action discard(n: int): bool = all {
    noPending(n),
    L(n).pc == "discarding",
    L(n).snapId != 0,
    val id = L(n).snapId
    all {
      // P:734-738: own_objects of the unpublished snapshot are deleted without any fencing.
      metaObjs' = metaObjs.exclude(Set(id)),
      ghost' = { ...ghost,
        discardedRetained: if (retainedSet.contains(id))
          ghost.discardedRetained.union(Set(id)) else ghost.discardedRetained },
      // created shared SSTs that were never started are left to the orphan sweep (P:770-775).
      loc' = loc.set(n, { ...L(n), pc: "releasing", created: Set(), fenced: false }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- CleanupRead
  // P:1202-1215 + P:1289-1327 + P:1799-1860: read latest.json and every retained metadata,
  // bail (AbortCleanup) if any is missing, and build the referenced-SST set.
  action cleanupRead(n: int): bool = all {
    L(n).pc == "cleanup",
    L(n).chkValid,
    allRetainedMetaPresent,
    all {
      loc' = loc.set(n, { ...L(n), refSet: retainedFiles, refReady: true, chkValid: false }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  action abortCleanup(n: int): bool = all {
    L(n).pc == "cleanup",
    not(allRetainedMetaPresent),
    all {
      loc' = loc.set(n, { ...L(n), pc: "releasing" }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- EvictDeleteMeta
  // P:1490-1520: delete metadata.json only once every non-referenced file is gone, and through
  // delete_under_lease as well (P:1515), so it is fenced too.
  action evictDeleteMetaF(n: int, s: int): bool = all {
    L(n).pc == "cleanup",
    L(n).refReady,
    L(n).fenced,
    localValid(n),
    L(n).evicted.contains(s),
    metaObjs.contains(s),
    filesOf(s).exclude(L(n).refSet).forall(f => not(sstExists(f))),
    all {
      metaObjs' = metaObjs.exclude(Set(s)),
      ghost' = { ...ghost, metaDeleted: ghost.metaDeleted.union(Set(s)) },
      loc' = loc.set(n, { ...L(n), fenced: false }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      now' = now,
    },
  }

  action evictDeleteMeta(n: int): bool = all {
    L(n).pc == "cleanup",
    all {
      val cands = L(n).evicted.filter(s => metaObjs.contains(s)
        and filesOf(s).exclude(L(n).refSet).forall(f => not(sstExists(f))))
      all {
        cands.size() > 0,
        nondet s = cands.oneOf()
        evictDeleteMetaF(n, s),
      },
    },
  }

  // ---------------------------------------------------------------- SweepList
  // P:1235-1240 claim_orphan_scan + P:1746-1795 scan_partition_files +
  // P:1868-1912 find_orphaned_files: shared SSTs that no retained snapshot references and
  // whose last-modified time is at least MIN_AGE old.
  action sweepList(n: int): bool = all {
    L(n).pc == "cleanup",
    L(n).refReady,
    L(n).chkValid,
    all {
      loc' = loc.set(n, { ...L(n), pc: "sweep", chkValid: false,
        toDelete: sstPresent.filter(f => now - sstMtime.get(f) >= MIN_AGE
          and not(L(n).refSet.contains(f))) }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Release
  // L:459-500 spawn_release_task: CAS a marker with expires_at = UNIX_EPOCH, but only if the
  // slot still carries our lease id (L:471).
  action release(n: int): bool = all {
    noPending(n),
    L(n).pc == "releasing",
    all {
      lease' = if (lease.has and lease.id == L(n).myId)
        { ...lease, exp: 0, ver: lease.ver + 1 } else lease,
      loc' = loc.set(n, { ...L(n), pc: "idle", myId: 0, myExp: 0, chkValid: false,
        checkedAt: -1, snapId: 0, refReady: false, evicted: Set(), toDelete: Set(),
        refSet: Set(), created: Set() }),
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- Crash
  // SPEC.md "Crash(n): any pc; drops local state; the lease stays until it expires."
  action crash(n: int): bool = all {
    ALLOW_CRASH,
    L(n).pc != "idle",
    all {
      loc' = loc.set(n, { ...L(n), pc: "idle", myId: 0, myExp: 0, chkValid: false,
        checkedAt: -1, snapId: 0, snapFiles: Set(), uploaded: Set(), created: Set(),
        refSet: Set(), toDelete: Set(), evicted: Set(), refReady: false }),
      lease' = lease,
      pointer' = pointer,
      sstPresent' = sstPresent,
      sstMtime' = sstMtime,
      pendingSst' = pendingSst,
      pendingStart' = pendingStart,
      pendingOwner' = pendingOwner,
      pendingId' = pendingId,
      pendingKind' = pendingKind,
      metaObjs' = metaObjs,
      metaFiles' = metaFiles,
      lsnOf' = lsnOf,
      ghost' = ghost,
      now' = now,
    },
  }

  // ---------------------------------------------------------------- step
  action step = any {
    tick,
    landDelete,
    nondet n = 0.to(N - 1).oneOf()
    any {
      acquire(n), renew(n), loseLease(n), exportAttempt(n), headAndUpload(n), putMetadata(n),
      readPointer(n), checkLease(n), publishOk(n), publishCommitButError(n),
      resolveListed(n), resolveNotListed(n), resolveReadFails(n),
      publishRejected(n), superseded(n), discard(n), cleanupRead(n), abortCleanup(n),
      fenceConfirm(n), fenceAbort(n), startDiscardDelete(n), startEvictDelete(n),
      startSweepDelete(n), evictDeleteMeta(n), sweepList(n), cleanupDone(n),
      release(n), crash(n),
    },
  }

  // Test-only step: stutter one step and assert a predicate. Used by the directed runs in the
  // configuration modules (`run` chains need every step to assign the same variable set).
  action checkpoint(cond: bool): bool = all {
    cond,
    now' = now,
    lease' = lease,
    pointer' = pointer,
    sstPresent' = sstPresent,
    sstMtime' = sstMtime,
    pendingSst' = pendingSst,
    pendingStart' = pendingStart,
    pendingOwner' = pendingOwner,
    pendingId' = pendingId,
    pendingKind' = pendingKind,
    metaObjs' = metaObjs,
    metaFiles' = metaFiles,
    lsnOf' = lsnOf,
    loc' = loc,
    ghost' = ghost,
  }

  // ---------------------------------------------------------------- invariants
  // I1: every retained snapshot has its metadata and all of its files.
  val I1 = pointer.retained.indices().forall(i =>
    metaObjs.contains(pointer.retained[i])
    and filesOf(pointer.retained[i]).forall(f => sstExists(f)))

  // I2: retained strictly descending in LSN; the pointer LSN never decreases.
  val I2 = descLsn(pointer.retained) and pointer.latestLsn >= ghost.maxPointerLsn

  // I3: no delete of an ssts/ key while the store's lease belongs to another id.
  val I3 = ghost.i3Deletes == 0

  // I4: a snapshot id that was published and is still retained is never Discarded.
  val I4 = ghost.discardedRetained.size() == 0

  // I5: EvictDeleteMeta(s) implies no file unique to s remains.
  val I5 = ghost.metaDeleted.forall(s => filesOf(s).forall(f =>
    not(sstExists(f))
    or pointer.retained.indices().exists(i =>
      pointer.retained[i] != s and filesOf(pointer.retained[i]).contains(f))))

  val allInvariants = and { I1, I2, I3, I4, I5 }

  // ---------------------------------------------------------------- reachability witnesses
  // Reachability witnesses: `quint run --witnesses` counts the traces in which the expression is
  // true in some state, so a witness must be WITNESSED (count > 0) in a run to show that the
  // interesting state is reachable. The spec asks for witnesses that must be reachable even
  // though they look "impossible" (e.g. "never 3 publishes").
  val W_publishes = ghost.publishCount >= 3          // 3 publishes in one trace
  val W_evictions = ghost.evictDeletes >= 1          // an eviction delete
  val W_discardSsts = ghost.discardSsts >= 1         // a created-SST discard
  val W_foreignPublish = ghost.foreignPublishes >= 1 // publish while the lease id is another's
  val W_sweeps = ghost.sweepDeletes >= 1             // a sweep delete
  val W_discardRetained = ghost.discardedRetained.size() >= 1 // commit-but-error discard
}
