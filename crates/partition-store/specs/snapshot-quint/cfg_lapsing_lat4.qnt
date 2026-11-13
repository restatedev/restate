// C8: same as C5 (LAPSING, unbounded stall, no clock skew) but the in-flight delete bound is
// LAT = 4 >= MARGIN = 3, i.e. the operation-deadline assumption is broken: a delete started while
// the lease is still valid may land after the lease expired and after the next holder published.
module cfg_lapsing_lat4 {
  import snapshot_store(
    N = 2, F = 4, NUM_RETAINED = 2, MODE = 1, MAX_SKEW = 0,
    BOUNDED = false, MAX_STALL = 1, ALLOW_COMMIT_ERR = false, ALLOW_CRASH = false,
    LAT = 4
  ) as M from "./snapshot_store"

  // A fenced delete that stays in flight for LAT = 4 >= MARGIN ticks outlives the operation
  // deadline: it starts at now 13 while the lease is valid (13 + 3 < 17) and lands at now 17,
  // the earliest instant node 1 may take over. Node 1 publishes a snapshot that reuses ssts/2
  // in between, so the landing destroys a referenced SST -> I1, and it lands while the stored
  // lease belongs to node 1 -> I3.
  run lateLandingDeletesRetainedSstTest =
    M::init
      .then(M::acquire(0))
      .then(M::exportWith(0, 1, 4))     // abandoned attempt: files {2}, lsn 1
      .then(M::headAndUploadF(0, 2))    // ssts/2 at mtime 0
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                    // now 7
      .then(M::loseLease(0))
      .then(M::discard(0))
      .then(M::release(0))
      .then(M::acquire(0))              // exp = 17
      .then(M::exportWith(0, 1, 8))     // files {3}, lsn 1
      .then(M::headAndUploadF(0, 3))
      .then(M::putMetadata(0))
      .then(M::readPointer(0))
      .then(M::checkLease(0))
      .then(M::publishOk(0))            // retained [s2]
      .then(M::checkLease(0))
      .then(M::cleanupRead(0))          // refSet = {3}
      .then(M::checkLease(0))
      .then(M::sweepList(0))            // toDelete = {2}
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                    // now 13
      .then(M::checkLease(0))           // (a) valid: 13 + 3 < 17
      .then(M::fenceConfirm(0))         // (b) stored lease id is ours (L:476)
      .then(M::startSweepDeleteF(0, 2)) // (c) starts at 13, may land until 17
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                    // now 17: Tick is blocked past start + LAT
      .then(M::acquire(1))              // takeover at 17
      .then(M::exportWith(1, 1, 4))     // files {2}, lsn 2
      .then(M::headAndUploadF(1, 2))    // reuse of ssts/2
      .then(M::putMetadata(1))
      .then(M::readPointer(1))
      .then(M::checkLease(1))
      .then(M::publishOk(1))            // retained [s3, s2]; ssts/2 referenced again
      .then(M::landDeleteF(2))          // lands at start + LAT, lease is node 1's
      .then(M::checkpoint(and { not(M::I1), not(M::I3) }))
}
