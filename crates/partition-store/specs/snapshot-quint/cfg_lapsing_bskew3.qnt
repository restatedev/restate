// C4: LAPSING, bounded stall (MAX_STALL = 1), LAT = 1, clock skew exactly equal to the safety
// margin (MAX_SKEW = 3 = MARGIN). Fencing bounds the delete by the operation deadline on the
// *deleter's* clock (exp - MARGIN) and checks the stored lease id, but with skew = MARGIN the new
// holder can take over at exactly the instant the fenced delete is allowed to land, so I1 and I3
// are still reachable: the assumption "pairwise skew < MARGIN" is what breaks here.
module cfg_lapsing_bskew3 {
  import snapshot_store(
    N = 2, F = 4, NUM_RETAINED = 2, MODE = 1, MAX_SKEW = 3,
    BOUNDED = true, MAX_STALL = 1, ALLOW_COMMIT_ERR = false, ALLOW_CRASH = false, LAT = 1
  ) as M from "./snapshot_store"

  // Node 0 (skew 0) is the sweeper. ssts/2 is left behind by an abandoned attempt, node 0
  // publishes a second snapshot, lists ssts/2 as an orphan, keeps revalidating (bounded stall
  // forces a fresh check every tick), and node 1 (skew +3) takes the lease over at now = 14,
  // exactly one tick after node 0's last valid check.
  run skewEqualsMarginBoundedStallTest =
    M::init
      .then(M::acquire(0))            // L:395 exp = 10, valid by node 0's clock until now 6
      .then(M::exportWith(0, 1, 4))   // abandoned attempt: files {2}, lsn 1
      .then(M::headAndUploadF(0, 2))  // ssts/2 created at mtime 0
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                  // now = 7, node 0's lease is invalid by its own clock
      .then(M::loseLease(0))          // L:353 -> abandon; the local check now says invalid
      .then(M::discard(0))            // P:726-743 with is_valid() false: ssts/2 is NOT deleted
      .then(M::release(0))            // L:459-500: exp = 0
      .then(M::acquire(0))            // exp = 17
      .then(M::exportWith(0, 1, 8))   // files {3}, lsn 1
      .then(M::headAndUploadF(0, 3))  // ssts/3 at mtime 7
      .then(M::putMetadata(0))
      .then(M::readPointer(0))
      .then(M::checkLease(0))         // valid: 7 + 3 + 3 < 17
      .then(M::publishOk(0))          // retained [s2]
      .then(M::checkLease(0))
      .then(M::cleanupRead(0))        // refSet = {3}
      .then(M::checkLease(0))
      .then(M::sweepList(0))          // toDelete = {2}: age 7 >= MIN_AGE and unreferenced
      .then(M::checkLease(0))         // bounded stall: revalidate ...
      .then(M::tick)
      .then(M::checkLease(0))         // bounded stall: revalidate ...
      .then(M::tick)
      .then(M::checkLease(0))         // bounded stall: revalidate ...
      .then(M::tick)
      .then(M::checkLease(0))         // bounded stall: revalidate ...
      .then(M::tick)
      .then(M::checkLease(0))         // bounded stall: revalidate ...
      .then(M::tick)
      .then(M::checkLease(0))         // bounded stall: revalidate ...
      .then(M::tick)
      .then(M::checkLease(0))         // (a) local check: 13 + 0 + MARGIN(3) < 17
      .then(M::fenceConfirm(0))       // (b) stored lease id is still ours (L:476)
      .then(M::startSweepDeleteF(0, 2)) // (c) second local check + deadline, delete goes out at 13
      .then(M::tick)                  // now 14: exactly one tick of stall, still within LAT = 1
      .then(M::acquire(1))            // L:439 takeover: 14 + 3 >= 17
      .then(M::exportWith(1, 1, 4))   // files {2}, lsn 2
      .then(M::headAndUploadF(1, 2))  // reuse of ssts/2, mtime stays 0
      .then(M::putMetadata(1))
      .then(M::readPointer(1))
      .then(M::checkLease(1))         // valid: 14 + 3 + 3 < 24
      .then(M::publishOk(1))          // retained [s3, s2], ssts/2 is referenced again
      .then(M::landDeleteF(2))        // lands at 14, exactly when node 1's clock allows takeover
      .then(M::checkpoint(and { not(M::I1), not(M::I3) }))
}
