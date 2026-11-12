// C5: LAPSING with unbounded stall, no clock skew, NUM_RETAINED = 2, LAT = 1.
// AFTER FENCING this configuration is safe again: the stalled sweeper's shared-SST delete is
// refused because the stored lease id is no longer its own (L:476) and its local check has
// lapsed (L:274). The directed run drives the exact interleaving of the old counterexample and
// asserts that I1 and I3 now hold.
module cfg_lapsing_unb_skew0 {
  import snapshot_store(
    N = 2, F = 4, NUM_RETAINED = 2, MODE = 1, MAX_SKEW = 0,
    BOUNDED = false, MAX_STALL = 1, ALLOW_COMMIT_ERR = false, ALLOW_CRASH = false, LAT = 1
  ) as M from "./snapshot_store"

  // ssts/2 is left behind by an abandoned attempt (loseLease -> discard with an invalid local
  // check, so the created shared SST survives, P:734/P:770-775), node 0 publishes a second
  // snapshot, lists ssts/2 as an orphan, revalidates at now 13, stalls 4 ticks, node 1 takes the
  // lease over at now 17 and publishes a snapshot that reuses ssts/2. Node 0 now tries to delete
  // ssts/2 and `fenced` refuses it: local check lapsed and the stored lease id is node 1's.
  run unboundedStallIsRefusedByFencingTest =
    M::init
      .then(M::acquire(0))
      .then(M::exportWith(0, 1, 4))
      .then(M::headAndUploadF(0, 2))
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::loseLease(0))
      .then(M::discard(0))
      .then(M::release(0))
      .then(M::acquire(0))            // exp = 17
      .then(M::exportWith(0, 1, 8))
      .then(M::headAndUploadF(0, 3))
      .then(M::putMetadata(0))
      .then(M::readPointer(0))
      .then(M::checkLease(0))
      .then(M::publishOk(0))          // retained [s2]
      .then(M::checkLease(0))
      .then(M::cleanupRead(0))        // refSet = {3}
      .then(M::checkLease(0))
      .then(M::sweepList(0))          // toDelete = {2}
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                  // now 13
      .then(M::checkLease(0))
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                  // unbounded stall: now 17
      .then(M::acquire(1))            // takeover
      .then(M::exportWith(1, 1, 4))
      .then(M::headAndUploadF(1, 2))  // reuse of ssts/2, mtime stays 0
      .then(M::putMetadata(1))
      .then(M::readPointer(1))
      .then(M::checkLease(1))
      .then(M::publishOk(1))          // retained [s3, s2]; ssts/2 referenced again
      .then(M::checkLease(0))         // locally invalid
      .then(M::fenceAbort(0))         // fenced refuses: ssts/2 survives
      .then(M::checkpoint(and { M::I1, M::I3 }))
}
