// C7: LAPSING, unbounded stall, no clock skew, NUM_RETAINED = 1, Crash enabled, LAT = 1.
// AFTER FENCING the two races below no longer delete anything: the shared-SST delete is refused
// (fenceConfirm / the second local check fail), so these runs assert that I1 and I3 HOLD.
// Directed runs live here because they need this configuration. Each run executes one exact
// interleaving and asserts the violation the spec predicts.
module cfg_lapsing_r1 {
  import snapshot_store(
    N = 2, F = 4, NUM_RETAINED = 1, MODE = 1, MAX_SKEW = 0,
    BOUNDED = false, MAX_STALL = 1, ALLOW_COMMIT_ERR = false, ALLOW_CRASH = true, LAT = 1
  ) as M from "./snapshot_store"

  // Fenced sweep race: node 0's orphan ssts/2 is reused by node 1's publish, node 1 lists it for
  // the sweep and revalidates; node 0 then takes the lease over and publishes a snapshot that
  // reuses ssts/2. When node 1 now tries to delete, `fenced` refuses: the local check is already
  // invalid and the stored lease id belongs to node 0 (L:274, L:476, L:291), so nothing is deleted
  // and the rest is left to the orphan sweep (P:770-775).
  run sweepRaceIsRefusedByFencingTest =
    M::init
      .then(M::acquire(0))
      .then(M::exportWith(0, 1, 4))   // files {2}, lsn 1
      .then(M::headAndUploadF(0, 2))  // creates ssts/2, mtime 0
      .then(M::crash(0))              // orphan left, lease stays, exp 10
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                  // now 10
      .then(M::acquire(1))            // takeover
      .then(M::exportWith(1, 1, 8))   // node 1 publishes files {3}, lsn 1
      .then(M::headAndUploadF(1, 3))
      .then(M::putMetadata(1))
      .then(M::readPointer(1))
      .then(M::checkLease(1))
      .then(M::publishOk(1))          // retained [s2]
      .then(M::checkLease(1))
      .then(M::cleanupRead(1))        // refSet = {3}
      .then(M::checkLease(1))
      .then(M::sweepList(1))          // toDelete = {2}
      .then(M::checkLease(1))
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                  // now 20, node 1's lease exp 20
      .then(M::acquire(0))            // node 0 takes the lease back
      .then(M::exportWith(0, 1, 4))   // files {2}, lsn 2
      .then(M::headAndUploadF(0, 2))  // reuses ssts/2
      .then(M::putMetadata(0))
      .then(M::readPointer(0))
      .then(M::checkLease(0))
      .then(M::publishOk(0))          // retained [s3] references ssts/2
      .then(M::checkLease(1))         // node 1 revalidates: locally invalid now
      .then(M::fenceAbort(1))         // fenced refuses -> cleanup ends, nothing is deleted
      .then(M::checkpoint(and { M::I1, M::I3 }))

  // Fenced discard race: node 0 created ssts/2 and revalidated at now 0; node 1 takes the lease
  // over and publishes a snapshot that reused ssts/2; node 0's publish is rejected (etag moved),
  // so it would discard. Own objects of the unpublished snapshot are still deleted unfenced
  // (P:734-738), but the created shared SST goes through the fenced path and is refused
  // (L:274/L:476), so ssts/2 survives.
  run discardRaceIsRefusedByFencingTest =
    M::init
      .then(M::acquire(0))
      .then(M::exportWith(0, 1, 4))
      .then(M::headAndUploadF(0, 2))  // creates ssts/2
      .then(M::putMetadata(0))
      .then(M::readPointer(0))        // seenEtag 0, branch newer
      .then(M::checkLease(0))         // valid at t = 0
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)
      .then(M::tick)                  // now 10
      .then(M::acquire(1))            // takeover
      .then(M::exportWith(1, 1, 4))   // files {2}, lsn 1
      .then(M::headAndUploadF(1, 2))  // reuse, no mtime refresh
      .then(M::putMetadata(1))
      .then(M::readPointer(1))
      .then(M::checkLease(1))
      .then(M::publishOk(1))          // retained [s2], etag 0 -> 1
      .then(M::publishRejected(0))    // P:941-945: FailedPrecondition
      .then(M::checkLease(0))         // locally invalid by now
      .then(M::fenceAbort(0))         // shared-SST delete refused
      .then(M::checkpoint(and { M::I1, M::I3 }))
}
