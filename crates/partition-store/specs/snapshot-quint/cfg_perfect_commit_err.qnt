// C2: PERFECT lease (atomic mutex), PublishCommitButError enabled, no crash.
// Calibration (SPEC.md): I4 and I1 must FAIL here, because put() discards the upload of a
// snapshot whose pointer write was already committed (P:936-947 + P:717-720).
module cfg_perfect_commit_err {
  import snapshot_store(
    N = 2, F = 4, NUM_RETAINED = 2, MODE = 0, MAX_SKEW = 0,
    BOUNDED = true, MAX_STALL = 1, ALLOW_COMMIT_ERR = true, ALLOW_CRASH = false, LAT = 1
  ) as M from "./snapshot_store"

  // The conditional put of latest.json returns an error although it landed. put_snapshot_inner
  // re-reads the pointer (P:992-1052); the re-read lists this snapshot, so it counts as published
  // and its files are kept; because it is still the newest, eviction cleanup runs as on success
  // (P:968-974). Nothing is discarded, so I1 and I4 must hold.
  run commitButErrorKeepsPublishedSnapshotTest =
    M::init
      .then(M::acquire(0))
      .then(M::exportWith(0, 1, 4))   // files {2}, lsn 1
      .then(M::headAndUploadF(0, 2))  // creates ssts/2 (creates_shared)
      .then(M::putMetadata(0))
      .then(M::readPointer(0))        // etag 0, branch newer
      .then(M::checkLease(0))         // P:933 pre-publish validity check
      .then(M::publishCommitButError(0)) // P:938-951: pointer committed, Err returned
      .then(M::resolveListed(0))      // P:1017-1038: re-read lists us -> keep files, cleanup
      .then(M::checkLease(0))
      .then(M::cleanupRead(0))        // refSet = {2}
      .then(M::checkLease(0))
      .then(M::sweepList(0))          // nothing is old enough to sweep
      .then(M::cleanupDone(0))
      .then(M::checkpoint(and { M::I1, M::I4, M::I5 }))

  // The re-read cannot reach the pointer: the uploaded files are kept for the orphan sweep
  // (P:1006-1016) and nothing is discarded.
  run commitButErrorUnreadablePointerKeepsFilesTest =
    M::init
      .then(M::acquire(0))
      .then(M::exportWith(0, 1, 4))
      .then(M::headAndUploadF(0, 2))
      .then(M::putMetadata(0))
      .then(M::readPointer(0))
      .then(M::checkLease(0))
      .then(M::publishCommitButError(0))
      .then(M::resolveReadFails(0))   // files kept, no discard
      .then(M::checkpoint(and { M::I1, M::I4 }))
}
