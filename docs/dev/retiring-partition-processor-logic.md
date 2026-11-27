# Retiring partition processor logic

A partition processor rebuilds its state by applying its log. A server can only drop logic that
applies log records, such as a command, a command variant or a state format, once no partition it
runs can still need that logic: neither in the state of its store, nor in the part of its log it
hasn't applied yet. This guide describes how to retire such logic safely. Journal v1 was the first
logic retired this way (`PartitionFeatureChange::RetireJournalV1`).

## The mechanism

A retirement is a partition feature, a `PartitionFeatureChange` in
`crates/types/src/partitions/features.rs`. Like every partition feature, it is enabled by a
`VersionBarrier` that the partition's leader proposes, and every replica applies it at the same log
position.

- **Enabling the feature migrates the data the logic acts on, if needed.** If the state machine
  needs the logic to act on data in the store, the feature's arm in
  `crates/worker/src/partition/processor/commands/version_barrier.rs` migrates or removes that
  data. The feature is only stored once that is done, so a migration that doesn't finish is
  continued when the barrier is applied again. A retirement that only stops using a command, or
  whose data is migrated transparently when it is read, needs no such migration; its data can be
  cleaned up later.
- **With the feature, the partition creates no new data that needs the logic.** The code paths
  that would create it check the feature and do something else.
- **A server that dropped the logic requires the feature.** It lists the feature in
  `REQUIRED_PARTITION_FEATURES`, and then:
  - refuses to start a partition whose store has applied a barrier but lacks the feature
    (`check_on_startup` in `crates/worker/src/partition/processor/required_features.rs`);
  - applies only `AnnounceLeader` and `VersionBarrier` records while the feature is missing
    (`CommandCheck`), so the barrier that enables it must come before any other command;
  - replaces a local store that lacks the feature with the newest snapshot that has it, as
    recorded in the snapshot's compatibility metadata, when it opens the store.

  A blocked partition stops with `ProcessorError::MissingRequiredFeatures`, raises the
  `missing_required_features` blocked flare and retries at maximum backoff. The flare clears once
  the partition has the feature.

## Rollout over three releases

1. **Release N adds the feature**, its migration and the guards against new data, behind an
   experimental option (`experimental-enable-<name>`). The feature's `min_required_version` is N,
   so a server older than N stops at the barrier instead of applying records it doesn't
   understand.
2. **Release N+1 enables the feature by default.**
3. **Release N+2 requires the feature and deletes the logic.** A partition that reaches N+2
   without the feature stays blocked until a snapshot has it, which takes running N+1 again.
   Its release notes tell operators to run `restatectl snapshots check-features <name>` before
   upgrading, which checks that the latest snapshot of every partition has the feature.

## Checklist for a new retirement

- Add the `PartitionFeatureChange` variant. The name that its `enabled_name` returns must never
  change: snapshots record it, and a server that requires the feature looks for it.
- If the state machine needs the logic to act on data in the store, implement the migration in the
  feature's arm of the version barrier. It must be deterministic: every replica applies it to the
  same state, at the same log position. If it commits in parts, it must be safe to run again after
  any of them. Discard the actions it produces: the leader applies the barrier before it starts
  leading.
- Run the migration after the arms it depends on. Arms run in the order the barrier lists them,
  and the leader proposes the feature after every other change that has a migration. Only changes
  that ride along (`PartitionFeatureChange::rides_along`) follow it, and those migrate nothing.
- Guard every code path that would create data needing the logic.
- Add the experimental option and the leader proposal.
- Test the migration, if there is one, on a store with data that needs the logic, including that
  applying it again changes nothing.
