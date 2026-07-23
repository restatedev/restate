# Release Notes: Automatic detection and recovery of stalled partition processors

## Behavioral Change / New Feature

### What Changed
A partition processor whose apply loop is stalled -- the log has records past the last applied
LSN, but the processor is neither applying them nor doing other legitimate work -- previously kept
reporting `replay_status = active` indefinitely. Recovering it required an operator to notice and
manually restart the node or run `restatectl partition leader pin` to move leadership elsewhere.

Each worker's partition processor manager now runs an apply-progress tracker per partition. It
combines a lightweight heartbeat from the partition processor's own event loop with periodic tail
observations (including a new low-rate, authoritative `ConsistentRead` check that can't be fooled
by a stale cached tail) to distinguish a genuinely stalled processor from one that is idle, busy
applying a large batch, or doing legitimate long-running work such as leader initialization.

When a stall is confirmed:
- The reported replay status is downgraded from `Active` to `CatchingUp`, and a new
  `apply_stalled_since` timestamp is attached to the partition's status, visible via
  `restatectl partition list` and the `partition_state` introspection table.
- If the stall persists past a grace period, the processor cooperatively stops itself and is
  restarted by the manager's existing supervision -- no new partition processor is ever started
  alongside a still-running one, and the node never force-exits the process.
- The quarantine signal is sticky: it survives the Starting/Stopping states around the restart and
  is only cleared once the worker observes real progress past the LSN recorded at quarantine time
  (or, for a loop that had stopped scheduling entirely rather than lagging, once its heartbeat is
  fresh again).
- The cluster controller's leader-selection scheduler treats a quarantined node as ineligible for
  leadership. If every alive replica of a partition is quarantined, the controller stops trying to
  reassign that partition's leader (`target_leader` is cleared) rather than repeatedly re-electing
  a node that cannot make progress; a `restatectl partition leader pin`/freeze remains available for
  manual override.

A healthy but idle partition processor -- caught up with the log and simply waiting for new work --
is never touched by this detection.

A second, faster-acting watchdog covers a leader-elect specifically: if a candidate's own
`AnnounceLeader` marker commits to the log but is never applied within
`candidate-activation-timeout` (default 30s from commit, extended for as long as the candidate
keeps making apply progress on other backlog), it self-bails immediately rather than waiting on the
slower general detector. This is quarantined the same way -- the committed-but-unapplied marker is
itself authoritative evidence of a stall, so the partition is marked `apply_stalled_since` and
excluded from leader candidacy until real progress is observed.

### Why This Matters
Previously, a wedged partition processor could silently report itself as healthy while making no
progress, hiding the problem from monitoring and requiring manual intervention to recover. This
closes that gap: detection and recovery now happen automatically, and the unhealthy state is
visible in the reported status the whole time it persists.

### Impact on Users
- New `worker.stall-detection` configuration section (enabled by default) controls the grace
  periods, probe timeouts, and backoff used by the detector; see the configuration reference for
  the full list of keys.
- New metrics: `restate.partition.apply_stalled`, `restate.partition.apply_phase_stuck`, and
  `restate.partition.stop_stuck`.
- New `apply_stalled_since` field on partition processor status, surfaced through
  `restatectl partition list` and the `partition_state` datafusion table.
- Partition processors may now restart automatically in scenarios that previously required manual
  intervention; this is expected behavior and is logged and reflected in the metrics above.

### Migration Guidance
No action required. Operators who want to disable the new detection can set
`worker.stall-detection.enabled = false`.

### Related Issues
- Defect B: partition processor apply-stall detection and safe self-bail.
