# Failure-detector observer-pause findings

The deterministic failure-detector harness records every production-state-machine event, including
the full elapsed interval count and local/published peer state. It models a local observer pause,
not peer failures caused by that pause.

The Tokio gossip interval uses `MissedTickBehavior::Skip`: once the observer resumes, it emits one
tick rather than replaying each missed timer arm. The failure detector must preserve the full
elapsed interval count for diagnostics, but the time during which its own observer did not run is
not evidence that a peer failed to gossip for the same number of rounds.

The logical-tick candidate therefore advances a local peer age by at most one observation interval
per resumed tick. It keeps healthy queued peers Alive after the observed 12, 20, 29, 30, 38, 39,
and 51 interval observer pauses. A genuinely silent peer still becomes Dead after eleven actual
resumed polls, which is the failure threshold plus one. Ordinary scheduled transport loss retains
the same eleven-tick bound.

## Not a production conclusion

`gossip_age` is wire-advertised and min-merged. A logical-tick observer can therefore advertise a
lower age for a genuinely dead peer than a healthy observer has locally. The deterministic
four-node replay distinguishes two effects:

- With A resuming every thirty intervals, healthy C makes its first B `Dead` transition before
  A's next message; A's lower age can then reanimate B to `Suspect`.
- With A resuming every five intervals, A's lower age postpones C's first B `Dead` transition
  from eleven ordinary C polls to A's seventh resume. This is the actual initial-detection-delay
  counterexample.

Consequently the logical-tick change must remain experimental rather than a wire-compatible
production fix.

The test-only wire-compatible dual-gate experiment keeps bulk wall-time `gossip_age` unchanged
and additionally requires eleven local observation polls since fresh evidence before a local
age-based death. Direct gossip resets that counter. For indirect gossip, an accepted lower peer
age must also reset it: otherwise a healthy peer observed only through another node can be killed
after its counter has saturated. The harness proves this indirect-health property and the
eleven-poll bound once the evidence is stale.

That rule relies on a compatibility invariant: every shipped node must retain bulk wall-time age
on the wire. That invariant is necessary but not sufficient. The selected failure-detector-loop
ordering ticks and sends before returning to the select loop to consume queued messages. Even so,
the deterministic replay finds a bulk-wire counterexample: A records the oldest low indirect age
in each paused mailbox batch, then its next bulk tick advertises an age still below C's current
age. Repeating a five-interval pause therefore indefinitely resets C's local counter for a dead
B. Consuming the mailbox before the initial send is an additional ordering control with the same
failure. This is the actual blocker for the simple dual gate.

Logical wire aging makes the same problem easier to trigger: a logical A repeatedly advertises a
low age and can indefinitely refresh a dual-gate C. This remains a separate proof that the
logical-tick candidate must never ship, but the bulk-wire replay means abandoning it is not enough
to make the simple dual gate safe.

The dual gate remains test-only until its production representation either closes this queued
indirect-evidence loophole or explicitly specifies an acceptable bound, and until existing-peer
behavior and terminal-connection interaction have a separately reviewed design.

## Observer-discontinuity grace candidate

The next test-only candidate leaves `gossip_age`, gossip messages, and merge behavior entirely
unchanged. On a local tick that observed more than one elapsed interval, it resets a local
`consecutive_normal_fd_polls` decision counter. Each subsequent ordinary local tick increments the
counter once. It suppresses only age-based `Dead` decisions until the counter exceeds the failure
threshold; no peer gossip can reset the counter.

This candidate is strictly observer-local and wire-compatible. The harness keeps healthy queued
peers Alive in both tick/message orders, detects a silent peer on the eleventh normal poll after
one or repeated local stalls, preserves startup pre-aging safety, and leaves ordinary transport
loss at eleven ticks. A healthy C, which never stalled, still detects B despite repeatedly stalled
and gossiping A. The candidate deliberately does not fix the pre-existing bulk-wire queued-age
delay at C; it only prevents A's own discontinuity from being treated as B's missed observations.

`is_gone()` bypasses this local age gate and remains a separate terminal-connection question.

The separate, deliberately unmodified risk is a terminal gossip connection: `is_gone()` is
independent of gossip age and can still transition a peer to Dead after a local observer gap. The
real-loop injected-stall characterization must determine whether a healthy gossip connection stays
recoverable across a long local pause before changing that rule.
