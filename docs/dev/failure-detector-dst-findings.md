# Failure-detector observer-pause findings

The deterministic failure-detector harness records production state-machine events, including the
full elapsed interval count and local/published peer state. It models a local observer pause, not
a peer failure caused by that pause.

The Tokio gossip interval uses `MissedTickBehavior::Skip`: a resumed observer sees one tick rather
than every missed timer arm. The full elapsed count remains useful diagnostics, but a local pause
is not evidence that peers missed the same gossip rounds.

## Logical wire tick is not a production candidate

`gossip_age` is wire-advertised and min-merged. A logical-tick observer can advertise a lower age
for a genuinely dead peer than a healthy observer has locally. The four-node replay distinguishes:

- With A resuming every thirty intervals, C makes its first B `Dead` transition before A's next
  message; A's lower age can then reanimate B to `Suspect`.
- With A resuming every five intervals, A's lower age postpones C's first B `Dead` transition from
  eleven ordinary C polls to A's seventh resume.

The logical-wire experiment must not ship or be used in a rolling upgrade.

## Freshness dual gate is not sufficient

The test-only dual gate retained bulk wire age but required eleven local polls since direct or
accepted lower indirect evidence. Resetting on lower indirect evidence is necessary to avoid a
false death of a healthy indirectly observed peer. It is nevertheless insufficient: a paused A
can replay the oldest low age from each queued C-to-A mailbox batch, repeatedly lowering C's age
and resetting C's local counter for a dead B. This happens with bulk wire age and the selected
tick/send/then-receive loop ordering.

## Observer-discontinuity grace candidate

The remaining test-only candidate leaves `gossip_age`, gossip messages, and merge behavior
unchanged. A tick with more than one elapsed interval resets an observer-local normal-poll counter;
an ordinary one-interval tick increments it once; a zero-interval tick does neither. Until that
counter exceeds the existing failure threshold, the harness holds only an age-caused worsening
transition. It preserves the true age and current state, and does not permit a recovery without
real gossip lowering the age. Terminal connection and failover causes remain ungated.

The harness shows healthy queued peers, startup pre-aging, one/repeated stalls, and ordinary
transport loss have the intended local behavior; a silent peer is detected on the eleventh normal
poll after a stall. Dead decisions under grace are a subset of baseline decisions, no-discontinuity
transitions are identical, and canonicalized outgoing gossip payloads are byte-identical. A
terminal `is_gone()` still bypasses grace. The grace mechanism is strictly observer-local and
wire-compatible, but deliberately does not repair stale queued-age propagation at other observers.

No production FD behavior is proposed by these experiments.
