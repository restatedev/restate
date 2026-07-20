// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cheap, lock-free liveness signal for the partition processor's `run_inner` loop.
//!
//! The processor stores its current loop phase and a monotonically increasing counter in a
//! single packed `AtomicU64`, written with `Release` ordering and read with `Acquire` ordering so
//! that a reader always observes a coherent `(phase, counter)` pair -- never a phase from one
//! write torn against the counter of another. The counter lets a reader detect "the phase
//! changed since I last looked" without needing to compare phases (which repeat).
//!
//! Timestamps are deliberately kept out of the atomic: `Instant` cannot be packed into a `u64`,
//! and the manager (not the processor) is the one that cares about elapsed time. The manager
//! samples `(phase, counter)` and stamps its own `Instant` locally whenever the counter changes.

use std::sync::atomic::{AtomicU64, Ordering};

const PHASE_BITS: u32 = 3;
const PHASE_MASK: u64 = (1 << PHASE_BITS) - 1;

/// Where the `run_inner` loop currently is. Written by the partition processor at well-defined
/// points in its select loop; read by the `PartitionProcessorManager`'s apply-progress tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LoopPhase {
    /// Parked in `tokio::select!`, waiting for the next event (including the record stream).
    /// This is the only phase whose staleness is a sound proof that the loop cannot run --
    /// every other branch of the select is guaranteed to fire the ~500ms(+/-50%) status timer at
    /// the latest, which always brings the loop back here.
    InSelect = 0,
    /// Applying a batch of records read from the log.
    ApplyingBatch = 1,
    /// Handling a target-leader-state change or running the leadership state machine.
    ControlOp = 2,
}

impl LoopPhase {
    fn from_bits(bits: u64) -> Self {
        match bits {
            0 => LoopPhase::InSelect,
            1 => LoopPhase::ApplyingBatch,
            2 => LoopPhase::ControlOp,
            other => panic!("invalid packed LoopPhase bits: {other}"),
        }
    }
}

/// Packed `[phase: 3 bits | counter: 61 bits]` heartbeat. Cheap enough to write twice per loop
/// iteration (relaxed load + released store); `sample()` is a single acquire load, so phase and
/// counter are always observed together.
#[derive(Debug, Default)]
pub struct LoopHeartbeat(AtomicU64);

impl LoopHeartbeat {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Record that the loop has reached `phase`. Monotonically increments the counter so
    /// repeated beats of the same phase are still distinguishable from a stale reading.
    pub fn beat(&self, phase: LoopPhase) {
        let counter = self.0.load(Ordering::Relaxed) >> PHASE_BITS;
        let packed = (counter.wrapping_add(1) << PHASE_BITS) | (phase as u64);
        self.0.store(packed, Ordering::Release);
    }

    /// A single coherent `(phase, counter)` snapshot.
    pub fn sample(&self) -> (LoopPhase, u64) {
        let packed = self.0.load(Ordering::Acquire);
        (
            LoopPhase::from_bits(packed & PHASE_MASK),
            packed >> PHASE_BITS,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_coherence_unit() {
        let hb = LoopHeartbeat::new();

        // Initial sample: counter 0, phase InSelect (the packed zero value).
        let (phase, counter) = hb.sample();
        assert_eq!(phase, LoopPhase::InSelect);
        assert_eq!(counter, 0);

        hb.beat(LoopPhase::ApplyingBatch);
        let (phase, counter1) = hb.sample();
        assert_eq!(phase, LoopPhase::ApplyingBatch);
        assert_eq!(counter1, 1);

        hb.beat(LoopPhase::ControlOp);
        let (phase, counter2) = hb.sample();
        assert_eq!(phase, LoopPhase::ControlOp);
        assert_eq!(counter2, 2);
        assert!(
            counter2 > counter1,
            "counter must be monotonically increasing"
        );

        // Repeated beats of the same phase still bump the counter, so a reader can tell a fresh
        // beat happened even though the phase read is identical to the last sample.
        hb.beat(LoopPhase::InSelect);
        hb.beat(LoopPhase::InSelect);
        let (phase, counter3) = hb.sample();
        assert_eq!(phase, LoopPhase::InSelect);
        assert_eq!(counter3, 4);
    }

    #[test]
    fn sample_never_tears_phase_and_counter() {
        // Regression guard for the packed-atomic design: since beat() does a single store and
        // sample() does a single load, there is no way to observe a phase from one beat combined
        // with a counter from another -- verified structurally (single AtomicU64), exercised here
        // by checking every phase transition keeps phase and counter in lock-step.
        let hb = LoopHeartbeat::new();
        let phases = [
            LoopPhase::ApplyingBatch,
            LoopPhase::ControlOp,
            LoopPhase::InSelect,
            LoopPhase::ApplyingBatch,
        ];
        for (i, phase) in phases.iter().enumerate() {
            hb.beat(*phase);
            let (sampled_phase, counter) = hb.sample();
            assert_eq!(sampled_phase, *phase);
            assert_eq!(counter, (i + 1) as u64);
        }
    }
}
