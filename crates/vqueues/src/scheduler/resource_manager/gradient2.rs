// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adaptive concurrency controller (Netflix Gradient2 style) for user
//! concurrency rules. One controller instance exists per adaptive rule.
//! The latency signal is the permit hold time (acquire to release);
//! it is suspension-filtered by construction, so journal-wait dwell
//! never pollutes the signal.
//!
//! The limit grows/shrinks by the ratio of a slow baseline to the current
//! short sample, clamped to `[min, max]`; growth is additive only and
//! never revokes issued permits.
//!
//! A CoDel-style sojourn backstop guards slow creeping degradation that
//! Gradient2 alone can miss: if the minimum acquire-side sojourn stays
//! above target for a full window while the limit sits at its ceiling,
//! increases freeze, and persistence trims the limit once per episode.

use std::num::NonZeroU32;
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram, counter, gauge, histogram};
use tokio::time::Instant;

use restate_limiter::AdaptiveConcurrency;

use crate::metric_definitions::{
    ADAPTIVE_BACKSTOP_TOTAL, ADAPTIVE_DRIFT_DECAY_TOTAL, ADAPTIVE_GRADIENT, ADAPTIVE_IN_FLIGHT,
    ADAPTIVE_LIMIT, ADAPTIVE_LONG_RTT_MS, ADAPTIVE_SAMPLES_TOTAL, ADAPTIVE_SHORT_RTT_MS,
    ADAPTIVE_SOJOURN_MIN_MS, ADAPTIVE_UPDATES_TOTAL, HOLD_TIME_SECONDS,
};

/// Default lower bound: below ~2-4 slots long-running handlers stop producing
/// a usable gradient (and throughput dies).
const DEFAULT_MIN: u32 = 4;
/// Default upper bound: practically open — the node-level invoker permit pool
/// is the real system ceiling. An explicit max (e.g. the previous static cap)
/// is recommended as a guardrail against the first-burst blind phase after
/// idle periods.
const DEFAULT_MAX: u32 = 10_000;
/// Default tolerance (permille): current sample may be 1.5x the baseline
/// before the limit shrinks.
const DEFAULT_TOLERANCE_PERMILLE: u32 = 1_500;
/// Default smoothing (permille): bounds shrink to ~10%/update.
const DEFAULT_SMOOTHING_PERMILLE: u32 = 200;
/// Additive growth headroom per update (Netflix queueSize constant).
const QUEUE_SIZE: f64 = 4.0;
/// Minimum interval between limit updates (Temporal ramp-throttle lesson).
const MIN_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// Time constant of the long (baseline) EMA.
const LONG_EMA_TIME_CONSTANT: Duration = Duration::from_secs(120);
/// Number of warm-up samples averaged arithmetically before the EMA takes over.
const WARMUP_SAMPLES: u32 = 10;
/// Sojourn backstop: target acquire-side wait and evaluation window.
const BACKSTOP_SOJOURN_TARGET: Duration = Duration::from_secs(5);
const BACKSTOP_WINDOW: Duration = Duration::from_secs(5);
/// Consecutive over-target windows before trimming (after freezing).
const BACKSTOP_TRIM_WINDOWS: u32 = 2;

/// Outcome of feeding one sample; used for metrics and by the caller to
/// decide whether waiters need waking (limit increased).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    Increase,
    Decrease,
    ClampMin,
    ClampMax,
    AppLimited,
    IntervalSkip,
}

pub struct Gradient2Controller {
    // -- configuration ------------------------------------------------------
    /// The raw rule config; kept so identical re-upserts (helm redeploys)
    /// preserve the learned state instead of resetting it.
    cfg: AdaptiveConcurrency,
    min: f64,
    max: f64,
    tolerance: f64,
    smoothing: f64,
    // -- pre-resolved metric handles (labels are fixed per controller; this
    // -- avoids per-completion String clones on the hot path) ---------------
    m_samples: Counter,
    m_hold_time: Histogram,
    m_in_flight: Gauge,
    m_limit: Gauge,
    m_long_rtt: Gauge,
    m_short_rtt: Gauge,
    m_gradient: Gauge,
    m_sojourn_min: Gauge,
    m_drift_decay: Counter,
    m_backstop_freeze: Counter,
    m_backstop_trim: Counter,
    m_out_increase: Counter,
    m_out_decrease: Counter,
    m_out_clamp_min: Counter,
    m_out_clamp_max: Counter,
    m_out_app_limited: Counter,

    // -- controller state ---------------------------------------------------
    limit: f64,
    long_ema: f64,
    warmup_count: u32,
    warmup_sum: f64,
    last_update: Instant,
    last_sample: Instant,

    // -- sojourn backstop ---------------------------------------------------
    window_start: Instant,
    window_min_sojourn: Option<Duration>,
    over_target_windows: u32,
    freeze_increases: bool,
    trimmed_this_episode: bool,
}

impl Gradient2Controller {
    pub fn from_config(
        cfg: &AdaptiveConcurrency,
        pattern_label: String,
        partition_label: String,
        now: Instant,
    ) -> Self {
        let min = cfg.min.map(NonZeroU32::get).unwrap_or(DEFAULT_MIN) as f64;
        let max = cfg
            .max
            .map(NonZeroU32::get)
            .unwrap_or(DEFAULT_MAX)
            .max(cfg.min.map(NonZeroU32::get).unwrap_or(DEFAULT_MIN)) as f64;
        let tolerance = cfg
            .tolerance_permille
            .map(NonZeroU32::get)
            .unwrap_or(DEFAULT_TOLERANCE_PERMILLE) as f64
            / 1000.0;
        let smoothing = (cfg
            .smoothing_permille
            .map(NonZeroU32::get)
            .unwrap_or(DEFAULT_SMOOTHING_PERMILLE) as f64
            / 1000.0)
            .clamp(0.01, 1.0);
        // Cold start: with an explicit max, a quarter into the corridor;
        // without, a small multiple of min.
        let initial = if cfg.max.is_some() {
            min + (max - min) / 4.0
        } else {
            (min * 8.0).min(max)
        };
        let outcome_counter = |o: &'static str| counter!(ADAPTIVE_UPDATES_TOTAL, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone(), "outcome" => o);
        let controller = Self {
            cfg: cfg.clone(),
            min,
            max,
            tolerance,
            smoothing,
            m_samples: counter!(ADAPTIVE_SAMPLES_TOTAL, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_hold_time: histogram!(HOLD_TIME_SECONDS, "pattern" => pattern_label.clone()),
            m_in_flight: gauge!(ADAPTIVE_IN_FLIGHT, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_limit: gauge!(ADAPTIVE_LIMIT, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_long_rtt: gauge!(ADAPTIVE_LONG_RTT_MS, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_short_rtt: gauge!(ADAPTIVE_SHORT_RTT_MS, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_gradient: gauge!(ADAPTIVE_GRADIENT, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_sojourn_min: gauge!(ADAPTIVE_SOJOURN_MIN_MS, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_drift_decay: counter!(ADAPTIVE_DRIFT_DECAY_TOTAL, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone()),
            m_backstop_freeze: counter!(ADAPTIVE_BACKSTOP_TOTAL, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone(), "action" => "freeze"),
            m_backstop_trim: counter!(ADAPTIVE_BACKSTOP_TOTAL, "pattern" => pattern_label.clone(), "partition_id" => partition_label.clone(), "action" => "trim"),
            m_out_increase: outcome_counter("increase"),
            m_out_decrease: outcome_counter("decrease"),
            m_out_clamp_min: outcome_counter("clamp_min"),
            m_out_clamp_max: outcome_counter("clamp_max"),
            m_out_app_limited: outcome_counter("app_limited"),
            limit: initial,
            long_ema: 0.0,
            warmup_count: 0,
            warmup_sum: 0.0,
            last_update: now,
            last_sample: now,
            window_start: now,
            window_min_sojourn: None,
            over_target_windows: 0,
            freeze_increases: false,
            trimmed_this_episode: false,
        };
        controller.emit_state_gauges(1.0);
        controller
    }

    /// True when the given config equals this controller's — the caller keeps
    /// the learned state on identical re-upserts.
    pub fn config_matches(&self, cfg: &AdaptiveConcurrency) -> bool {
        self.cfg == *cfg
    }

    pub fn current_limit(&self) -> NonZeroU32 {
        // limit is clamped to [min>=1, ..]; the fallback can't trigger but
        // keeps the conversion total.
        NonZeroU32::new(self.limit as u32).unwrap_or(NonZeroU32::MIN)
    }

    /// Feeds one permit-hold-time sample. Returns the update outcome; on
    /// [`UpdateOutcome::Increase`] the caller should wake waiters blocked on
    /// this rule.
    pub fn on_sample(&mut self, hold: Duration, in_flight: u32, now: Instant) -> UpdateOutcome {
        self.m_samples.increment(1);
        self.m_hold_time.record(hold.as_secs_f64());

        // Guard degenerate samples: a zero-duration hold carries no gradient
        // information; floor at 100µs to keep the ratio finite.
        let sample_ms = (hold.as_secs_f64() * 1000.0).max(0.1);
        self.last_sample = now;

        // Baseline: arithmetic warm-up, then a time-constant EMA (rate
        // independent — sample rates differ wildly across rules).
        if self.warmup_count < WARMUP_SAMPLES {
            self.warmup_count += 1;
            self.warmup_sum += sample_ms;
            self.long_ema = self.warmup_sum / self.warmup_count as f64;
        } else {
            let dt = now
                .saturating_duration_since(self.last_update)
                .as_secs_f64()
                .max(0.001);
            let alpha = 1.0 - (-dt / LONG_EMA_TIME_CONSTANT.as_secs_f64()).exp();
            self.long_ema += alpha * (sample_ms - self.long_ema);
        }

        // Baseline drift decay: after a load spike ends, pull an inflated
        // baseline back down.
        if self.long_ema / sample_ms > 2.0 {
            self.long_ema *= 0.95;
            self.m_drift_decay.increment(1);
        }

        // Update gate: the effect of the previous update must become
        // measurable before the next one. Hot path: most samples land here.
        if now.saturating_duration_since(self.last_update) < MIN_UPDATE_INTERVAL {
            return UpdateOutcome::IntervalSkip;
        }
        self.last_update = now;
        self.roll_backstop_window(now);
        self.m_in_flight.set(in_flight as f64);

        // App-limited guard: no growth without real demand.
        if (in_flight as f64) < self.limit / 2.0 {
            return self.finish_update(UpdateOutcome::AppLimited, sample_ms, 1.0);
        }

        let gradient = (self.tolerance * self.long_ema / sample_ms).clamp(0.5, 1.0);
        let mut new_limit = self.limit * gradient + QUEUE_SIZE;
        new_limit = self.limit * (1.0 - self.smoothing) + new_limit * self.smoothing;

        if self.freeze_increases && new_limit > self.limit {
            new_limit = self.limit;
        }

        let outcome = if new_limit <= self.min {
            new_limit = self.min;
            UpdateOutcome::ClampMin
        } else if new_limit >= self.max {
            new_limit = self.max;
            UpdateOutcome::ClampMax
        } else if new_limit > self.limit {
            UpdateOutcome::Increase
        } else {
            UpdateOutcome::Decrease
        };
        self.limit = new_limit;
        self.finish_update(outcome, sample_ms, gradient)
    }

    /// Feeds one acquire-side sojourn observation (time the head entry spent
    /// runnable in the durable queue before admission was attempted). Drives
    /// the CoDel-style backstop.
    pub fn on_sojourn(&mut self, sojourn: Duration, now: Instant) {
        self.window_min_sojourn = Some(match self.window_min_sojourn {
            Some(current) => current.min(sojourn),
            None => sojourn,
        });
        self.roll_backstop_window(now);
    }

    fn roll_backstop_window(&mut self, now: Instant) {
        if now.saturating_duration_since(self.window_start) < BACKSTOP_WINDOW {
            return;
        }
        let min_sojourn = self.window_min_sojourn.take();
        self.window_start = now;
        self.m_sojourn_min
            .set(min_sojourn.map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0));

        let at_ceiling = self.limit >= 0.95 * self.max;
        let over_target = matches!(min_sojourn, Some(s) if s > BACKSTOP_SOJOURN_TARGET);
        if over_target && at_ceiling {
            self.over_target_windows += 1;
            if !self.freeze_increases {
                self.freeze_increases = true;
                self.m_backstop_freeze.increment(1);
            }
            if self.over_target_windows >= BACKSTOP_TRIM_WINDOWS && !self.trimmed_this_episode {
                self.limit = (self.limit * 0.9).max(self.min);
                self.trimmed_this_episode = true;
                self.m_backstop_trim.increment(1);
            }
        } else if matches!(min_sojourn, Some(s) if s <= BACKSTOP_SOJOURN_TARGET) {
            // Observed sojourn back under target: episode over. Over-target
            // windows below the ceiling (e.g. right after a trim) keep the
            // episode state — only genuine recovery unfreezes.
            self.over_target_windows = 0;
            self.freeze_increases = false;
            self.trimmed_this_episode = false;
        }
    }

    fn finish_update(
        &self,
        outcome: UpdateOutcome,
        sample_ms: f64,
        gradient: f64,
    ) -> UpdateOutcome {
        match outcome {
            UpdateOutcome::Increase => self.m_out_increase.increment(1),
            UpdateOutcome::Decrease => self.m_out_decrease.increment(1),
            UpdateOutcome::ClampMin => self.m_out_clamp_min.increment(1),
            UpdateOutcome::ClampMax => self.m_out_clamp_max.increment(1),
            UpdateOutcome::AppLimited => self.m_out_app_limited.increment(1),
            UpdateOutcome::IntervalSkip => {}
        }
        self.m_short_rtt.set(sample_ms);
        self.emit_state_gauges(gradient);
        outcome
    }

    fn emit_state_gauges(&self, gradient: f64) {
        self.m_limit.set(self.limit);
        self.m_long_rtt.set(self.long_ema);
        self.m_gradient.set(gradient);
    }

    /// Zeroes all state gauges. Called when the controller is dropped (rule
    /// removed or overridden by a static concurrency) so dashboards show "no
    /// adaptive controller" instead of the last learned values forever.
    pub fn zero_gauges(&self) {
        self.m_limit.set(0.0);
        self.m_in_flight.set(0.0);
        self.m_gradient.set(0.0);
        self.m_short_rtt.set(0.0);
        self.m_long_rtt.set(0.0);
        self.m_sojourn_min.set(0.0);
    }

    #[cfg(test)]
    pub fn limit_f64(&self) -> f64 {
        self.limit
    }

    #[cfg(test)]
    pub fn long_ema(&self) -> f64 {
        self.long_ema
    }

    #[cfg(test)]
    pub fn is_frozen(&self) -> bool {
        self.freeze_increases
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AdaptiveConcurrency {
        AdaptiveConcurrency {
            min: NonZeroU32::new(4),
            max: NonZeroU32::new(300),
            tolerance_permille: None,
            smoothing_permille: None,
        }
    }

    fn controller(now: Instant) -> Gradient2Controller {
        Gradient2Controller::from_config(&cfg(), "payment".into(), "0".into(), now)
    }

    fn ms(v: u64) -> Duration {
        Duration::from_millis(v)
    }

    /// Feeds one sample per second (past the update gate) with the given
    /// latency; in_flight tracks the limit so the app-limited guard stays off.
    fn drive(c: &mut Gradient2Controller, now: &mut Instant, latency: Duration, n: usize) {
        for _ in 0..n {
            *now += Duration::from_millis(1050);
            let in_flight = c.current_limit().get();
            c.on_sample(latency, in_flight, *now);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cold_start_within_corridor() {
        let now = Instant::now();
        let c = controller(now);
        // corridor start: min + (max-min)/4 = 4 + 74 = 78
        assert_eq!(c.current_limit().get(), 78);

        // without max: min*8
        let c2 = Gradient2Controller::from_config(
            &AdaptiveConcurrency::default(),
            "p".into(),
            "0".into(),
            now,
        );
        assert_eq!(c2.current_limit().get(), 32);
    }

    #[tokio::test(start_paused = true)]
    async fn healthy_latency_grows_additively_and_clamps_at_max() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 400);
        // stable latency -> gradient clamps at 1.0 -> additive growth to max
        assert_eq!(c.current_limit().get(), 300);
    }

    #[tokio::test(start_paused = true)]
    async fn latency_spike_shrinks_bounded_per_update() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 100);
        let before = c.limit_f64();
        // 4x usual latency -> gradient floor 0.5; shrink is smoothing-bounded
        now += Duration::from_millis(1050);
        c.on_sample(ms(400), c.current_limit().get(), now);
        let after = c.limit_f64();
        assert!(after < before);
        assert!(after > before * 0.89, "shrink must be <= ~10%/update");
    }

    #[tokio::test(start_paused = true)]
    async fn converges_down_under_sustained_congestion() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 100);
        // congestion ONSET: 5x latency vs baseline -> gradient floors, limit
        // falls fast, well below the healthy level
        drive(&mut c, &mut now, ms(500), 20);
        assert!(c.limit_f64() < 100.0, "limit was {}", c.limit_f64());
        // SUSTAINED congestion: the 120s-EMA baseline adapts to the new normal
        // (by design — G2 reacts to latency *changes*, not absolute levels),
        // so the limit partially recovers but stays below the ceiling
        drive(&mut c, &mut now, ms(500), 180);
        assert!(c.limit_f64() < 300.0, "limit was {}", c.limit_f64());
        // and recovers fully once latency normalizes (drift decay pulls the
        // inflated baseline back down)
        drive(&mut c, &mut now, ms(100), 400);
        assert_eq!(c.current_limit().get(), 300);
    }

    #[tokio::test(start_paused = true)]
    async fn limit_always_within_bounds_under_random_latency() {
        let mut now = Instant::now();
        let mut c = controller(now);
        // deterministic pseudo-random latencies (LCG), 100µs..8s
        let mut seed: u64 = 0x5eed;
        for _ in 0..5_000 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let latency_us = 100 + (seed >> 33) % 8_000_000;
            now += Duration::from_millis(300);
            let in_flight = ((seed >> 7) % 400) as u32;
            c.on_sample(Duration::from_micros(latency_us), in_flight, now);
            let l = c.limit_f64();
            assert!((4.0..=300.0).contains(&l), "limit out of bounds: {l}");
            assert!(l.is_finite());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn app_limited_blocks_growth() {
        let mut now = Instant::now();
        let mut c = controller(now);
        let before = c.current_limit().get();
        for _ in 0..50 {
            now += Duration::from_millis(1050);
            // healthy latency but almost no in-flight demand
            c.on_sample(ms(100), 1, now);
        }
        assert_eq!(c.current_limit().get(), before, "app-limited must not grow");
    }

    #[tokio::test(start_paused = true)]
    async fn interval_gate_skips_fast_samples() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 20);
        let before = c.limit_f64();
        // 100 samples within one second: all gated, limit unchanged
        for _ in 0..100 {
            now += Duration::from_millis(5);
            let out = c.on_sample(ms(100), c.current_limit().get(), now);
            assert_eq!(out, UpdateOutcome::IntervalSkip);
        }
        assert_eq!(c.limit_f64(), before);
    }

    #[tokio::test(start_paused = true)]
    async fn drift_decay_pulls_baseline_down() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(800), 50);
        let inflated = c.long_ema();
        // latency collapses to a fifth: decay activates (ratio > 2)
        drive(&mut c, &mut now, ms(100), 30);
        assert!(c.long_ema() < inflated / 2.0);
    }

    #[tokio::test(start_paused = true)]
    async fn degenerate_samples_do_not_poison() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, Duration::ZERO, 30);
        assert!(c.limit_f64().is_finite());
        drive(&mut c, &mut now, Duration::from_secs(3600), 5);
        assert!(c.limit_f64().is_finite());
        assert!((4.0..=300.0).contains(&c.limit_f64()));
    }

    #[tokio::test(start_paused = true)]
    async fn backstop_freezes_then_trims_once_per_episode() {
        let mut now = Instant::now();
        let mut c = controller(now);
        drive(&mut c, &mut now, ms(100), 400);
        assert_eq!(c.current_limit().get(), 300); // at ceiling

        // sustained high sojourn: freeze after first window, trim after second
        for _ in 0..4 {
            now += Duration::from_millis(5100);
            c.on_sojourn(Duration::from_secs(8), now);
        }
        assert!(c.is_frozen());
        let trimmed = c.limit_f64();
        assert!(trimmed <= 300.0 * 0.9 + 1.0);
        // more over-target windows: no second trim within the episode
        for _ in 0..4 {
            now += Duration::from_millis(5100);
            c.on_sojourn(Duration::from_secs(8), now);
        }
        assert!((c.limit_f64() - trimmed).abs() < 1.0);

        // recovery ends the episode
        now += Duration::from_millis(5100);
        c.on_sojourn(ms(10), now);
        now += Duration::from_millis(5100);
        c.on_sojourn(ms(10), now);
        assert!(!c.is_frozen());
    }
}
