// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Instant;

use enum_map::{Enum, EnumMap};
use metrics::{counter, gauge};

use restate_types::config::{
    SelfProposerCongestionControl, SelfProposerCongestionControlMode, SelfProposerFlow,
};
use restate_types::sharding::PartitionId;

use super::self_proposer_scheduler::SelfProposerSchedulerFlow;
use crate::metric_definitions::{
    PARTITION_LABEL, SELF_PROPOSER_CONGESTION_CONTROL_GATED, SELF_PROPOSER_CONGESTION_CONTROL_LIMIT,
};

/// Every class is held within congestion control's limit on records in flight, and some further:
/// - Latency flows may use the whole limit, and a share of it is kept free for them.
/// - Regular flows may use what the latency share leaves.
/// - Background flows may use up to their own share.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Enum)]
pub enum FlowClass {
    /// Flows that are latency sensitive, such as the network service. They're prioritized, so they
    /// don't wait behind the other flows during a Bifrost slowdown.
    Latency,
    /// Most flows.
    #[default]
    Regular,
    /// Work that can be deferred, such as the cleaner's.
    Background,
}

/// Sets the limit on records in flight.
enum Controller {
    Static { limit: usize },
}

impl Controller {
    fn limit(&self) -> usize {
        match self {
            Self::Static { limit } => *limit,
        }
    }
}

/// Splits the controller's limit into a share per class.
pub struct CongestionControl {
    controller: Controller,
    classes: EnumMap<SelfProposerSchedulerFlow, FlowClass>,
    latency_share: f32,
    background_share: f32,
    // The records each class may have in flight, as of the controller's current limit.
    shares: EnumMap<FlowClass, usize>,
    metrics: CongestionControlMetrics,
}

impl CongestionControl {
    /// Returns `None` if congestion control is disabled.
    pub fn new(opts: &SelfProposerCongestionControl, partition_id: PartitionId) -> Option<Self> {
        let controller = match opts.mode {
            SelfProposerCongestionControlMode::Disabled => return None,
            SelfProposerCongestionControlMode::Static { limit } => {
                Controller::Static { limit: limit.get() }
            }
        };
        let classes = EnumMap::from_fn(|flow| {
            let flow = SelfProposerFlow::from(flow);
            if opts.latency_flows.contains(&flow) {
                FlowClass::Latency
            } else if opts.background_flows.contains(&flow) {
                FlowClass::Background
            } else {
                FlowClass::Regular
            }
        });

        let mut cc = Self {
            controller,
            classes,
            latency_share: opts.latency_share,
            background_share: opts.background_share,
            shares: EnumMap::default(),
            metrics: CongestionControlMetrics::new(partition_id),
        };
        cc.update_shares();
        Some(cc)
    }

    pub fn classes(&self) -> EnumMap<SelfProposerSchedulerFlow, FlowClass> {
        self.classes
    }

    /// Whether `flow` may propose, given the records in flight per class and overall. Called for
    /// ready flows, to also account the time they're held back.
    pub fn admits(
        &mut self,
        flow: SelfProposerSchedulerFlow,
        class_inflight: &EnumMap<FlowClass, usize>,
        total_inflight: usize,
    ) -> bool {
        let class = self.classes[flow];
        let admitted =
            total_inflight < self.controller.limit() && class_inflight[class] < self.shares[class];
        self.metrics.on_gate_checked(flow, !admitted);
        admitted
    }

    /// Rechecks the flows that are held back, once records in flight dropped. Only that ends a
    /// flow's wait, so its gated time stops right then, even if the scheduler doesn't check it yet.
    pub fn on_inflight_dropped(
        &mut self,
        class_inflight: &EnumMap<FlowClass, usize>,
        total_inflight: usize,
    ) {
        for (flow, since) in self.metrics.gated_since {
            if since.is_some() {
                self.admits(flow, class_inflight, total_inflight);
            }
        }
    }

    /// Splits the controller's current limit into the class shares. A class with flows gets at
    /// least one record and one without gets none.
    fn update_shares(&mut self) {
        let limit = self.controller.limit();
        let share = |class: FlowClass, fraction: f32| {
            if self.classes.values().any(|c| *c == class) {
                ((limit as f64 * fraction as f64).round() as usize).max(1)
            } else {
                0
            }
        };
        let latency_reserved = share(FlowClass::Latency, self.latency_share);
        let background = share(FlowClass::Background, self.background_share);
        self.shares = EnumMap::from_fn(|class| match class {
            FlowClass::Latency => limit,
            FlowClass::Regular => limit.saturating_sub(latency_reserved).max(1),
            FlowClass::Background => background,
        });
        self.metrics.limit.set(limit as f64);
    }
}

struct CongestionControlMetrics {
    limit: metrics::Gauge,
    gated_micros: EnumMap<SelfProposerSchedulerFlow, metrics::Counter>,
    // Since when each flow has been held back, as of the last time it was checked.
    gated_since: EnumMap<SelfProposerSchedulerFlow, Option<Instant>>,
}

impl CongestionControlMetrics {
    fn new(partition_id: PartitionId) -> Self {
        let partition = partition_id.to_string();
        Self {
            limit: gauge!(SELF_PROPOSER_CONGESTION_CONTROL_LIMIT, PARTITION_LABEL => partition.clone()),
            gated_micros: EnumMap::from_fn(
                |flow: SelfProposerSchedulerFlow| counter!(SELF_PROPOSER_CONGESTION_CONTROL_GATED, PARTITION_LABEL => partition.clone(), "flow" => flow.as_str()),
            ),
            gated_since: EnumMap::default(),
        }
    }

    /// Accounts the time `flow` was held back since the last check.
    fn on_gate_checked(&mut self, flow: SelfProposerSchedulerFlow, gated: bool) {
        let since = &mut self.gated_since[flow];
        if !gated && since.is_none() {
            return;
        }
        let now = Instant::now();
        if let Some(since) = since.replace(now) {
            self.gated_micros[flow].increment(now.duration_since(since).as_micros() as u64);
        }
        if !gated {
            *since = None;
        }
    }
}

impl Drop for CongestionControlMetrics {
    fn drop(&mut self) {
        self.limit.set(0.0);
    }
}
