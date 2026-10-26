// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use serde::{Deserialize, Serialize};

/// # Self-proposer congestion control
///
/// Congestion control limits the records a partition leader keeps in flight. The `mode` key
/// selects how that limit is set, e.g. a fixed one:
///
/// ```toml
/// [worker.self-proposer-congestion-control]
/// mode = "static"
/// limit = 1000
/// ```
///
/// Whatever its mode, the limit is shared by three classes of flows:
/// - Latency flows may use the whole limit, and a share of it is kept free for them.
/// - Regular flows, the ones in neither list, and background flows together may use what the
///   latency share leaves.
/// - Background flows may use up to their share of the limit.
///
/// ```toml
/// latency-share = 0.2
/// background-share = 0.1
/// latency-flows = ["network-service"]
/// background-flows = ["cleaner"]
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    rename_all = "kebab-case",
    try_from = "SelfProposerCongestionControlShadow"
)]
pub struct SelfProposerCongestionControl {
    #[serde(flatten)]
    pub mode: SelfProposerCongestionControlMode,
    /// # Latency share
    ///
    /// The fraction of the limit kept free for the latency flows: the other flows may only use
    /// what it leaves. Ignored when there are no latency flows.
    ///
    /// Default: 0.2
    ///
    /// Since v1.8.2
    pub latency_share: f32,
    /// # Background share
    ///
    /// The largest fraction of the limit the background flows may use.
    ///
    /// Default: 0.1
    ///
    /// Since v1.8.2
    pub background_share: f32,
    /// # Latency flows
    ///
    /// Flows that are latency sensitive, and that the self-proposer scheduler prioritizes.
    ///
    /// Default: `["network-service"]`
    ///
    /// Since v1.8.2
    pub latency_flows: Vec<SelfProposerFlow>,
    /// # Background flows
    ///
    /// Flows whose work can be deferred, limited to the background share.
    ///
    /// Default: `["cleaner"]`
    ///
    /// Since v1.8.2
    pub background_flows: Vec<SelfProposerFlow>,
}

impl Default for SelfProposerCongestionControl {
    fn default() -> Self {
        Self {
            mode: SelfProposerCongestionControlMode::default(),
            latency_share: 0.2,
            background_share: 0.1,
            latency_flows: vec![SelfProposerFlow::NetworkService],
            background_flows: vec![SelfProposerFlow::Cleaner],
        }
    }
}

impl SelfProposerCongestionControl {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SelfProposerCongestionControlShadow {
    #[serde(flatten)]
    mode: SelfProposerCongestionControlMode,
    #[serde(default = "default_latency_share")]
    latency_share: f32,
    #[serde(default = "default_background_share")]
    background_share: f32,
    #[serde(default = "default_latency_flows")]
    latency_flows: Vec<SelfProposerFlow>,
    #[serde(default = "default_background_flows")]
    background_flows: Vec<SelfProposerFlow>,
}

fn default_latency_share() -> f32 {
    SelfProposerCongestionControl::default().latency_share
}

fn default_background_share() -> f32 {
    SelfProposerCongestionControl::default().background_share
}

fn default_latency_flows() -> Vec<SelfProposerFlow> {
    SelfProposerCongestionControl::default().latency_flows
}

fn default_background_flows() -> Vec<SelfProposerFlow> {
    SelfProposerCongestionControl::default().background_flows
}

impl TryFrom<SelfProposerCongestionControlShadow> for SelfProposerCongestionControl {
    type Error = String;

    fn try_from(shadow: SelfProposerCongestionControlShadow) -> Result<Self, Self::Error> {
        let SelfProposerCongestionControlShadow {
            mode,
            latency_share,
            background_share,
            latency_flows,
            background_flows,
        } = shadow;
        if !(0.0..1.0).contains(&latency_share) {
            return Err(format!(
                "latency-share ({latency_share}) must be at least 0 and less than 1"
            ));
        }
        if !(0.0..=1.0).contains(&background_share) {
            return Err(format!(
                "background-share ({background_share}) must be between 0 and 1"
            ));
        }
        if let Some(flow) = latency_flows
            .iter()
            .find(|flow| background_flows.contains(flow))
        {
            return Err(format!(
                "flow '{flow}' is in both latency-flows and background-flows"
            ));
        }
        Ok(Self {
            mode,
            latency_share,
            background_share,
            latency_flows,
            background_flows,
        })
    }
}

/// How the self-proposer congestion control limits the records in flight.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum SelfProposerCongestionControlMode {
    /// No limit on the number of in-flight records.
    #[default]
    Disabled,
    /// A fixed limit on the number of in-flight records.
    #[serde(rename_all = "kebab-case")]
    Static { limit: NonZeroUsize },
}

/// A source of the records a partition leader proposes to its own log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SelfProposerFlow {
    #[display("invoker")]
    Invoker,
    #[display("timer")]
    Timer,
    #[display("shuffle")]
    Shuffle,
    #[display("cleaner")]
    Cleaner,
    #[display("upsert-schema")]
    UpsertSchema,
    #[display("upsert-rule-book")]
    UpsertRuleBook,
    #[display("network-service")]
    NetworkService,
    #[display("partition-maintenance")]
    PartitionMaintenance,
    #[display("scheduler")]
    Scheduler,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkerOptions;

    #[test]
    fn self_proposer_congestion_control_config() {
        // Disabled by default and omitted when serialized.
        let options = WorkerOptions::default();
        assert!(options.self_proposer_congestion_control.is_default());
        let defaults = serde_json::to_value(options).unwrap();
        assert!(defaults.get("self-proposer-congestion-control").is_none());

        let parse = |cc: serde_json::Value| {
            let mut value = defaults.clone();
            value["self-proposer-congestion-control"] = cc;
            serde_json::from_value::<WorkerOptions>(value)
                .map(|options| options.self_proposer_congestion_control)
        };

        // Unset class options keep their defaults.
        let cc = parse(serde_json::json!({"mode": "static", "limit": 500})).unwrap();
        assert_eq!(
            cc.mode,
            SelfProposerCongestionControlMode::Static {
                limit: NonZeroUsize::new(500).unwrap()
            }
        );
        assert_eq!(cc.latency_flows, vec![SelfProposerFlow::NetworkService]);
        assert_eq!(cc.background_flows, vec![SelfProposerFlow::Cleaner]);

        // Custom classes survive a round trip.
        let cc = parse(serde_json::json!({
            "mode": "static",
            "limit": 500,
            "latency-share": 0.3,
            "latency-flows": ["network-service", "invoker"],
            "background-flows": [],
        }))
        .unwrap();
        assert_eq!(cc.latency_share, 0.3);
        assert_eq!(cc.background_share, 0.1);
        assert_eq!(
            cc.latency_flows,
            vec![SelfProposerFlow::NetworkService, SelfProposerFlow::Invoker]
        );
        assert!(cc.background_flows.is_empty());
        assert_eq!(parse(serde_json::to_value(&cc).unwrap()).unwrap(), cc);

        // A flow can't be in two classes, and the latency share must leave room for the rest.
        assert!(
            parse(serde_json::json!({"mode": "disabled", "latency-flows": ["cleaner"]})).is_err()
        );
        assert!(parse(serde_json::json!({"mode": "disabled", "latency-share": 1.0})).is_err());
    }
}
