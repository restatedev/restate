// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Exports the per-role statuses of [`Health`] as gauges, so they can be alerted on.
//!
//! Each status is a gauge broken down by the label "status", one series per status value.
//! The series for the current status is 1, all others are 0.

use metrics::{describe_gauge, gauge};

use restate_core::{ShutdownError, TaskCenter, TaskKind, cancellation_token};
use restate_types::config::Configuration;
use restate_types::health::{
    AdminStatus, Health, HealthStatus, IngressStatus, LogServerStatus, MetadataServerStatus,
    NodeStatus, WorkerStatus,
};
use restate_types::nodes_config::Role;

trait StatusMetric: Copy + PartialEq + Send + Sync + 'static {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;
    /// All values, in the order of their protobuf discriminants.
    const ALL: &'static [Self];

    fn label(self) -> &'static str;
}

impl StatusMetric for NodeStatus {
    const NAME: &'static str = "restate.node.status";
    const DESCRIPTION: &'static str = "Node status, 1 for the current status and 0 for all others";
    const ALL: &'static [Self] = &[
        Self::Unknown,
        Self::Alive,
        Self::StartingUp,
        Self::ShuttingDown,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Alive => "alive",
            Self::StartingUp => "starting_up",
            Self::ShuttingDown => "shutting_down",
        }
    }
}

impl StatusMetric for WorkerStatus {
    const NAME: &'static str = "restate.worker.status";
    const DESCRIPTION: &'static str =
        "Worker role status, 1 for the current status and 0 for all others";
    const ALL: &'static [Self] = &[Self::Unknown, Self::Ready, Self::StartingUp];

    fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Ready => "ready",
            Self::StartingUp => "starting_up",
        }
    }
}

impl StatusMetric for AdminStatus {
    const NAME: &'static str = "restate.admin.status";
    const DESCRIPTION: &'static str =
        "Admin role status, 1 for the current status and 0 for all others";
    const ALL: &'static [Self] = &[Self::Unknown, Self::Ready, Self::StartingUp];

    fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Ready => "ready",
            Self::StartingUp => "starting_up",
        }
    }
}

impl StatusMetric for LogServerStatus {
    const NAME: &'static str = "restate.log_server.status";
    const DESCRIPTION: &'static str =
        "Log-server role status, 1 for the current status and 0 for all others";
    const ALL: &'static [Self] = &[
        Self::Unknown,
        Self::Ready,
        Self::StartingUp,
        Self::Failsafe,
        Self::Stopping,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Ready => "ready",
            Self::StartingUp => "starting_up",
            Self::Failsafe => "failsafe",
            Self::Stopping => "stopping",
        }
    }
}

impl StatusMetric for MetadataServerStatus {
    const NAME: &'static str = "restate.metadata_server.status";
    const DESCRIPTION: &'static str =
        "Metadata server role status, 1 for the current status and 0 for all others";
    const ALL: &'static [Self] = &[
        Self::Unknown,
        Self::StartingUp,
        Self::AwaitingProvisioning,
        Self::Member,
        Self::Standby,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::StartingUp => "starting_up",
            Self::AwaitingProvisioning => "awaiting_provisioning",
            Self::Member => "member",
            Self::Standby => "standby",
        }
    }
}

impl StatusMetric for IngressStatus {
    const NAME: &'static str = "restate.ingress.status";
    const DESCRIPTION: &'static str =
        "HTTP ingress role status, 1 for the current status and 0 for all others";
    const ALL: &'static [Self] = &[Self::Unknown, Self::Ready, Self::StartingUp];

    fn label(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Ready => "ready",
            Self::StartingUp => "starting_up",
        }
    }
}

fn set_gauges<T: StatusMetric>(current: T) {
    for &status in T::ALL {
        let value = if status == current { 1.0 } else { 0.0 };
        gauge!(T::NAME, "status" => status.label()).set(value);
    }
}

fn spawn_reporter<T: StatusMetric>(status: HealthStatus<T>) -> Result<(), ShutdownError> {
    describe_gauge!(T::NAME, T::DESCRIPTION);
    let mut status_rx = status.subscribe();
    TaskCenter::spawn(TaskKind::Background, "status-metric", async move {
        cancellation_token()
            .run_until_cancelled(async {
                while status_rx.changed().await.is_ok() {
                    let current = *status_rx.borrow_and_update();
                    set_gauges(current);
                }
            })
            .await;
        Ok(())
    })?;
    Ok(())
}

/// Starts one reporter for the node status and one for each role that this node runs.
pub fn spawn_status_reporters(
    health: &Health,
    config: &Configuration,
) -> Result<(), ShutdownError> {
    spawn_reporter(health.node_status())?;
    if config.has_role(Role::Worker) {
        spawn_reporter(health.worker_status())?;
    }
    if config.has_role(Role::Admin) {
        spawn_reporter(health.admin_status())?;
    }
    if config.has_role(Role::LogServer) {
        spawn_reporter(health.log_server_status())?;
    }
    if config.has_role(Role::MetadataServer) {
        spawn_reporter(health.metadata_server_status())?;
    }
    if config.has_role(Role::HttpIngress) {
        spawn_reporter(health.ingress_status())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    /// `ALL` must hold every protobuf value exactly once, so no status is missing from the gauge.
    fn assert_all_values<T>()
    where
        T: StatusMetric + Into<i32> + TryFrom<i32>,
    {
        for (i, &status) in T::ALL.iter().enumerate() {
            assert_eq!(status.into(), i as i32, "{} is out of order", T::NAME);
        }
        assert!(
            T::try_from(T::ALL.len() as i32).is_err(),
            "{} misses a value",
            T::NAME
        );
    }

    #[test]
    fn status_gauges() {
        assert_all_values::<NodeStatus>();
        assert_all_values::<WorkerStatus>();
        assert_all_values::<AdminStatus>();
        assert_all_values::<LogServerStatus>();
        assert_all_values::<MetadataServerStatus>();
        assert_all_values::<IngressStatus>();

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            set_gauges(LogServerStatus::Ready);
            set_gauges(LogServerStatus::Failsafe);
            set_gauges(NodeStatus::Alive);
        });
        let rendered = handle.render();

        for expected in [
            r#"restate_log_server_status{status="failsafe"} 1"#,
            r#"restate_log_server_status{status="ready"} 0"#,
            r#"restate_log_server_status{status="unknown"} 0"#,
            r#"restate_log_server_status{status="starting_up"} 0"#,
            r#"restate_log_server_status{status="stopping"} 0"#,
            r#"restate_node_status{status="alive"} 1"#,
            r#"restate_node_status{status="shutting_down"} 0"#,
        ] {
            assert!(
                rendered.contains(expected),
                "missing `{expected}` in:\n{rendered}"
            );
        }
    }
}
