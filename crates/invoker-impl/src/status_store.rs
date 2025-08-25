// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use restate_invoker_api::status_handle::{InvocationStatusReport, InvocationStatusReportInner};

use restate_types::service_protocol::ServiceProtocolVersion;
use std::time::SystemTime;

#[derive(Default, Debug)]
pub(super) struct InvocationStatusStore(
    HashMap<PartitionLeaderEpoch, HashMap<InvocationId, InvocationStatusReportInner>>,
);

impl InvocationStatusStore {
    pub(super) fn status_for_partition(
        &self,
        partition_leader_epoch: PartitionLeaderEpoch,
    ) -> impl Iterator<Item = InvocationStatusReport> + '_ {
        self.0
            .get(&partition_leader_epoch)
            .into_iter()
            .flat_map(move |hash| {
                hash.iter().map(move |(invocation_id, report)| {
                    InvocationStatusReport::new(
                        *invocation_id,
                        partition_leader_epoch,
                        report.clone(),
                    )
                })
            })
    }

    // -- Methods used by the invoker to notify the status

    pub(super) fn on_start(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) {
        let report = self
            .0
            .entry(partition)
            .or_default()
            .entry(invocation_id)
            .or_default();
        report.start_count += 1;
        report.last_start_at = SystemTime::now();
        report.next_retry_at = None;
        report.in_flight = true;
    }

    pub(super) fn on_deployment_chosen(
        &mut self,
        partition: &PartitionLeaderEpoch,
        invocation_id: &InvocationId,
        deployment_id: DeploymentId,
        protocol_version: ServiceProtocolVersion,
    ) {
        if let Some(inner) = self.0.get_mut(partition)
            && let Some(report) = inner.get_mut(invocation_id)
        {
            report.last_attempt_deployment_id = Some(deployment_id);
            report.last_attempt_protocol_version = Some(protocol_version)
        }
    }

    pub(super) fn on_server_header_receiver(
        &mut self,
        partition: &PartitionLeaderEpoch,
        invocation_id: &InvocationId,
        x_restate_server_header: String,
    ) {
        if let Some(inner) = self.0.get_mut(partition)
            && let Some(report) = inner.get_mut(invocation_id)
        {
            report.last_attempt_server = Some(x_restate_server_header);
        }
    }

    pub(super) fn on_end(
        &mut self,
        partition: &PartitionLeaderEpoch,
        invocation_id: &InvocationId,
    ) {
        if let Some(inner) = self.0.get_mut(partition) {
            inner.remove(invocation_id);
            if inner.is_empty() {
                self.0.remove(partition);
            }
        }
    }

    pub(super) fn on_failure(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        reason: InvocationErrorReport,
        next_retry_at: Option<SystemTime>,
    ) {
        let report = self
            .0
            .entry(partition)
            .or_default()
            .entry(invocation_id)
            .or_default();
        report.in_flight = false;
        report.next_retry_at = next_retry_at;
        report.last_retry_attempt_failure = Some(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl InvocationStatusStore {
        pub fn resolve_invocation(
            &self,
            partition: PartitionLeaderEpoch,
            invocation_id: &InvocationId,
        ) -> Option<InvocationStatusReport> {
            self.0.get(&partition).and_then(|inner| {
                inner.get(invocation_id).map(|report| {
                    InvocationStatusReport::new(*invocation_id, partition, report.clone())
                })
            })
        }
    }
}
