// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::SystemTime;

use restate_platform::hash::HashMap;
use restate_types::identifiers::{DeploymentId, InvocationId};
use restate_types::journal_v2::UnresolvedFuture;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_worker_api::invoker::{
    InvocationErrorReport,
    status_handle::{InvocationStatusReport, InvocationStatusReportInner},
};

#[derive(Default, Debug)]
pub(super) struct InvocationStatusStore(HashMap<InvocationId, InvocationStatusReportInner>);

impl InvocationStatusStore {
    pub(super) fn status(&self) -> impl Iterator<Item = InvocationStatusReport> + '_ {
        self.0.iter().map(|(invocation_id, report)| {
            InvocationStatusReport::new(*invocation_id, report.clone())
        })
    }

    // -- Methods used by the invoker to notify the status

    pub(super) fn on_start(&mut self, invocation_id: InvocationId) {
        let report = self.0.entry(invocation_id).or_default();
        report.start_count += 1;
        report.last_start_at = SystemTime::now();
        report.next_retry_at = None;
        report.in_flight = true;
    }

    pub(super) fn on_progress_made(&mut self, invocation_id: &InvocationId) {
        if let Some(report) = self.0.get_mut(invocation_id) {
            // When we do progress, we reset the last retry attempt failure and last unresolved future as that's now invalid
            report.last_retry_attempt_failure = None;
            report.last_awaiting_on_unresolved_future = None;
        }
    }

    pub(super) fn on_deployment_chosen(
        &mut self,
        invocation_id: &InvocationId,
        deployment_id: DeploymentId,
        protocol_version: ServiceProtocolVersion,
    ) {
        if let Some(report) = self.0.get_mut(invocation_id) {
            report.last_attempt_deployment_id = Some(deployment_id);
            report.last_attempt_protocol_version = Some(protocol_version)
        }
    }

    pub(super) fn on_server_header_receiver(
        &mut self,
        invocation_id: &InvocationId,
        x_restate_server_header: String,
    ) {
        if let Some(report) = self.0.get_mut(invocation_id) {
            report.last_attempt_server = Some(x_restate_server_header);
        }
    }

    pub(super) fn on_awaiting_on(
        &mut self,
        invocation_id: &InvocationId,
        unresolved_future: UnresolvedFuture,
    ) {
        if let Some(report) = self.0.get_mut(invocation_id) {
            report.last_awaiting_on_unresolved_future = Some(unresolved_future);
        }
    }

    pub(super) fn on_end(&mut self, invocation_id: &InvocationId) {
        self.0.remove(invocation_id);
    }

    pub(super) fn on_failure(
        &mut self,
        invocation_id: InvocationId,
        reason: InvocationErrorReport,
        next_retry_at: Option<SystemTime>,
    ) {
        let report = self.0.entry(invocation_id).or_default();
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
            invocation_id: &InvocationId,
        ) -> Option<InvocationStatusReport> {
            self.0
                .get(invocation_id)
                .map(|report| InvocationStatusReport::new(*invocation_id, report.clone()))
        }
    }
}
