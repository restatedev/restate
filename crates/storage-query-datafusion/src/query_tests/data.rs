// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use bytestring::ByteString;

use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, JournalMetadata, StatusTimestamps,
};
use restate_types::LimitKey;
use restate_types::Scope;
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{
    DeploymentId, InvocationId, InvocationUuid, ServiceId, WithPartitionKey,
};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocationSpanContext, ServiceType, Source, VirtualObjectHandlerType,
};
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_util_string::{ReString, RestateString};
use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;

pub(super) struct FixtureFactory {
    next_invocation: u128,
    next_vqueue: u64,
}

impl Default for FixtureFactory {
    fn default() -> Self {
        Self {
            next_invocation: 1,
            next_vqueue: 1,
        }
    }
}

impl FixtureFactory {
    pub(super) fn create_state(
        &self,
        scope: Option<&'static str>,
        service_name: &'static str,
        service_key: &'static str,
        state_key: &'static [u8],
        state_value: &'static [u8],
    ) -> StateFixture {
        StateFixture {
            service_id: ServiceId::new(
                scope.map(|scope| Scope::try_from_static(scope).unwrap()),
                service_name,
                service_key,
            ),
            state_key: Bytes::from_static(state_key),
            state_value: Bytes::from_static(state_value),
        }
    }

    pub(super) fn create_vqueue(&mut self) -> VQueueFixture {
        let scope = Scope::try_from_static("scope-a").unwrap();
        let id = VQueueId::custom(
            scope.partition_key(),
            format!("query-fixture-{}", self.next_vqueue),
        );
        self.next_vqueue += 1;
        VQueueFixture { id, scope }
    }

    pub(super) fn create_invocation(
        &mut self,
        options: InvocationOptions<'_>,
    ) -> InvocationFixture {
        let sequence = self.next_invocation;
        self.next_invocation += 1;

        let scope = options
            .vqueue
            .map(|vqueue| vqueue.scope.clone())
            .unwrap_or_else(|| Scope::try_from_static("scope-a").unwrap());
        let partition_key = scope.partition_key();
        let id = InvocationId::from_parts(partition_key, InvocationUuid::from_u128(sequence));
        let caller_id = InvocationId::from_parts(0, InvocationUuid::from_u128(10_000 + sequence));
        let deployment_id = DeploymentId::from_parts(1_000, 1);
        let vqueue_id = options.vqueue.map(|vqueue| vqueue.id.clone());

        let target = InvocationTarget::scoped_virtual_object(
            options.service_name,
            options.service_key,
            options.handler_name,
            VirtualObjectHandlerType::Exclusive,
            scope,
        );
        let source = Source::Service(
            caller_id,
            InvocationTarget::service("CallerService", "call"),
        );
        let timestamps = StatusTimestamps::new(
            MillisSinceEpoch::from(1_000),
            MillisSinceEpoch::from(6_000),
            Some(MillisSinceEpoch::from(2_000)),
            Some(MillisSinceEpoch::from(3_000)),
            Some(MillisSinceEpoch::from(5_000)),
            None,
        );
        let state = match options.status {
            InvocationFixtureStatus::Running => InvocationStatusReportInner {
                in_flight: true,
                start_count: 1,
                last_start_at: UNIX_EPOCH + Duration::from_secs(5),
                last_attempt_deployment_id: Some(deployment_id),
                last_attempt_protocol_version: Some(ServiceProtocolVersion::V5),
                last_attempt_server: Some("restate-sdk-rust/0.1.0".to_owned()),
                ..InvocationStatusReportInner::default()
            },
        };

        InvocationFixture {
            id,
            status: options.status,
            target,
            vqueue_id,
            limit_key: "tenant/eu".parse::<LimitKey<ReString>>().unwrap(),
            source,
            execution_time: Some(MillisSinceEpoch::from(4_000)),
            idempotency_key: Some("request-1".into()),
            timestamps,
            completion_retention: Duration::from_secs(30),
            journal_retention: Duration::from_secs(10),
            journal: JournalMetadata::new(7, 4, ServiceInvocationSpanContext::empty()),
            pinned_deployment: Some(PinnedDeployment::new(
                deployment_id,
                ServiceProtocolVersion::V5,
            )),
            state,
        }
    }
}

pub(super) struct StateFixture {
    pub(super) service_id: ServiceId,
    pub(super) state_key: Bytes,
    pub(super) state_value: Bytes,
}

#[derive(Clone)]
pub(super) struct VQueueFixture {
    pub(super) id: VQueueId,
    scope: Scope,
}

#[derive(Clone, Copy)]
pub(super) enum InvocationFixtureStatus {
    Running,
}

pub(super) struct InvocationOptions<'a> {
    pub(super) vqueue: Option<&'a VQueueFixture>,
    pub(super) service_name: &'a str,
    pub(super) service_key: &'a str,
    pub(super) handler_name: &'a str,
    pub(super) status: InvocationFixtureStatus,
}

impl Default for InvocationOptions<'_> {
    fn default() -> Self {
        Self {
            vqueue: None,
            service_name: "TestService",
            service_key: "key-1",
            handler_name: "run",
            status: InvocationFixtureStatus::Running,
        }
    }
}

pub(super) struct InvocationFixture {
    pub(super) id: InvocationId,
    pub(super) status: InvocationFixtureStatus,
    pub(super) target: InvocationTarget,
    pub(super) vqueue_id: Option<VQueueId>,
    pub(super) limit_key: LimitKey<ReString>,
    pub(super) source: Source,
    pub(super) execution_time: Option<MillisSinceEpoch>,
    pub(super) idempotency_key: Option<ByteString>,
    pub(super) timestamps: StatusTimestamps,
    pub(super) completion_retention: Duration,
    pub(super) journal_retention: Duration,
    pub(super) journal: JournalMetadata,
    pub(super) pinned_deployment: Option<PinnedDeployment>,
    pub(super) state: InvocationStatusReportInner,
}

impl InvocationFixture {
    pub(super) fn invocation_status(&self) -> InvocationStatus {
        match self.status {
            InvocationFixtureStatus::Running => {
                InvocationStatus::Invoked(InFlightInvocationMetadata {
                    invocation_target: self.target.clone(),
                    vqueue_id: self.vqueue_id.clone(),
                    limit_key: self.limit_key.clone(),
                    source: self.source.clone(),
                    execution_time: self.execution_time,
                    idempotency_key: self.idempotency_key.clone(),
                    timestamps: self.timestamps.clone(),
                    completion_retention_duration: self.completion_retention,
                    journal_retention_duration: self.journal_retention,
                    journal_metadata: self.journal.clone(),
                    pinned_deployment: self.pinned_deployment.clone(),
                    ..InFlightInvocationMetadata::mock()
                })
            }
        }
    }

    pub(super) fn source(&self) -> InvocationSource {
        match &self.source {
            Source::Ingress(_) => InvocationSource::new("ingress"),
            Source::Subscription(id) => InvocationSource {
                invoked_by: "subscription",
                subscription_id: Some(id.to_string()),
                ..InvocationSource::default()
            },
            Source::Service(id, target) => InvocationSource {
                invoked_by: "service",
                id: Some(id.to_string()),
                target: Some(target.to_string()),
                ..InvocationSource::default()
            },
            Source::RestartAsNew(id) => InvocationSource {
                invoked_by: "restart_as_new",
                restarted_from: Some(id.to_string()),
                ..InvocationSource::default()
            },
            Source::Internal => InvocationSource::new("restate"),
        }
    }

    pub(super) fn target_service_ty(&self) -> &'static str {
        match self.target.service_ty() {
            ServiceType::Service => "service",
            ServiceType::VirtualObject => "virtual_object",
            ServiceType::Workflow => "workflow",
        }
    }

    pub(super) fn last_failure(&self) -> Option<String> {
        self.state
            .last_retry_attempt_failure
            .as_ref()
            .map(|failure| failure.err.to_string())
    }

    pub(super) fn last_failure_error_code(&self) -> Option<&'static str> {
        self.state
            .last_retry_attempt_failure
            .as_ref()
            .and_then(|failure| failure.doc_error_code)
            .map(|code| code.code())
    }

    pub(super) fn status_name(&self) -> &'static str {
        match self.status {
            InvocationFixtureStatus::Running => "running",
        }
    }

    pub(super) fn completion_result(&self) -> Option<&'static str> {
        match self.status {
            InvocationFixtureStatus::Running => None,
        }
    }

    pub(super) fn completion_failure(&self) -> Option<String> {
        match self.status {
            InvocationFixtureStatus::Running => None,
        }
    }

    pub(super) fn last_awaiting_on_future_json(&self) -> Option<String> {
        self.state
            .last_awaiting_on_unresolved_future
            .as_ref()
            .map(|future| serde_json::to_string(future).unwrap())
    }

    pub(super) fn suspended_waiting(&self) -> Option<SuspendedWaiting> {
        match self.status {
            InvocationFixtureStatus::Running => None,
        }
    }
}

pub(super) struct SuspendedWaiting {
    pub(super) completions: Vec<u32>,
    pub(super) signals: Vec<u32>,
    pub(super) future_json: String,
}

#[derive(Default)]
pub(super) struct InvocationSource {
    pub(super) invoked_by: &'static str,
    pub(super) id: Option<String>,
    pub(super) subscription_id: Option<String>,
    pub(super) target: Option<String>,
    pub(super) restarted_from: Option<String>,
}

impl InvocationSource {
    fn new(invoked_by: &'static str) -> Self {
        Self {
            invoked_by,
            ..Self::default()
        }
    }
}
