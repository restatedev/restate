// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;

use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatus,
    JournalMetadata, JournalRetentionPolicy, PreFlightInvocationMetadata, ScheduledInvocation,
    StatusTimestamps,
};
use restate_types::LimitKey;
use restate_types::Scope;
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{DeploymentId, InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::invocation::{
    InvocationTarget, ResponseResult, ServiceInvocationSpanContext, Source,
    VirtualObjectHandlerType,
};
use restate_types::journal_v2::{NotificationId, UnresolvedFuture};
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_util_string::{ReString, RestateString};

const TEXT_TABLE_INVOCATION_SEQUENCE_START: u128 = 1 << 64;

pub(super) struct FixtureFactory {
    next_invocation: u128,
}

impl FixtureFactory {
    pub(super) fn for_text_tables() -> Self {
        Self {
            next_invocation: TEXT_TABLE_INVOCATION_SEQUENCE_START,
        }
    }

    pub(super) fn create_invocation(
        &mut self,
        options: InvocationOptions<'_>,
    ) -> InvocationFixture {
        let sequence = self.next_invocation;
        self.next_invocation += 1;

        let scope = Scope::try_from_static("scope-a").unwrap();
        let partition_key = scope.partition_key();
        let id = InvocationId::from_parts(partition_key, InvocationUuid::from_u128(sequence));
        let caller_id = InvocationId::from_parts(0, InvocationUuid::from_u128(10_000 + sequence));
        let deployment_id = DeploymentId::from_parts(1_000, 1);
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
        InvocationFixture {
            id,
            status: options.status,
            target,
            vqueue_id: None,
            limit_key: "tenant/eu".parse::<LimitKey<ReString>>().unwrap(),
            source,
            execution_time: Some(MillisSinceEpoch::from(4_000)),
            idempotency_key: Some("request-1".into()),
            timestamps,
            completion_retention: Duration::from_secs(30),
            journal_retention: Duration::from_secs(10),
            journal: JournalMetadata::new(2, 2, ServiceInvocationSpanContext::empty()),
            pinned_deployment: Some(PinnedDeployment::new(
                deployment_id,
                ServiceProtocolVersion::V5,
            )),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum InvocationFixtureStatus {
    Scheduled,
    Inboxed,
    Running,
    Suspended,
    Paused,
    CompletedSuccess,
    CompletedFailure,
}

impl TryFrom<(&str, Option<&str>)> for InvocationFixtureStatus {
    type Error = anyhow::Error;

    fn try_from((status, completion_result): (&str, Option<&str>)) -> Result<Self, Self::Error> {
        match (status, completion_result) {
            ("scheduled", None) => Ok(Self::Scheduled),
            ("inboxed", None) => Ok(Self::Inboxed),
            ("invoked", None) => Ok(Self::Running),
            ("suspended", None) => Ok(Self::Suspended),
            ("paused", None) => Ok(Self::Paused),
            ("completed", Some("success")) => Ok(Self::CompletedSuccess),
            ("completed", Some("failure")) => Ok(Self::CompletedFailure),
            _ => anyhow::bail!(
                "unsupported invocation status values: status={status:?}, completion_result={completion_result:?}"
            ),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct InvocationOptions<'a> {
    pub(super) service_name: &'a str,
    pub(super) service_key: &'a str,
    pub(super) handler_name: &'a str,
    pub(super) status: InvocationFixtureStatus,
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
}

impl InvocationFixture {
    fn pre_flight_metadata(&self) -> PreFlightInvocationMetadata {
        PreFlightInvocationMetadata {
            invocation_target: self.target.clone(),
            vqueue_id: self.vqueue_id.clone(),
            limit_key: self.limit_key.clone(),
            source: self.source.clone(),
            execution_time: self.execution_time,
            idempotency_key: self.idempotency_key.clone(),
            timestamps: self.timestamps.clone(),
            completion_retention_duration: self.completion_retention,
            journal_retention_duration: self.journal_retention,
            ..PreFlightInvocationMetadata::mock()
        }
    }

    pub(super) fn invocation_status(&self) -> InvocationStatus {
        let metadata = InFlightInvocationMetadata {
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
        };

        match self.status {
            InvocationFixtureStatus::Scheduled => {
                InvocationStatus::Scheduled(ScheduledInvocation {
                    metadata: self.pre_flight_metadata(),
                })
            }
            InvocationFixtureStatus::Inboxed => InvocationStatus::Inboxed(InboxedInvocation {
                inbox_sequence_number: 1,
                metadata: self.pre_flight_metadata(),
            }),
            InvocationFixtureStatus::Running => InvocationStatus::Invoked(metadata),
            InvocationFixtureStatus::Suspended => InvocationStatus::Suspended {
                metadata,
                awaiting_on: UnresolvedFuture::Single(NotificationId::for_completion(1)),
            },
            InvocationFixtureStatus::Paused => InvocationStatus::Paused(metadata),
            InvocationFixtureStatus::CompletedSuccess => InvocationStatus::Completed(
                CompletedInvocation::from_in_flight_invocation_metadata(
                    metadata,
                    JournalRetentionPolicy::Retain,
                    ResponseResult::Success(Bytes::from_static(b"fixture result")),
                    MillisSinceEpoch::from(7_000),
                ),
            ),
            InvocationFixtureStatus::CompletedFailure => InvocationStatus::Completed(
                CompletedInvocation::from_in_flight_invocation_metadata(
                    metadata,
                    JournalRetentionPolicy::Retain,
                    ResponseResult::Failure(InvocationError::internal("fixture failure")),
                    MillisSinceEpoch::from(7_000),
                ),
            ),
        }
    }
}
