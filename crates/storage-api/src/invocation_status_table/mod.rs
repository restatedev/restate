// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{protobuf_storage_encode_decode, Result};
use bytes::Bytes;
use bytestring::ByteString;
use futures_util::Stream;
use restate_types::identifiers::{DeploymentId, EntryIndex, InvocationId, PartitionKey};
use restate_types::invocation::{
    Header, Idempotency, InvocationInput, InvocationTarget, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::future::Future;
use std::ops::RangeInclusive;
use std::time::Duration;

/// Holds timestamps of the [`InvocationStatus`].
#[derive(Debug, Clone, PartialEq)]
pub struct StatusTimestamps {
    creation_time: MillisSinceEpoch,
    modification_time: MillisSinceEpoch,
}

impl StatusTimestamps {
    pub fn new(creation_time: MillisSinceEpoch, modification_time: MillisSinceEpoch) -> Self {
        Self {
            creation_time,
            modification_time,
        }
    }

    pub fn now() -> Self {
        StatusTimestamps::new(MillisSinceEpoch::now(), MillisSinceEpoch::now())
    }

    /// Update the statistics with an updated [`Self::modification_time()`].
    pub fn update(&mut self) {
        self.modification_time = MillisSinceEpoch::now()
    }

    /// Creation time of the [`InvocationStatus`].
    ///
    /// Note: The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it for business logic, but only for observability purposes.
    pub fn creation_time(&self) -> MillisSinceEpoch {
        self.creation_time
    }

    /// Modification time of the [`InvocationStatus`].
    ///
    /// Note: The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it for business logic, but only for observability purposes.
    pub fn modification_time(&self) -> MillisSinceEpoch {
        self.modification_time
    }
}

/// Status of an invocation.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum InvocationStatus {
    Inboxed(InboxedInvocation),
    Invoked(InFlightInvocationMetadata),
    Suspended {
        metadata: InFlightInvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    Completed(CompletedInvocation),
    /// Service instance is currently not invoked
    #[default]
    Free,
}

impl InvocationStatus {
    #[inline]
    pub fn invocation_target(&self) -> Option<&InvocationTarget> {
        match self {
            InvocationStatus::Inboxed(metadata) => Some(&metadata.invocation_target),
            InvocationStatus::Invoked(metadata) => Some(&metadata.invocation_target),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.invocation_target),
            InvocationStatus::Completed(completed) => Some(&completed.invocation_target),
            _ => None,
        }
    }

    #[inline]
    pub fn into_journal_metadata(self) -> Option<JournalMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(metadata.journal_metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata.journal_metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn get_journal_metadata(&self) -> Option<&JournalMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(&metadata.journal_metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.journal_metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn get_journal_metadata_mut(&mut self) -> Option<&mut JournalMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(&mut metadata.journal_metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(&mut metadata.journal_metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn into_invocation_metadata(self) -> Option<InFlightInvocationMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn get_invocation_metadata(&self) -> Option<&InFlightInvocationMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn get_invocation_metadata_mut(&mut self) -> Option<&mut InFlightInvocationMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn get_response_sinks_mut(
        &mut self,
    ) -> Option<&mut HashSet<ServiceInvocationResponseSink>> {
        match self {
            InvocationStatus::Inboxed(metadata) => Some(&mut metadata.response_sinks),
            InvocationStatus::Invoked(metadata) => Some(&mut metadata.response_sinks),
            InvocationStatus::Suspended { metadata, .. } => Some(&mut metadata.response_sinks),
            _ => None,
        }
    }

    #[inline]
    pub fn get_timestamps(&self) -> Option<&StatusTimestamps> {
        match self {
            InvocationStatus::Inboxed(metadata) => Some(&metadata.timestamps),
            InvocationStatus::Invoked(metadata) => Some(&metadata.timestamps),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.timestamps),
            InvocationStatus::Completed(completed) => Some(&completed.timestamps),
            _ => None,
        }
    }

    pub fn update_timestamps(&mut self) {
        match self {
            InvocationStatus::Inboxed(metadata) => metadata.timestamps.update(),
            InvocationStatus::Invoked(metadata) => metadata.timestamps.update(),
            InvocationStatus::Suspended { metadata, .. } => metadata.timestamps.update(),
            _ => {}
        }
    }
}

protobuf_storage_encode_decode!(InvocationStatus);

/// Metadata associated with a journal
#[derive(Debug, Clone, PartialEq)]
pub struct JournalMetadata {
    pub length: EntryIndex,
    pub span_context: ServiceInvocationSpanContext,
}

impl JournalMetadata {
    pub fn new(length: EntryIndex, span_context: ServiceInvocationSpanContext) -> Self {
        Self {
            span_context,
            length,
        }
    }

    pub fn initialize(span_context: ServiceInvocationSpanContext) -> Self {
        Self::new(0, span_context)
    }
}

/// This is similar to [ServiceInvocation], but allows many response sinks,
/// plus holds some inbox metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct InboxedInvocation {
    pub inbox_sequence_number: u64,
    pub response_sinks: HashSet<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,

    // --- From ServiceInvocation
    pub invocation_target: InvocationTarget,

    // Could be split out of ServiceInvocation, e.g. InvocationContent or similar.
    pub argument: Bytes,
    pub source: Source,
    pub span_context: ServiceInvocationSpanContext,
    pub headers: Vec<Header>,
    /// Time when the request should be executed
    pub execution_time: Option<MillisSinceEpoch>,
    pub idempotency: Option<Idempotency>,
}

impl InboxedInvocation {
    pub fn from_service_invocation(
        service_invocation: ServiceInvocation,
        inbox_sequence_number: u64,
    ) -> Self {
        Self {
            inbox_sequence_number,
            response_sinks: service_invocation.response_sink.into_iter().collect(),
            timestamps: StatusTimestamps::now(),
            invocation_target: service_invocation.invocation_target,
            argument: service_invocation.argument,
            source: service_invocation.source,
            span_context: service_invocation.span_context,
            headers: service_invocation.headers,
            execution_time: service_invocation.execution_time,
            idempotency: service_invocation.idempotency,
        }
    }

    pub fn append_response_sink(&mut self, new_sink: ServiceInvocationResponseSink) {
        self.response_sinks.insert(new_sink);
        self.timestamps.update();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InFlightInvocationMetadata {
    pub invocation_target: InvocationTarget,
    pub journal_metadata: JournalMetadata,
    pub deployment_id: Option<DeploymentId>,
    pub response_sinks: HashSet<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,
    pub source: Source,
    /// If zero, the invocation completion will not be retained.
    pub completion_retention_time: Duration,
    pub idempotency_key: Option<ByteString>,
}

impl InFlightInvocationMetadata {
    pub fn from_service_invocation(
        service_invocation: ServiceInvocation,
    ) -> (Self, InvocationInput) {
        let (completion_retention_time, idempotency_key) = service_invocation
            .idempotency
            .map(|idempotency| (idempotency.retention, Some(idempotency.key)))
            .unwrap_or((Duration::ZERO, None));
        (
            Self {
                invocation_target: service_invocation.invocation_target,
                journal_metadata: JournalMetadata::initialize(service_invocation.span_context),
                deployment_id: None,
                response_sinks: service_invocation.response_sink.into_iter().collect(),
                timestamps: StatusTimestamps::now(),
                source: service_invocation.source,
                completion_retention_time,
                idempotency_key,
            },
            InvocationInput {
                argument: service_invocation.argument,
                headers: service_invocation.headers,
            },
        )
    }

    pub fn from_inboxed_invocation(
        mut inboxed_invocation: InboxedInvocation,
    ) -> (Self, InvocationInput) {
        let (completion_retention_time, idempotency_key) = inboxed_invocation
            .idempotency
            .map(|idempotency| (idempotency.retention, Some(idempotency.key)))
            .unwrap_or((Duration::ZERO, None));

        inboxed_invocation.timestamps.update();

        (
            Self {
                invocation_target: inboxed_invocation.invocation_target,
                journal_metadata: JournalMetadata::initialize(inboxed_invocation.span_context),
                deployment_id: None,
                response_sinks: inboxed_invocation.response_sinks,
                timestamps: inboxed_invocation.timestamps,
                source: inboxed_invocation.source,
                completion_retention_time,
                idempotency_key,
            },
            InvocationInput {
                argument: inboxed_invocation.argument,
                headers: inboxed_invocation.headers,
            },
        )
    }

    pub fn set_deployment_id(&mut self, deployment_id: DeploymentId) {
        debug_assert_eq!(
            self.deployment_id, None,
            "No deployment_id should be fixed for the current invocation"
        );
        self.deployment_id = Some(deployment_id);
        self.timestamps.update();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletedInvocation {
    pub invocation_target: InvocationTarget,
    pub source: Source,
    pub idempotency_key: Option<ByteString>,
    pub timestamps: StatusTimestamps,
    pub response_result: ResponseResult,
}

impl CompletedInvocation {
    pub fn from_in_flight_invocation_metadata(
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        response_result: ResponseResult,
    ) -> (Self, Duration) {
        in_flight_invocation_metadata.timestamps.update();

        (
            Self {
                invocation_target: in_flight_invocation_metadata.invocation_target,
                source: in_flight_invocation_metadata.source,
                idempotency_key: in_flight_invocation_metadata.idempotency_key,
                timestamps: in_flight_invocation_metadata.timestamps,
                response_result,
            },
            in_flight_invocation_metadata.completion_retention_time,
        )
    }
}

pub trait ReadOnlyInvocationStatusTable {
    fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<InvocationStatus>> + Send;

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationTarget)>> + Send;
}

pub trait InvocationStatusTable: ReadOnlyInvocationStatusTable {
    fn put_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: InvocationStatus,
    ) -> impl Future<Output = ()> + Send;

    fn delete_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = ()> + Send;
}

#[cfg(any(test, feature = "mocks"))]
mod mocks {
    use super::*;

    use restate_types::invocation::HandlerType;

    impl InFlightInvocationMetadata {
        pub fn mock() -> Self {
            InFlightInvocationMetadata {
                invocation_target: InvocationTarget::virtual_object(
                    "MyService",
                    "MyKey",
                    "mock",
                    HandlerType::Exclusive,
                ),
                journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
                deployment_id: None,
                response_sinks: HashSet::new(),
                timestamps: StatusTimestamps::now(),
                source: Source::Ingress,
                completion_retention_time: Duration::ZERO,
                idempotency_key: None,
            }
        }
    }
}
