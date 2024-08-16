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
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{EntryIndex, InvocationId, PartitionKey};
use restate_types::invocation::{
    Header, InvocationInput, InvocationTarget, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::future::Future;
use std::ops::RangeInclusive;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceTable {
    Old,
    New,
}

/// Holds timestamps of the [`InvocationStatus`].
#[derive(Debug, Clone, PartialEq)]
pub struct StatusTimestamps {
    creation_time: MillisSinceEpoch,
    modification_time: MillisSinceEpoch,

    inboxed_transition_time: Option<MillisSinceEpoch>,
    scheduled_transition_time: Option<MillisSinceEpoch>,
    running_transition_time: Option<MillisSinceEpoch>,
    completed_transition_time: Option<MillisSinceEpoch>,
}

impl StatusTimestamps {
    pub fn init(creation_time: MillisSinceEpoch) -> Self {
        Self {
            creation_time,
            modification_time: creation_time,
            inboxed_transition_time: None,
            scheduled_transition_time: None,
            running_transition_time: None,
            completed_transition_time: None,
        }
    }

    pub fn new(
        creation_time: MillisSinceEpoch,
        modification_time: MillisSinceEpoch,
        inboxed_transition_time: Option<MillisSinceEpoch>,
        scheduled_transition_time: Option<MillisSinceEpoch>,
        running_transition_time: Option<MillisSinceEpoch>,
        completed_transition_time: Option<MillisSinceEpoch>,
    ) -> Self {
        Self {
            creation_time,
            modification_time,
            inboxed_transition_time,
            scheduled_transition_time,
            running_transition_time,
            completed_transition_time,
        }
    }

    /// Update the statistics with an updated [`Self::modification_time()`].
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub fn update(&mut self) {
        self.modification_time = MillisSinceEpoch::now()
    }

    /// Create a new StatusTimestamps data structure using the system time.
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub fn now() -> Self {
        StatusTimestamps::init(MillisSinceEpoch::now())
    }

    /// Update the statistics with an updated [`Self::inboxed_transition_time()`].
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    fn record_inboxed_transition_time(&mut self) {
        self.update();
        self.inboxed_transition_time = Some(self.modification_time)
    }

    /// Update the statistics with an updated [`Self::scheduled_transition_time()`].
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    fn record_scheduled_transition_time(&mut self) {
        self.update();
        self.scheduled_transition_time = Some(self.modification_time)
    }

    /// Update the statistics with an updated [`Self::running_transition_time()`].
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    fn record_running_transition_time(&mut self) {
        self.update();
        self.running_transition_time = Some(self.modification_time)
    }

    /// Update the statistics with an updated [`Self::completed_transition_time()`].
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    fn record_completed_transition_time(&mut self) {
        self.update();
        self.completed_transition_time = Some(self.modification_time)
    }

    /// Creation time of the [`InvocationStatus`].
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub unsafe fn creation_time(&self) -> MillisSinceEpoch {
        self.creation_time
    }

    /// Modification time of the [`InvocationStatus`].
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub unsafe fn modification_time(&self) -> MillisSinceEpoch {
        self.modification_time
    }

    /// Inboxed transition time of the [`InvocationStatus`], if any.
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub unsafe fn inboxed_transition_time(&self) -> Option<MillisSinceEpoch> {
        self.inboxed_transition_time
    }

    /// Scheduled transition time of the [`InvocationStatus`], if any.
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub unsafe fn scheduled_transition_time(&self) -> Option<MillisSinceEpoch> {
        self.scheduled_transition_time
    }

    /// First transition to Running time of the [`InvocationStatus`], if any.
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub unsafe fn running_transition_time(&self) -> Option<MillisSinceEpoch> {
        self.running_transition_time
    }

    /// Completed transition time of the [`InvocationStatus`], if any.
    ///
    /// # Safety
    /// The value of this time is not consistent across replicas of a partition, because it's not agreed.
    /// You **MUST NOT** use it within the Partition processor business logic, but only for observability purposes.
    pub unsafe fn completed_transition_time(&self) -> Option<MillisSinceEpoch> {
        self.completed_transition_time
    }
}

/// Status of an invocation.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum InvocationStatus {
    Scheduled(ScheduledInvocation),
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
            InvocationStatus::Scheduled(metadata) => Some(&metadata.metadata.invocation_target),
            InvocationStatus::Inboxed(metadata) => Some(&metadata.metadata.invocation_target),
            InvocationStatus::Invoked(metadata) => Some(&metadata.invocation_target),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.invocation_target),
            InvocationStatus::Completed(completed) => Some(&completed.invocation_target),
            _ => None,
        }
    }

    #[inline]
    pub fn idempotency_key(&self) -> Option<&ByteString> {
        match self {
            InvocationStatus::Scheduled(metadata) => metadata.metadata.idempotency_key.as_ref(),
            InvocationStatus::Inboxed(metadata) => metadata.metadata.idempotency_key.as_ref(),
            InvocationStatus::Invoked(metadata) => metadata.idempotency_key.as_ref(),
            InvocationStatus::Suspended { metadata, .. } => metadata.idempotency_key.as_ref(),
            InvocationStatus::Completed(completed) => completed.idempotency_key.as_ref(),
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
            InvocationStatus::Scheduled(metadata) => Some(&mut metadata.metadata.response_sinks),
            InvocationStatus::Inboxed(metadata) => Some(&mut metadata.metadata.response_sinks),
            InvocationStatus::Invoked(metadata) => Some(&mut metadata.response_sinks),
            InvocationStatus::Suspended { metadata, .. } => Some(&mut metadata.response_sinks),
            _ => None,
        }
    }

    #[inline]
    pub fn get_response_sinks(&self) -> Option<&HashSet<ServiceInvocationResponseSink>> {
        match self {
            InvocationStatus::Scheduled(metadata) => Some(&metadata.metadata.response_sinks),
            InvocationStatus::Inboxed(metadata) => Some(&metadata.metadata.response_sinks),
            InvocationStatus::Invoked(metadata) => Some(&metadata.response_sinks),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.response_sinks),
            _ => None,
        }
    }

    #[inline]
    pub fn get_timestamps(&self) -> Option<&StatusTimestamps> {
        match self {
            InvocationStatus::Scheduled(metadata) => Some(&metadata.metadata.timestamps),
            InvocationStatus::Inboxed(metadata) => Some(&metadata.metadata.timestamps),
            InvocationStatus::Invoked(metadata) => Some(&metadata.timestamps),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.timestamps),
            InvocationStatus::Completed(completed) => Some(&completed.timestamps),
            _ => None,
        }
    }

    #[inline]
    pub fn get_timestamps_mut(&mut self) -> Option<&mut StatusTimestamps> {
        match self {
            InvocationStatus::Scheduled(metadata) => Some(&mut metadata.metadata.timestamps),
            InvocationStatus::Inboxed(metadata) => Some(&mut metadata.metadata.timestamps),
            InvocationStatus::Invoked(metadata) => Some(&mut metadata.timestamps),
            InvocationStatus::Suspended { metadata, .. } => Some(&mut metadata.timestamps),
            InvocationStatus::Completed(completed) => Some(&mut completed.timestamps),
            _ => None,
        }
    }
}

protobuf_storage_encode_decode!(InvocationStatus);

/// Wrapper used by the table implementation, don't use it!
#[derive(Debug, Default, Clone, PartialEq)]
pub struct NeoInvocationStatus(pub InvocationStatus);

protobuf_storage_encode_decode!(NeoInvocationStatus);

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

/// This is similar to [ServiceInvocation].
#[derive(Debug, Clone, PartialEq)]
pub struct PreFlightInvocationMetadata {
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
    /// If zero, the invocation completion will not be retained.
    pub completion_retention_duration: Duration,
    pub idempotency_key: Option<ByteString>,

    /// Used by the Table implementation to pick where to write
    pub source_table: SourceTable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledInvocation {
    pub metadata: PreFlightInvocationMetadata,
}

impl ScheduledInvocation {
    pub fn from_pre_flight_invocation_metadata(mut metadata: PreFlightInvocationMetadata) -> Self {
        metadata.timestamps.record_scheduled_transition_time();

        Self { metadata }
    }
}

impl PreFlightInvocationMetadata {
    pub fn from_service_invocation(
        service_invocation: ServiceInvocation,
        source_table: SourceTable,
    ) -> Self {
        Self {
            response_sinks: service_invocation.response_sink.into_iter().collect(),
            timestamps: StatusTimestamps::now(),
            invocation_target: service_invocation.invocation_target,
            argument: service_invocation.argument,
            source: service_invocation.source,
            span_context: service_invocation.span_context,
            headers: service_invocation.headers,
            execution_time: service_invocation.execution_time,
            completion_retention_duration: service_invocation
                .completion_retention_duration
                .unwrap_or_default(),
            idempotency_key: service_invocation.idempotency_key,
            source_table,
        }
    }
}

/// This is similar to [ServiceInvocation], but allows many response sinks,
/// plus holds some inbox metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct InboxedInvocation {
    pub inbox_sequence_number: u64,
    pub metadata: PreFlightInvocationMetadata,
}

impl InboxedInvocation {
    pub fn from_pre_flight_invocation_metadata(
        mut metadata: PreFlightInvocationMetadata,
        inbox_sequence_number: u64,
    ) -> Self {
        metadata.timestamps.record_inboxed_transition_time();

        Self {
            inbox_sequence_number,
            metadata,
        }
    }

    pub fn from_scheduled_invocation(
        scheduled_invocation: ScheduledInvocation,
        inbox_sequence_number: u64,
    ) -> Self {
        Self::from_pre_flight_invocation_metadata(
            scheduled_invocation.metadata,
            inbox_sequence_number,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InFlightInvocationMetadata {
    pub invocation_target: InvocationTarget,
    pub journal_metadata: JournalMetadata,
    pub pinned_deployment: Option<PinnedDeployment>,
    pub response_sinks: HashSet<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,
    pub source: Source,
    /// If zero, the invocation completion will not be retained.
    pub completion_retention_duration: Duration,
    pub idempotency_key: Option<ByteString>,

    /// Used by the Table implementation to pick where to write
    pub source_table: SourceTable,
}

impl InFlightInvocationMetadata {
    pub fn from_pre_flight_invocation_metadata(
        mut pre_flight_invocation_metadata: PreFlightInvocationMetadata,
    ) -> (Self, InvocationInput) {
        pre_flight_invocation_metadata
            .timestamps
            .record_running_transition_time();

        (
            Self {
                invocation_target: pre_flight_invocation_metadata.invocation_target,
                journal_metadata: JournalMetadata::initialize(
                    pre_flight_invocation_metadata.span_context,
                ),
                pinned_deployment: None,
                response_sinks: pre_flight_invocation_metadata.response_sinks,
                timestamps: pre_flight_invocation_metadata.timestamps,
                source: pre_flight_invocation_metadata.source,
                completion_retention_duration: pre_flight_invocation_metadata
                    .completion_retention_duration,
                idempotency_key: pre_flight_invocation_metadata.idempotency_key,
                source_table: pre_flight_invocation_metadata.source_table,
            },
            InvocationInput {
                argument: pre_flight_invocation_metadata.argument,
                headers: pre_flight_invocation_metadata.headers,
            },
        )
    }

    pub fn from_inboxed_invocation(
        inboxed_invocation: InboxedInvocation,
    ) -> (Self, InvocationInput) {
        Self::from_pre_flight_invocation_metadata(inboxed_invocation.metadata)
    }

    pub fn set_pinned_deployment(&mut self, pinned_deployment: PinnedDeployment) {
        debug_assert_eq!(
            self.pinned_deployment, None,
            "No deployment should be chosen for the current invocation"
        );
        self.pinned_deployment = Some(pinned_deployment);
        self.timestamps.update();
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletedInvocation {
    pub invocation_target: InvocationTarget,
    pub span_context: ServiceInvocationSpanContext,
    pub source: Source,
    pub idempotency_key: Option<ByteString>,
    pub timestamps: StatusTimestamps,
    pub response_result: ResponseResult,
    pub completion_retention_duration: Duration,

    /// Used by the Table implementation to pick where to write
    pub source_table: SourceTable,
}

impl CompletedInvocation {
    pub fn from_in_flight_invocation_metadata(
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        response_result: ResponseResult,
    ) -> (Self, Duration) {
        in_flight_invocation_metadata
            .timestamps
            .record_completed_transition_time();

        (
            Self {
                invocation_target: in_flight_invocation_metadata.invocation_target,
                span_context: in_flight_invocation_metadata.journal_metadata.span_context,
                source: in_flight_invocation_metadata.source,
                idempotency_key: in_flight_invocation_metadata.idempotency_key,
                timestamps: in_flight_invocation_metadata.timestamps,
                response_result,
                completion_retention_duration: in_flight_invocation_metadata
                    .completion_retention_duration,
                source_table: in_flight_invocation_metadata.source_table,
            },
            in_flight_invocation_metadata.completion_retention_duration,
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

    fn all_invocation_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send;
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

#[cfg(any(test, feature = "test-util"))]
mod test_util {
    use super::*;

    use restate_types::invocation::VirtualObjectHandlerType;

    impl InFlightInvocationMetadata {
        pub fn mock() -> Self {
            InFlightInvocationMetadata {
                invocation_target: InvocationTarget::virtual_object(
                    "MyService",
                    "MyKey",
                    "mock",
                    VirtualObjectHandlerType::Exclusive,
                ),
                journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
                pinned_deployment: None,
                response_sinks: HashSet::new(),
                timestamps: StatusTimestamps::now(),
                source: Source::Ingress,
                completion_retention_duration: Duration::ZERO,
                idempotency_key: None,
                source_table: SourceTable::New,
            }
        }
    }

    impl CompletedInvocation {
        pub fn mock_neo() -> Self {
            let mut timestamps = StatusTimestamps::now();
            timestamps.record_running_transition_time();
            timestamps.record_completed_transition_time();

            CompletedInvocation {
                invocation_target: InvocationTarget::virtual_object(
                    "MyService",
                    "MyKey",
                    "mock",
                    VirtualObjectHandlerType::Exclusive,
                ),
                span_context: ServiceInvocationSpanContext::default(),
                source: Source::Ingress,
                idempotency_key: None,
                timestamps,
                response_result: ResponseResult::Success(Bytes::from_static(b"123")),
                completion_retention_duration: Duration::from_secs(60 * 60),
                source_table: SourceTable::New,
            }
        }

        pub fn mock_old() -> Self {
            CompletedInvocation {
                invocation_target: InvocationTarget::virtual_object(
                    "MyService",
                    "MyKey",
                    "mock",
                    VirtualObjectHandlerType::Exclusive,
                ),
                span_context: ServiceInvocationSpanContext::default(),
                source: Source::Ingress,
                idempotency_key: None,
                timestamps: StatusTimestamps::now(),
                response_result: ResponseResult::Success(Bytes::from_static(b"123")),
                completion_retention_duration: Duration::from_secs(60 * 60),
                source_table: SourceTable::Old,
            }
        }
    }
}
