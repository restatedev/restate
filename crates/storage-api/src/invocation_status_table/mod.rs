// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt::Display;
use std::future::Future;
use std::ops::{ControlFlow, RangeInclusive};
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use opentelemetry::trace::TraceId;
use rangemap::RangeInclusiveMap;

use restate_types::RestateVersion;
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::ConversionError;
use restate_types::identifiers::{DeploymentId, InvocationId, PartitionKey, SubscriptionId};
use restate_types::invocation::{
    Header, InvocationEpoch, InvocationInput, InvocationTarget, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
};
use restate_types::journal_v2::{CompletionId, EntryIndex, NotificationId};
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::time::MillisSinceEpoch;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

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
    /// Create a new StatusTimestamps data structure using the system time.
    pub fn init(created_at: MillisSinceEpoch) -> Self {
        // typically, created_at is the timestamp of the command entry in bifrost that
        // creates this invocation.
        Self {
            creation_time: created_at,
            modification_time: created_at,
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
    pub fn update(&mut self, timestamp: MillisSinceEpoch) {
        self.modification_time = self.modification_time.max(timestamp);
    }

    /// Update the statistics with an updated [`Self::inboxed_transition_time()`].
    fn record_inboxed_transition_time(&mut self, timestamp: MillisSinceEpoch) {
        self.update(timestamp);
        self.inboxed_transition_time = Some(self.modification_time)
    }

    /// Update the statistics with an updated [`Self::scheduled_transition_time()`].
    fn record_scheduled_transition_time(&mut self, timestamp: MillisSinceEpoch) {
        self.update(timestamp);
        self.scheduled_transition_time = Some(self.modification_time)
    }

    /// Update the statistics with an updated [`Self::running_transition_time()`].
    fn record_running_transition_time(&mut self, timestamp: MillisSinceEpoch) {
        self.update(timestamp);
        self.running_transition_time = Some(self.modification_time)
    }

    /// Update the statistics with an updated [`Self::completed_transition_time()`].
    fn record_completed_transition_time(&mut self, timestamp: MillisSinceEpoch) {
        self.update(timestamp);
        self.completed_transition_time = Some(self.modification_time)
    }

    /// Creation time of the [`InvocationStatus`].
    pub fn creation_time(&self) -> MillisSinceEpoch {
        self.creation_time
    }

    /// Modification time of the [`InvocationStatus`].
    pub fn modification_time(&self) -> MillisSinceEpoch {
        self.modification_time
    }

    /// Inboxed transition time of the [`InvocationStatus`], if any.
    pub fn inboxed_transition_time(&self) -> Option<MillisSinceEpoch> {
        self.inboxed_transition_time
    }

    /// Scheduled transition time of the [`InvocationStatus`], if any.
    pub fn scheduled_transition_time(&self) -> Option<MillisSinceEpoch> {
        self.scheduled_transition_time
    }

    /// First transition to Running time of the [`InvocationStatus`], if any.
    pub fn running_transition_time(&self) -> Option<MillisSinceEpoch> {
        self.running_transition_time
    }

    /// Completed transition time of the [`InvocationStatus`], if any.
    pub fn completed_transition_time(&self) -> Option<MillisSinceEpoch> {
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
        waiting_for_notifications: HashSet<NotificationId>,
    },
    Completed(CompletedInvocation),
    /// Service instance is currently not invoked
    #[default]
    Free,
}

impl PartitionStoreProtobufValue for InvocationStatus {
    type ProtobufType = crate::protobuf_types::v1::InvocationStatusV2;
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
    pub fn source(&self) -> Option<&Source> {
        match self {
            InvocationStatus::Scheduled(metadata) => Some(&metadata.metadata.source),
            InvocationStatus::Inboxed(metadata) => Some(&metadata.metadata.source),
            InvocationStatus::Invoked(metadata) => Some(&metadata.source),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.source),
            InvocationStatus::Completed(completed) => Some(&completed.source),
            _ => None,
        }
    }

    #[inline]
    pub fn execution_time(&self) -> Option<MillisSinceEpoch> {
        match self {
            InvocationStatus::Scheduled(metadata) => metadata.metadata.execution_time,
            InvocationStatus::Inboxed(metadata) => metadata.metadata.execution_time,
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
            InvocationStatus::Invoked(InFlightInvocationMetadata {
                journal_metadata, ..
            })
            | InvocationStatus::Suspended {
                metadata:
                    InFlightInvocationMetadata {
                        journal_metadata, ..
                    },
                ..
            }
            | InvocationStatus::Completed(CompletedInvocation {
                journal_metadata, ..
            }) => Some(journal_metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn get_journal_metadata(&self) -> Option<&JournalMetadata> {
        match self {
            InvocationStatus::Invoked(InFlightInvocationMetadata {
                journal_metadata, ..
            })
            | InvocationStatus::Suspended {
                metadata:
                    InFlightInvocationMetadata {
                        journal_metadata, ..
                    },
                ..
            }
            | InvocationStatus::Completed(CompletedInvocation {
                journal_metadata, ..
            }) => Some(journal_metadata),
            _ => None,
        }
    }

    #[inline]
    pub fn get_journal_metadata_mut(&mut self) -> Option<&mut JournalMetadata> {
        match self {
            InvocationStatus::Invoked(InFlightInvocationMetadata {
                journal_metadata, ..
            })
            | InvocationStatus::Suspended {
                metadata:
                    InFlightInvocationMetadata {
                        journal_metadata, ..
                    },
                ..
            }
            | InvocationStatus::Completed(CompletedInvocation {
                journal_metadata, ..
            }) => Some(journal_metadata),
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

    #[inline]
    pub fn discriminant(&self) -> Option<InvocationStatusDiscriminants> {
        match self {
            InvocationStatus::Scheduled(_) => Some(InvocationStatusDiscriminants::Scheduled),
            InvocationStatus::Inboxed(_) => Some(InvocationStatusDiscriminants::Inboxed),
            InvocationStatus::Invoked(_) => Some(InvocationStatusDiscriminants::Invoked),
            InvocationStatus::Suspended { .. } => Some(InvocationStatusDiscriminants::Suspended),
            InvocationStatus::Completed(_) => Some(InvocationStatusDiscriminants::Completed),
            InvocationStatus::Free => None,
        }
    }

    pub fn accessor(
        &self,
    ) -> InvocationStatusAccessor<
        &PreFlightInvocationMetadata,
        &InFlightInvocationMetadata,
        &CompletedInvocation,
    > {
        match self {
            InvocationStatus::Scheduled(s) => InvocationStatusAccessor::Scheduled(&s.metadata),
            InvocationStatus::Inboxed(i) => InvocationStatusAccessor::Inboxed(&i.metadata),
            InvocationStatus::Invoked(metadata) => InvocationStatusAccessor::Invoked(metadata),
            InvocationStatus::Suspended { metadata, .. } => {
                InvocationStatusAccessor::Suspended(metadata)
            }
            InvocationStatus::Completed(c) => InvocationStatusAccessor::Completed(c),
            InvocationStatus::Free => InvocationStatusAccessor::Free,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvocationStatusDiscriminants {
    Scheduled,
    Inboxed,
    Invoked,
    Suspended,
    Killed,
    Completed,
}

/// Metadata associated with a journal
#[derive(Debug, Clone, PartialEq)]
pub struct JournalMetadata {
    pub length: EntryIndex,
    /// Number of commands stored in the current journal
    pub commands: EntryIndex,
    pub span_context: ServiceInvocationSpanContext,
}

impl JournalMetadata {
    pub fn new(
        length: EntryIndex,
        commands: EntryIndex,
        span_context: ServiceInvocationSpanContext,
    ) -> Self {
        Self {
            span_context,
            length,
            commands,
        }
    }

    pub fn initialize(span_context: ServiceInvocationSpanContext) -> Self {
        Self::new(0, 0, span_context)
    }

    pub fn empty() -> Self {
        Self::initialize(ServiceInvocationSpanContext::empty())
    }
}

/// This is similar to [ServiceInvocation].
#[derive(Debug, Clone, PartialEq)]
pub struct PreFlightInvocationMetadata {
    pub response_sinks: HashSet<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,

    // --- From ServiceInvocation
    pub invocation_target: InvocationTarget,
    /// Restate version the invocation was created with.
    ///
    /// This is agreed among replicas, but be aware **it might come from the future**.
    /// No, this ain't a Marty McFly adventure, the problem is that the PP leader proposing the request to Bifrost
    /// might be on a newer version than the replicas, or the subsequent leader in the next leader epoch.
    pub created_using_restate_version: RestateVersion,

    // Could be split out of ServiceInvocation, e.g. InvocationContent or similar.
    pub argument: Bytes,
    pub source: Source,
    pub span_context: ServiceInvocationSpanContext,
    pub headers: Vec<Header>,
    /// Time when the request should be executed
    pub execution_time: Option<MillisSinceEpoch>,

    /// If zero, the invocation completion will not be retained.
    pub completion_retention_duration: Duration,

    /// If zero, the journal will not be retained.
    pub journal_retention_duration: Duration,

    pub idempotency_key: Option<ByteString>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledInvocation {
    pub metadata: PreFlightInvocationMetadata,
}

impl ScheduledInvocation {
    pub fn from_pre_flight_invocation_metadata(
        mut metadata: PreFlightInvocationMetadata,
        timestamp: MillisSinceEpoch,
    ) -> Self {
        metadata
            .timestamps
            .record_scheduled_transition_time(timestamp);

        Self { metadata }
    }
}

impl PreFlightInvocationMetadata {
    pub fn from_service_invocation(
        created_at: MillisSinceEpoch,
        service_invocation: ServiceInvocation,
    ) -> Self {
        Self {
            response_sinks: service_invocation.response_sink.into_iter().collect(),
            timestamps: StatusTimestamps::init(created_at),
            invocation_target: service_invocation.invocation_target,
            argument: service_invocation.argument,
            source: service_invocation.source,
            span_context: service_invocation.span_context,
            headers: service_invocation.headers,
            execution_time: service_invocation.execution_time,
            completion_retention_duration: service_invocation.completion_retention_duration,
            journal_retention_duration: service_invocation.journal_retention_duration,
            idempotency_key: service_invocation.idempotency_key,
            created_using_restate_version: service_invocation.restate_version,
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
        timestamp: MillisSinceEpoch,
    ) -> Self {
        metadata
            .timestamps
            .record_inboxed_transition_time(timestamp);

        Self {
            inbox_sequence_number,
            metadata,
        }
    }

    pub fn from_scheduled_invocation(
        scheduled_invocation: ScheduledInvocation,
        inbox_sequence_number: u64,
        timestamp: MillisSinceEpoch,
    ) -> Self {
        Self::from_pre_flight_invocation_metadata(
            scheduled_invocation.metadata,
            inbox_sequence_number,
            timestamp,
        )
    }
}

/// This map is used to record trim points and determine whether a completion from an old epoch should be accepted or rejected.
///
/// For more details, see the unit tests below and InvocationStatusExt in the restate-worker module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionRangeEpochMap(RangeInclusiveMap<CompletionId, InvocationEpoch>);

impl Default for CompletionRangeEpochMap {
    fn default() -> Self {
        Self(RangeInclusiveMap::from([(
            CompletionId::MIN..=CompletionId::MAX,
            InvocationEpoch::MIN,
        )]))
    }
}

impl CompletionRangeEpochMap {
    /// This must use the vec returned by [Self::into_trim_points_iter].
    pub fn from_trim_points(
        serialized_completion_range_epoch_map: impl IntoIterator<Item = (CompletionId, InvocationEpoch)>,
    ) -> Self {
        let mut this = Self::default();

        for (first_inclusive_completion_id_of_new_epoch, new_epoch) in
            serialized_completion_range_epoch_map
        {
            this.add_trim_point(first_inclusive_completion_id_of_new_epoch, new_epoch);
        }

        this
    }

    /// Returns a serializable representation of the map
    pub fn into_trim_points_iter(self) -> impl Iterator<Item = (CompletionId, InvocationEpoch)> {
        debug_assert!(
            !self.0.is_empty(),
            "CompletionRangeEpochMap constraint not respected, it must contain at least one range 0..=MAX"
        );
        self.0
            .into_iter()
            .skip_while(
                // No need to serialize the default range
                |r| r.1 == 0,
            )
            .map(|(range, epoch)| (*range.start(), epoch))
    }

    pub fn add_trim_point(
        &mut self,
        first_inclusive_completion_id_of_new_epoch: CompletionId,
        new_epoch: InvocationEpoch,
    ) {
        self.0.insert(
            first_inclusive_completion_id_of_new_epoch..=CompletionId::MAX,
            new_epoch,
        );
    }

    pub fn maximum_epoch_for(&self, completion_id: CompletionId) -> InvocationEpoch {
        *self
            .0
            .get(&completion_id)
            .expect("This range map MUST not have gaps!")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InFlightInvocationMetadata {
    pub invocation_target: InvocationTarget,
    /// Restate version the invocation was created with.
    ///
    /// This is agreed among replicas, but be aware **it might come from the future**.
    /// No, this ain't a Marty McFly adventure, the problem is that the PP leader proposing the request to Bifrost
    /// might be on a newer version than the replicas, or the subsequent leader in the next leader epoch.
    pub created_using_restate_version: RestateVersion,
    pub journal_metadata: JournalMetadata,
    pub pinned_deployment: Option<PinnedDeployment>,
    pub response_sinks: HashSet<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,
    pub source: Source,
    /// For invocations that were originally scheduled, retains the time when the request was originally scheduled to execute
    pub execution_time: Option<MillisSinceEpoch>,

    /// If zero, the invocation completion will not be retained.
    pub completion_retention_duration: Duration,

    /// If zero, the journal will not be retained.
    pub journal_retention_duration: Duration,

    pub idempotency_key: Option<ByteString>,
    // TODO remove this when we remove protocol <= v3
    pub hotfix_apply_cancellation_after_deployment_is_pinned: bool,
    pub current_invocation_epoch: InvocationEpoch,
    pub completion_range_epoch_map: CompletionRangeEpochMap,
}

impl InFlightInvocationMetadata {
    pub fn from_pre_flight_invocation_metadata(
        mut pre_flight_invocation_metadata: PreFlightInvocationMetadata,
        timestamp: MillisSinceEpoch,
    ) -> (Self, InvocationInput) {
        pre_flight_invocation_metadata
            .timestamps
            .record_running_transition_time(timestamp);

        (
            Self {
                invocation_target: pre_flight_invocation_metadata.invocation_target,
                created_using_restate_version: pre_flight_invocation_metadata
                    .created_using_restate_version,
                journal_metadata: JournalMetadata::initialize(
                    pre_flight_invocation_metadata.span_context,
                ),
                pinned_deployment: None,
                response_sinks: pre_flight_invocation_metadata.response_sinks,
                timestamps: pre_flight_invocation_metadata.timestamps,
                source: pre_flight_invocation_metadata.source,
                execution_time: pre_flight_invocation_metadata.execution_time,
                completion_retention_duration: pre_flight_invocation_metadata
                    .completion_retention_duration,
                journal_retention_duration: pre_flight_invocation_metadata
                    .journal_retention_duration,
                idempotency_key: pre_flight_invocation_metadata.idempotency_key,
                hotfix_apply_cancellation_after_deployment_is_pinned: false,
                current_invocation_epoch: 0,
                completion_range_epoch_map: Default::default(),
            },
            InvocationInput {
                argument: pre_flight_invocation_metadata.argument,
                headers: pre_flight_invocation_metadata.headers,
            },
        )
    }

    pub fn from_inboxed_invocation(
        inboxed_invocation: InboxedInvocation,
        timestamp: MillisSinceEpoch,
    ) -> (Self, InvocationInput) {
        Self::from_pre_flight_invocation_metadata(inboxed_invocation.metadata, timestamp)
    }

    pub fn set_pinned_deployment(
        &mut self,
        pinned_deployment: PinnedDeployment,
        timestamp: MillisSinceEpoch,
    ) {
        debug_assert_eq!(
            self.pinned_deployment, None,
            "No deployment should be chosen for the current invocation"
        );
        self.pinned_deployment = Some(pinned_deployment);
        self.timestamps.update(timestamp);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompletedInvocation {
    pub invocation_target: InvocationTarget,
    /// Restate version the invocation was created with.
    ///
    /// This is agreed among replicas, but be aware **it might come from the future**.
    /// No, this ain't a Marty McFly adventure, the problem is that the PP leader proposing the request to Bifrost
    /// might be on a newer version than the replicas, or the subsequent leader in the next leader epoch.
    pub created_using_restate_version: RestateVersion,
    pub source: Source,
    /// For invocations that were originally scheduled, retains the time when the request was originally scheduled to execute
    pub execution_time: Option<MillisSinceEpoch>,
    pub idempotency_key: Option<ByteString>,
    pub timestamps: StatusTimestamps,
    pub response_result: ResponseResult,

    pub completion_retention_duration: Duration,
    pub journal_retention_duration: Duration,

    pub journal_metadata: JournalMetadata,
    pub pinned_deployment: Option<PinnedDeployment>,
}

#[derive(PartialEq, Eq)]
pub enum JournalRetentionPolicy {
    Retain,
    Drop,
}

impl CompletedInvocation {
    pub fn from_in_flight_invocation_metadata(
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        journal_retention_policy: JournalRetentionPolicy,
        response_result: ResponseResult,
        timestamp: MillisSinceEpoch,
    ) -> Self {
        in_flight_invocation_metadata
            .timestamps
            .record_completed_transition_time(timestamp);

        Self {
            invocation_target: in_flight_invocation_metadata.invocation_target,
            created_using_restate_version: in_flight_invocation_metadata
                .created_using_restate_version,
            source: in_flight_invocation_metadata.source,
            execution_time: in_flight_invocation_metadata.execution_time,
            idempotency_key: in_flight_invocation_metadata.idempotency_key,
            timestamps: in_flight_invocation_metadata.timestamps,
            response_result,
            completion_retention_duration: in_flight_invocation_metadata
                .completion_retention_duration,
            journal_retention_duration: in_flight_invocation_metadata.journal_retention_duration,
            journal_metadata: if journal_retention_policy == JournalRetentionPolicy::Retain {
                in_flight_invocation_metadata.journal_metadata
            } else {
                JournalMetadata::empty()
            },
            pinned_deployment: in_flight_invocation_metadata.pinned_deployment,
        }
    }

    /// Expiration time of the [`InvocationStatus::Completed`], if any.
    pub fn completion_expiry_time(&self) -> Option<MillisSinceEpoch> {
        self.timestamps
            .completed_transition_time()
            .map(|base| base + self.completion_retention_duration)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InvokedInvocationStatusLite {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub current_invocation_epoch: InvocationEpoch,
}

pub trait ReadOnlyInvocationStatusTable {
    fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<InvocationStatus>> + Send;
}

pub trait ScanInvocationStatusTable {
    type PreFlightInvocationMetadataAccessor<'a>: PreFlightInvocationMetadataAccessor;
    type InFlightInvocationMetadataAccessor<'a>: InFlightInvocationMetadataAccessor;
    type CompletedInvocationMetadataAccessor<'a>: CompletedInvocationMetadataAccessor;

    fn scan_invocation_statuses(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(InvocationId, InvocationStatus)>> + Send>;

    fn for_each_invocation_status<
        E: Into<anyhow::Error>,
        F: for<'a> FnMut(
                (
                    InvocationId,
                    InvocationStatusAccessor<
                        Self::PreFlightInvocationMetadataAccessor<'a>,
                        Self::InFlightInvocationMetadataAccessor<'a>,
                        Self::CompletedInvocationMetadataAccessor<'a>,
                    >,
                ),
            ) -> ControlFlow<std::result::Result<(), E>>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;

    fn scan_invoked_invocations(
        &self,
    ) -> Result<impl Stream<Item = Result<InvokedInvocationStatusLite>> + Send>;
}

pub trait InvocationStatusTable: ReadOnlyInvocationStatusTable {
    fn put_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        status: &InvocationStatus,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[cfg(any(test, feature = "test-util"))]
mod test_util {
    use super::*;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;

    use restate_types::invocation::VirtualObjectHandlerType;

    impl StatusTimestamps {
        pub fn mock() -> Self {
            Self::init(MillisSinceEpoch::now())
        }
    }

    impl PreFlightInvocationMetadata {
        pub fn mock() -> Self {
            PreFlightInvocationMetadata {
                invocation_target: InvocationTarget::virtual_object(
                    "MyService",
                    "MyKey",
                    "mock",
                    VirtualObjectHandlerType::Exclusive,
                ),
                created_using_restate_version: RestateVersion::current(),
                response_sinks: HashSet::new(),
                timestamps: StatusTimestamps::mock(),
                source: Source::Ingress(PartitionProcessorRpcRequestId::default()),
                span_context: Default::default(),
                headers: vec![],
                execution_time: None,
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                idempotency_key: None,
                argument: Default::default(),
            }
        }
    }

    impl InFlightInvocationMetadata {
        pub fn mock() -> Self {
            InFlightInvocationMetadata {
                invocation_target: InvocationTarget::virtual_object(
                    "MyService",
                    "MyKey",
                    "mock",
                    VirtualObjectHandlerType::Exclusive,
                ),
                created_using_restate_version: RestateVersion::current(),
                journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
                pinned_deployment: None,
                response_sinks: HashSet::new(),
                timestamps: StatusTimestamps::mock(),
                source: Source::Ingress(PartitionProcessorRpcRequestId::default()),
                execution_time: None,
                completion_retention_duration: Duration::ZERO,
                journal_retention_duration: Duration::ZERO,
                idempotency_key: None,
                hotfix_apply_cancellation_after_deployment_is_pinned: false,
                current_invocation_epoch: 0,
                completion_range_epoch_map: Default::default(),
            }
        }
    }

    impl CompletedInvocation {
        pub fn mock_neo() -> Self {
            let mut timestamps = StatusTimestamps::mock();
            let now = MillisSinceEpoch::now();
            timestamps.record_running_transition_time(now);
            timestamps.record_completed_transition_time(now);

            CompletedInvocation {
                invocation_target: InvocationTarget::virtual_object(
                    "MyService",
                    "MyKey",
                    "mock",
                    VirtualObjectHandlerType::Exclusive,
                ),
                created_using_restate_version: RestateVersion::current(),
                source: Source::Ingress(PartitionProcessorRpcRequestId::default()),
                execution_time: None,
                idempotency_key: None,
                timestamps,
                response_result: ResponseResult::Success(Bytes::from_static(b"123")),
                completion_retention_duration: Duration::from_secs(60 * 60),
                journal_retention_duration: Duration::ZERO,
                journal_metadata: JournalMetadata::empty(),
                pinned_deployment: None,
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
                created_using_restate_version: RestateVersion::current(),
                source: Source::Ingress(PartitionProcessorRpcRequestId::default()),
                execution_time: None,
                idempotency_key: None,
                timestamps: StatusTimestamps::mock(),
                response_result: ResponseResult::Success(Bytes::from_static(b"123")),
                completion_retention_duration: Duration::from_secs(60 * 60),
                journal_metadata: JournalMetadata::empty(),
                journal_retention_duration: Duration::ZERO,
                pinned_deployment: None,
            }
        }
    }
}

/// Lite status of an invocation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InvocationLite {
    pub status: InvocationStatusDiscriminants,
    pub invocation_target: InvocationTarget,
    pub current_invocation_epoch: InvocationEpoch,
}

impl PartitionStoreProtobufValue for InvocationLite {
    type ProtobufType = crate::protobuf_types::v1::InvocationV2Lite;
}

// TODO remove this once we remove the old InvocationStatus
#[derive(Debug, Default, Clone, PartialEq)]
pub struct InvocationStatusV1(pub InvocationStatus);

impl PartitionStoreProtobufValue for InvocationStatusV1 {
    type ProtobufType = crate::protobuf_types::v1::InvocationStatus;
}

pub enum InvocationStatusAccessor<
    PreFlightInvocationMetadataAccessor,
    InFlightInvocationMetadataAccessor,
    CompletedInvocationMetadataAccessor,
> {
    Scheduled(PreFlightInvocationMetadataAccessor),
    Inboxed(PreFlightInvocationMetadataAccessor),
    Invoked(InFlightInvocationMetadataAccessor),
    Suspended(InFlightInvocationMetadataAccessor),
    Completed(CompletedInvocationMetadataAccessor),
    Free,
}

impl<
    P: PreFlightInvocationMetadataAccessor,
    I: InFlightInvocationMetadataAccessor,
    C: CompletedInvocationMetadataAccessor,
> InvocationStatusAccessor<P, I, C>
{
    pub fn invocation_target(
        &self,
    ) -> std::result::Result<Option<InvocationTargetRef>, restate_types::errors::ConversionError>
    {
        match self {
            Self::Scheduled(metadata) | Self::Inboxed(metadata) => {
                Ok(Some(metadata.invocation_target()?))
            }
            Self::Invoked(metadata) | Self::Suspended(metadata) => {
                Ok(Some(metadata.invocation_target()?))
            }
            Self::Completed(completed) => Ok(Some(completed.invocation_target()?)),
            _ => Ok(None),
        }
    }

    pub fn source(&self) -> std::result::Result<Option<SourceRef>, ConversionError> {
        Ok(match self {
            Self::Scheduled(metadata) | Self::Inboxed(metadata) => Some(metadata.source()?),
            Self::Invoked(metadata) | Self::Suspended(metadata) => Some(metadata.source()?),
            Self::Completed(completed) => Some(completed.source()?),
            _ => None,
        })
    }

    pub fn execution_time(&self) -> Option<MillisSinceEpoch> {
        match self {
            Self::Scheduled(metadata) | Self::Inboxed(metadata) => metadata.execution_time(),
            _ => None,
        }
    }

    pub fn idempotency_key(&self) -> Option<&str> {
        match self {
            Self::Scheduled(metadata) | Self::Inboxed(metadata) => metadata.idempotency_key(),
            Self::Invoked(metadata) | Self::Suspended(metadata) => metadata.idempotency_key(),
            Self::Completed(completed) => completed.idempotency_key(),
            _ => None,
        }
    }
}

pub trait PreFlightInvocationMetadataAccessor: InvocationMetadataAccessor {}

impl<T: PreFlightInvocationMetadataAccessor + InvocationMetadataAccessor + ?Sized>
    PreFlightInvocationMetadataAccessor for &T
{
}

pub trait JournalMetadataAccessor {
    fn trace_id(&self) -> std::result::Result<TraceId, ConversionError>;
    fn length(&self) -> u32;
    fn commands(&self) -> u32;
}

impl<T: JournalMetadataAccessor + ?Sized> JournalMetadataAccessor for &T {
    fn trace_id(&self) -> std::result::Result<TraceId, ConversionError> {
        (*self).trace_id()
    }
    fn length(&self) -> u32 {
        (*self).length()
    }
    fn commands(&self) -> u32 {
        (*self).commands()
    }
}

pub trait InFlightInvocationMetadataAccessor:
    InvocationMetadataAccessor + JournalMetadataAccessor
{
    fn deployment_id(&self) -> std::result::Result<Option<DeploymentId>, ConversionError>;
    fn service_protocol_version(
        &self,
    ) -> std::result::Result<Option<ServiceProtocolVersion>, ConversionError>;
}

impl<
    T: InFlightInvocationMetadataAccessor
        + InvocationMetadataAccessor
        + JournalMetadataAccessor
        + ?Sized,
> InFlightInvocationMetadataAccessor for &T
{
    fn deployment_id(&self) -> std::result::Result<Option<DeploymentId>, ConversionError> {
        (*self).deployment_id()
    }
    fn service_protocol_version(
        &self,
    ) -> std::result::Result<Option<ServiceProtocolVersion>, ConversionError> {
        (*self).service_protocol_version()
    }
}

pub trait CompletedInvocationMetadataAccessor:
    InvocationMetadataAccessor + JournalMetadataAccessor
{
    fn response_result<'a>(&'a self)
    -> std::result::Result<ResponseResultRef<'a>, ConversionError>;
}

pub enum ResponseResultRef<'a> {
    Success,
    Failure {
        code: restate_types::errors::InvocationErrorCode,
        message: BytesOrStr<'a>,
    },
}

#[derive(derive_more::From)]
pub enum BytesOrStr<'a> {
    Str(&'a str),
    Bytes(Bytes),
}

impl<'a> BytesOrStr<'a> {
    pub fn as_str(&self, field_name: &'static str) -> std::result::Result<&str, ConversionError> {
        match self {
            BytesOrStr::Str(s) => Ok(s),
            BytesOrStr::Bytes(bytes) => str::from_utf8(bytes.as_ref())
                .map_err(|_| ConversionError::invalid_data(field_name)),
        }
    }
}

impl<T: CompletedInvocationMetadataAccessor + ?Sized> CompletedInvocationMetadataAccessor for &T {
    fn response_result<'a>(
        &'a self,
    ) -> std::result::Result<ResponseResultRef<'a>, ConversionError> {
        (*self).response_result()
    }
}

pub trait InvocationMetadataAccessor: Send + Sync {
    fn invocation_target(&self) -> std::result::Result<InvocationTargetRef, ConversionError>;
    fn idempotency_key(&self) -> Option<&str>;

    fn creation_time(&self) -> MillisSinceEpoch;
    fn modification_time(&self) -> MillisSinceEpoch;
    fn inboxed_transition_time(&self) -> Option<MillisSinceEpoch>;
    fn scheduled_transition_time(&self) -> Option<MillisSinceEpoch>;
    fn running_transition_time(&self) -> Option<MillisSinceEpoch>;
    fn completed_transition_time(&self) -> Option<MillisSinceEpoch>;

    fn created_using_restate_version(&self) -> &str;
    fn source(&self) -> std::result::Result<SourceRef, ConversionError>;
    fn execution_time(&self) -> Option<MillisSinceEpoch>;
    fn completion_retention_duration(&self) -> std::result::Result<Duration, ConversionError>;
    fn journal_retention_duration(&self) -> std::result::Result<Duration, ConversionError>;
}

pub enum SourceRef {
    Ingress,
    Subscription(SubscriptionId),
    Service(InvocationId, InvocationTargetRef),
    RestartAsNew(InvocationId),
    Internal,
}

impl<'a> From<&'a Source> for SourceRef {
    fn from(value: &'a Source) -> Self {
        match value {
            Source::Ingress(_) => Self::Ingress,
            Source::Subscription(id) => Self::Subscription(*id),
            Source::Service(id, target) => Self::Service(*id, target.into()),
            Source::RestartAsNew(id) => Self::RestartAsNew(*id),
            Source::Internal => Self::Internal,
        }
    }
}

impl<T: InvocationMetadataAccessor + ?Sized> InvocationMetadataAccessor for &T {
    fn invocation_target(
        &self,
    ) -> std::result::Result<InvocationTargetRef, restate_types::errors::ConversionError> {
        (*self).invocation_target()
    }
    fn idempotency_key(&self) -> Option<&str> {
        (*self).idempotency_key()
    }
    fn creation_time(&self) -> MillisSinceEpoch {
        (*self).creation_time()
    }
    fn modification_time(&self) -> MillisSinceEpoch {
        (*self).modification_time()
    }
    fn inboxed_transition_time(&self) -> Option<MillisSinceEpoch> {
        (*self).inboxed_transition_time()
    }
    fn scheduled_transition_time(&self) -> Option<MillisSinceEpoch> {
        (*self).scheduled_transition_time()
    }
    fn running_transition_time(&self) -> Option<MillisSinceEpoch> {
        (*self).running_transition_time()
    }
    fn completed_transition_time(&self) -> Option<MillisSinceEpoch> {
        (*self).completed_transition_time()
    }
    fn created_using_restate_version(&self) -> &str {
        (*self).created_using_restate_version()
    }
    fn source(&self) -> std::result::Result<SourceRef, restate_types::errors::ConversionError> {
        (*self).source()
    }
    fn execution_time(&self) -> Option<MillisSinceEpoch> {
        (*self).execution_time()
    }
    fn completion_retention_duration(
        &self,
    ) -> std::result::Result<std::time::Duration, restate_types::errors::ConversionError> {
        (*self).completion_retention_duration()
    }
    fn journal_retention_duration(
        &self,
    ) -> std::result::Result<std::time::Duration, restate_types::errors::ConversionError> {
        (*self).journal_retention_duration()
    }
}

pub struct InvocationTargetRef {
    pub service_name: Bytes,
    pub key: Bytes,
    pub handler_name: Bytes,
    pub service_ty: restate_types::invocation::ServiceType,
}

impl InvocationTargetRef {
    pub fn service_name(
        &self,
    ) -> std::result::Result<&str, restate_types::errors::ConversionError> {
        str::from_utf8(self.service_name.as_ref())
            .map_err(|_| restate_types::errors::ConversionError::invalid_data("name"))
    }
    pub fn key(&self) -> std::result::Result<Option<&str>, restate_types::errors::ConversionError> {
        if !self.service_ty().is_keyed() {
            return Ok(None);
        }

        let key = str::from_utf8(self.key.as_ref())
            .map_err(|_| restate_types::errors::ConversionError::invalid_data("key"))?;

        Ok(Some(key))
    }
    pub fn handler_name(
        &self,
    ) -> std::result::Result<&str, restate_types::errors::ConversionError> {
        str::from_utf8(self.handler_name.as_ref())
            .map_err(|_| restate_types::errors::ConversionError::invalid_data("name"))
    }
    pub fn service_ty(&self) -> restate_types::invocation::ServiceType {
        self.service_ty
    }

    pub fn target_fmt<'a>(
        &'a self,
    ) -> std::result::Result<TargetFormatter<'a>, restate_types::errors::ConversionError> {
        let service_name = self.service_name()?;
        let key = self.key()?;
        let handler_name = self.handler_name()?;
        Ok(TargetFormatter(service_name, key, handler_name))
    }
}

pub struct TargetFormatter<'a>(&'a str, Option<&'a str>, &'a str);

impl<'a> Display for TargetFormatter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(key) = self.1 {
            write!(f, "{}/{}/{}", self.0, key, self.2)
        } else {
            write!(f, "{}/{}", self.0, self.2)
        }
    }
}

impl From<&InvocationTarget> for InvocationTargetRef {
    fn from(value: &InvocationTarget) -> Self {
        Self {
            service_name: value.service_name().as_bytes().clone(),
            key: match value.key() {
                Some(key) => key.as_bytes().clone(),
                None => Bytes::new(),
            },
            handler_name: value.handler_name().as_bytes().clone(),
            service_ty: value.service_ty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod completion_range_epoch_map {
        use super::*;

        #[test]
        fn default() {
            let map = CompletionRangeEpochMap::default();

            // The default should be fine with any completion
            assert_eq!(map.maximum_epoch_for(CompletionId::MIN), 0);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX / 2), 0);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX), 0);

            let expected_trim_points = vec![];
            assert_eq!(
                map.clone().into_trim_points_iter().collect::<Vec<_>>(),
                expected_trim_points
            );
            assert_eq!(
                CompletionRangeEpochMap::from_trim_points(expected_trim_points),
                map
            );
        }

        #[test]
        fn trim_at_1() {
            let mut map = CompletionRangeEpochMap::default();

            map.add_trim_point(1, 1);

            // Before 1 is epoch 0, After including 1 is epoch 1
            assert_eq!(map.maximum_epoch_for(0), 0);
            assert_eq!(map.maximum_epoch_for(1), 1);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX), 1);

            let expected_trim_points = vec![(1, 1)];
            assert_eq!(
                map.clone().into_trim_points_iter().collect::<Vec<_>>(),
                expected_trim_points
            );
            assert_eq!(
                CompletionRangeEpochMap::from_trim_points(expected_trim_points),
                map
            );
        }

        #[test]
        fn multiple_trims() {
            let mut map = CompletionRangeEpochMap::default();

            map.add_trim_point(5, 1);

            // 0..=4 -> 0
            // 5..=MAX -> 1
            assert_eq!(map.maximum_epoch_for(0), 0);
            assert_eq!(map.maximum_epoch_for(4), 0);
            assert_eq!(map.maximum_epoch_for(5), 1);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX), 1);

            map.add_trim_point(2, 2);

            // 0..=1 -> 0
            // 2..=MAX -> 2
            assert_eq!(map.maximum_epoch_for(0), 0);
            assert_eq!(map.maximum_epoch_for(1), 0);
            assert_eq!(map.maximum_epoch_for(2), 2);
            assert_eq!(map.maximum_epoch_for(3), 2);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX), 2);

            map.add_trim_point(5, 3);

            // 0..=1 -> 0
            // 2..=4 -> 2
            // 5..=MAX -> 3
            assert_eq!(map.maximum_epoch_for(0), 0);
            assert_eq!(map.maximum_epoch_for(1), 0);
            assert_eq!(map.maximum_epoch_for(2), 2);
            assert_eq!(map.maximum_epoch_for(3), 2);
            assert_eq!(map.maximum_epoch_for(4), 2);
            assert_eq!(map.maximum_epoch_for(5), 3);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX), 3);

            let expected_trim_points = vec![(2, 2), (5, 3)];
            assert_eq!(
                map.clone().into_trim_points_iter().collect::<Vec<_>>(),
                expected_trim_points
            );
            assert_eq!(
                CompletionRangeEpochMap::from_trim_points(expected_trim_points),
                map
            );
        }

        #[test]
        fn trim_same_point_twice() {
            let mut map = CompletionRangeEpochMap::default();

            map.add_trim_point(2, 1);

            // 0..=2 -> 0
            // 2..=MAX -> 1
            assert_eq!(map.maximum_epoch_for(0), 0);
            assert_eq!(map.maximum_epoch_for(1), 0);
            assert_eq!(map.maximum_epoch_for(2), 1);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX), 1);

            map.add_trim_point(2, 2);

            // 0..=2 -> 0
            // 2..=MAX -> 2
            assert_eq!(map.maximum_epoch_for(0), 0);
            assert_eq!(map.maximum_epoch_for(1), 0);
            assert_eq!(map.maximum_epoch_for(2), 2);
            assert_eq!(map.maximum_epoch_for(CompletionId::MAX), 2);

            let expected_trim_points = vec![(2, 2)];
            assert_eq!(
                map.clone().into_trim_points_iter().collect::<Vec<_>>(),
                expected_trim_points
            );
            assert_eq!(
                CompletionRangeEpochMap::from_trim_points(expected_trim_points),
                map
            );
        }
    }
}
