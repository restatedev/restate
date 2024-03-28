// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use bytestring::ByteString;
use futures_util::Stream;
use restate_types::identifiers::{
    DeploymentId, EntryIndex, FullInvocationId, InvocationId, PartitionKey, ServiceId,
};
use restate_types::invocation::{Header, ResponseResult, ServiceInvocation, ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::future::Future;
use std::ops::RangeInclusive;
use std::time::Duration;
use bytes::Bytes;

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
    Inboxed(InboxedInvocationMetadata),
    Invoked(InFlightInvocationMetadata),
    Suspended {
        metadata: InFlightInvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    Completed(ResponseResult),
    /// Service instance is currently not invoked
    #[default]
    Free,
}

impl InvocationStatus {
    #[inline]
    pub fn service_id(&self) -> Option<ServiceId> {
        match self {
            InvocationStatus::Inboxed(metadata) => Some(metadata.service_id.clone()),
            InvocationStatus::Invoked(metadata) => Some(metadata.service_id.clone()),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata.service_id.clone()),
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
    pub fn get_timestamps(&self) -> Option<&StatusTimestamps> {
        match self {
            InvocationStatus::Inboxed(metadata) => Some(&metadata.timestamps),
            InvocationStatus::Invoked(metadata) => Some(&metadata.timestamps),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.timestamps),
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
pub struct InboxedInvocationMetadata {
    pub inbox_sequence_number: u64,
    pub response_sinks: HashSet<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,

    // --- From ServiceInvocation
    // This field and handler_name will be part of a single id with https://github.com/restatedev/restate/issues/1329
    pub service_id: ServiceId,
    pub handler_name: ByteString,

    // Could be split out of ServiceInvocation, e.g. InvocationContent or similar.
    pub argument: Bytes,
    pub source: Source,
    pub span_context: ServiceInvocationSpanContext,
    pub headers: Vec<Header>,
    /// Time when the request should be executed
    pub execution_time: Option<MillisSinceEpoch>,
}

impl InboxedInvocationMetadata {
    // TODO add method from ServiceInvocation and seq_number

    pub fn append_response_sink(&mut self, new_sink: ServiceInvocationResponseSink) {
        self.response_sinks.insert(new_sink);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InFlightInvocationMetadata {
    pub service_id: ServiceId,
    pub journal_metadata: JournalMetadata,
    pub deployment_id: Option<DeploymentId>,
    pub method: ByteString,
    pub response_sinks: HashSet<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,
    pub source: Source,
    /// If zero, the invocation completion will not be retained.
    pub completion_retention_time: Duration,
    pub idempotency_key: Option<ByteString>,
}

impl InFlightInvocationMetadata {
    // TODO add methods from ServiceInvocation and from InboxedInvocationMetadata,
    //  remove this big new method
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::mutable_key_type)]
    pub fn new(
        service_id: ServiceId,
        journal_metadata: JournalMetadata,
        deployment_id: Option<DeploymentId>,
        method: ByteString,
        response_sinks: HashSet<ServiceInvocationResponseSink>,
        timestamps: StatusTimestamps,
        source: Source,
        completion_retention_time: Duration,
        idempotency_key: Option<ByteString>,
    ) -> Self {
        Self {
            service_id,
            journal_metadata,
            deployment_id,
            method,
            response_sinks,
            timestamps,
            source,
            completion_retention_time,
            idempotency_key,
        }
    }

    pub fn append_response_sink(&mut self, new_sink: ServiceInvocationResponseSink) {
        self.response_sinks.insert(new_sink);
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
    ) -> impl Stream<Item = Result<FullInvocationId>> + Send;
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

    impl InFlightInvocationMetadata {
        pub fn mock() -> Self {
            InFlightInvocationMetadata {
                service_id: ServiceId::new("MyService", "MyKey"),
                journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
                deployment_id: None,
                method: ByteString::from("mock"),
                response_sinks: HashSet::new(),
                timestamps: StatusTimestamps::now(),
                source: Source::Ingress,
                completion_retention_time: Duration::ZERO,
                idempotency_key: None,
            }
        }
    }
}
