// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetFuture, GetStream, PutFuture};
use bytestring::ByteString;
use restate_types::identifiers::{
    EndpointId, EntryIndex, FullInvocationId, InvocationUuid, PartitionKey, ServiceId,
};
use restate_types::invocation::{ServiceInvocationResponseSink, ServiceInvocationSpanContext};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::ops::RangeInclusive;

#[derive(Debug, Clone, PartialEq)]
pub struct CompletionNotificationTarget {
    pub service: ServiceId,
    pub method: String,
}

/// Status of a service instance.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum InvocationStatus {
    Invoked(InvocationMetadata),
    Suspended {
        metadata: InvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    /// Service instance is currently not invoked
    #[default]
    Free,
    Virtual {
        invocation_uuid: InvocationUuid,
        journal_metadata: JournalMetadata,
        completion_notification_target: CompletionNotificationTarget,
    },
}

impl InvocationStatus {
    #[inline]
    pub fn invocation_uuid(&self) -> Option<InvocationUuid> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(metadata.invocation_uuid),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata.invocation_uuid),
            InvocationStatus::Virtual {
                invocation_uuid, ..
            } => Some(*invocation_uuid),
            InvocationStatus::Free => None,
        }
    }

    #[inline]
    pub fn into_journal_metadata(self) -> Option<JournalMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(metadata.journal_metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata.journal_metadata),
            InvocationStatus::Free => None,
            InvocationStatus::Virtual {
                journal_metadata, ..
            } => Some(journal_metadata),
        }
    }

    #[inline]
    pub fn get_journal_metadata(&self) -> Option<&JournalMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(&metadata.journal_metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.journal_metadata),
            InvocationStatus::Free => None,
            InvocationStatus::Virtual {
                journal_metadata, ..
            } => Some(journal_metadata),
        }
    }

    #[inline]
    pub fn get_journal_metadata_mut(&mut self) -> Option<&mut JournalMetadata> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(&mut metadata.journal_metadata),
            InvocationStatus::Suspended { metadata, .. } => Some(&mut metadata.journal_metadata),
            InvocationStatus::Free => None,
            InvocationStatus::Virtual {
                journal_metadata, ..
            } => Some(journal_metadata),
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

#[derive(Debug, Clone, PartialEq)]
pub struct InvocationMetadata {
    pub invocation_uuid: InvocationUuid,
    pub journal_metadata: JournalMetadata,
    pub endpoint_id: Option<EndpointId>,
    pub method: ByteString,
    pub response_sink: Option<ServiceInvocationResponseSink>,
    pub creation_time: MillisSinceEpoch,
    pub modification_time: MillisSinceEpoch,
}

impl InvocationMetadata {
    pub fn new(
        invocation_uuid: InvocationUuid,
        journal_metadata: JournalMetadata,
        endpoint_id: Option<String>,
        method: ByteString,
        response_sink: Option<ServiceInvocationResponseSink>,
        creation_time: MillisSinceEpoch,
        modification_time: MillisSinceEpoch,
    ) -> Self {
        Self {
            invocation_uuid,
            journal_metadata,
            endpoint_id,
            method,
            response_sink,
            creation_time,
            modification_time,
        }
    }
}

pub trait StatusTable {
    fn put_invocation_status(
        &mut self,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> PutFuture;

    fn get_invocation_status(
        &mut self,
        service_id: &ServiceId,
    ) -> GetFuture<Option<InvocationStatus>>;

    fn get_invocation_status_from(
        &mut self,
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
    ) -> GetFuture<Option<(ServiceId, InvocationStatus)>>;

    fn delete_invocation_status(&mut self, service_id: &ServiceId) -> PutFuture;

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> GetStream<FullInvocationId>;
}
