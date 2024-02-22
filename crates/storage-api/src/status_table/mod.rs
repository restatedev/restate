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
    DeploymentId, EntryIndex, FullInvocationId, InvocationUuid, PartitionKey, ServiceId,
};
use restate_types::invocation::{
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::future::Future;
use std::ops::RangeInclusive;

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

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NotificationTarget {
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
    Virtual {
        invocation_uuid: InvocationUuid,
        journal_metadata: JournalMetadata,
        timestamps: StatusTimestamps,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
    },
    /// Service instance is currently not invoked
    #[default]
    Free,
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

    #[inline]
    pub fn get_timestamps(&self) -> Option<&StatusTimestamps> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(&metadata.timestamps),
            InvocationStatus::Suspended { metadata, .. } => Some(&metadata.timestamps),
            InvocationStatus::Free => None,
            InvocationStatus::Virtual { timestamps, .. } => Some(timestamps),
        }
    }

    pub fn update_timestamps(&mut self) {
        match self {
            InvocationStatus::Invoked(metadata) => metadata.timestamps.update(),
            InvocationStatus::Suspended { metadata, .. } => metadata.timestamps.update(),
            InvocationStatus::Virtual { timestamps, .. } => timestamps.update(),
            InvocationStatus::Free => {}
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
    pub deployment_id: Option<DeploymentId>,
    pub method: ByteString,
    pub response_sink: Option<ServiceInvocationResponseSink>,
    pub timestamps: StatusTimestamps,
    pub source: Source,
}

impl InvocationMetadata {
    pub fn new(
        invocation_uuid: InvocationUuid,
        journal_metadata: JournalMetadata,
        deployment_id: Option<DeploymentId>,
        method: ByteString,
        response_sink: Option<ServiceInvocationResponseSink>,
        timestamps: StatusTimestamps,
        source: Source,
    ) -> Self {
        Self {
            invocation_uuid,
            journal_metadata,
            deployment_id,
            method,
            response_sink,
            timestamps,
            source,
        }
    }
}

pub trait StatusTable: ReadOnlyStatusTable {
    fn put_invocation_status(
        &mut self,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> impl Future<Output = ()> + Send;

    fn delete_invocation_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = ()> + Send;
}

pub trait ReadOnlyStatusTable {
    fn get_invocation_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = Result<Option<InvocationStatus>>> + Send;

    fn get_invocation_status_from(
        &mut self,
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
    ) -> impl Future<Output = Result<Option<(ServiceId, InvocationStatus)>>> + Send;

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<FullInvocationId>> + Send;
}

#[cfg(any(test, feature = "mocks"))]
mod mocks {
    use super::*;

    impl InvocationMetadata {
        pub fn mock() -> Self {
            InvocationMetadata {
                invocation_uuid: InvocationUuid::new(),
                journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
                deployment_id: None,
                method: ByteString::from("mock"),
                response_sink: None,
                timestamps: StatusTimestamps::now(),
                source: Source::Ingress,
            }
        }
    }
}
