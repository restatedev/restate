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
use restate_types::identifiers::{
    EntryIndex, FullInvocationId, InvocationUuid, PartitionKey, ServiceId,
};
use restate_types::invocation::ServiceInvocationResponseSink;
use restate_types::journal::JournalMetadata;
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::ops::RangeInclusive;

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
}

impl InvocationStatus {
    #[inline]
    pub fn invocation_uuid(&self) -> Option<InvocationUuid> {
        match self {
            InvocationStatus::Invoked(metadata) => Some(metadata.invocation_uuid),
            InvocationStatus::Suspended { metadata, .. } => Some(metadata.invocation_uuid),
            InvocationStatus::Free => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InvocationMetadata {
    pub invocation_uuid: InvocationUuid,
    pub journal_metadata: JournalMetadata,
    pub response_sink: Option<ServiceInvocationResponseSink>,
    pub creation_time: MillisSinceEpoch,
    pub modification_time: MillisSinceEpoch,
}

impl InvocationMetadata {
    pub fn new(
        invocation_id: InvocationUuid,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        creation_time: MillisSinceEpoch,
        modification_time: MillisSinceEpoch,
    ) -> Self {
        Self {
            invocation_uuid: invocation_id,
            journal_metadata,
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
