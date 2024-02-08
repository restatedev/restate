// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use bytes::Bytes;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::status_table::NotificationTarget;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{EntryIndex, FullInvocationId, InvocationUuid, ServiceId};
use restate_types::invocation::{
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source as InvocationSource,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::time::MillisSinceEpoch;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BuiltinServiceEffects {
    full_invocation_id: FullInvocationId,
    effects: Vec<BuiltinServiceEffect>,
}

impl BuiltinServiceEffects {
    pub fn new(full_invocation_id: FullInvocationId, effects: Vec<BuiltinServiceEffect>) -> Self {
        Self {
            full_invocation_id,
            effects,
        }
    }

    pub fn into_inner(self) -> (FullInvocationId, Vec<BuiltinServiceEffect>) {
        (self.full_invocation_id, self.effects)
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum BuiltinServiceEffect {
    CreateJournal {
        service_id: ServiceId,
        invocation_uuid: InvocationUuid,
        span_context: ServiceInvocationSpanContext,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
    },
    StoreEntry {
        service_id: ServiceId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    },
    DropJournal {
        service_id: ServiceId,
        journal_length: EntryIndex,
    },

    SetState {
        key: Cow<'static, str>,
        value: Bytes,
    },
    ClearState(Cow<'static, str>),

    OutboxMessage(OutboxMessage),
    DelayedInvoke {
        target_fid: FullInvocationId,
        target_method: String,
        argument: Bytes,
        source: InvocationSource,
        response_sink: Option<ServiceInvocationResponseSink>,
        time: MillisSinceEpoch,
        timer_index: EntryIndex,
    },

    End(
        // NBIS can optionally fail, depending on the context the error might or might not be used.
        Option<InvocationError>,
    ),
}
