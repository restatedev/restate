// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use prost::Message;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::timer_table::Timer;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::identifiers::{FullInvocationId, InvocationUuid, ServiceId};
use restate_types::invocation::{
    InvocationResponse, MaybeFullInvocationId, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, Source, SpanRelation,
};
use restate_types::time::MillisSinceEpoch;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

pub(crate) type InvokerEffect = restate_invoker_api::Effect;
pub(crate) type InvokerEffectKind = restate_invoker_api::EffectKind;

#[derive(Debug, Clone)]
pub struct TimerValue {
    pub invocation_uuid: InvocationUuid,
    pub wake_up_time: MillisSinceEpoch,
    pub entry_index: EntryIndex,
    pub value: Timer,
}

impl TimerValue {
    pub(crate) fn new_sleep(
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> Self {
        Self {
            invocation_uuid: full_invocation_id.invocation_uuid,
            wake_up_time,
            entry_index,
            value: Timer::CompleteSleepEntry(full_invocation_id.service_id),
        }
    }

    pub(crate) fn new_invoke(
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        service_invocation: ServiceInvocation,
    ) -> Self {
        Self {
            invocation_uuid: full_invocation_id.invocation_uuid,
            wake_up_time,
            entry_index,
            value: Timer::Invoke(full_invocation_id.service_id, service_invocation),
        }
    }

    pub(crate) fn display_key(&self) -> TimerKeyDisplay {
        return TimerKeyDisplay {
            service_id: self.value.service_id(),
            invocation_uuid: &self.invocation_uuid,
            entry_index: self.entry_index,
        };
    }
}

impl Hash for TimerValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.invocation_uuid, state);
        Hash::hash(&self.wake_up_time, state);
        Hash::hash(&self.entry_index, state);
        // We don't hash the value field.
    }
}

impl PartialEq for TimerValue {
    fn eq(&self, other: &Self) -> bool {
        self.invocation_uuid == other.invocation_uuid
            && self.wake_up_time == other.wake_up_time
            && self.entry_index == other.entry_index
    }
}

impl Eq for TimerValue {}

impl PartialOrd for TimerValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// We use the TimerKey to read the timers in an absolute order. The timer service
// relies on this order in order to process each timer exactly once. That is the
// reason why the ordering of the TimerValue and how the TimerKey is laid out in
// RocksDB need to be exactly the same.
//
// TODO: https://github.com/restatedev/restate/issues/394
impl Ord for TimerValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wake_up_time
            .cmp(&other.wake_up_time)
            .then_with(|| self.invocation_uuid.cmp(&other.invocation_uuid))
            .then_with(|| self.entry_index.cmp(&other.entry_index))
    }
}

impl restate_timer::Timer for TimerValue {
    type TimerKey = TimerValue;

    fn timer_key(&self) -> Self::TimerKey {
        self.clone()
    }
}

impl restate_timer::TimerKey for TimerValue {
    fn wake_up_time(&self) -> MillisSinceEpoch {
        self.wake_up_time
    }
}

// Helper to display timer key
#[derive(Debug)]
pub(crate) struct TimerKeyDisplay<'a> {
    pub(crate) service_id: &'a ServiceId,
    pub(crate) invocation_uuid: &'a InvocationUuid,
    pub(crate) entry_index: EntryIndex,
}

impl<'a> fmt::Display for TimerKeyDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{:?}][{}]({})",
            self.service_id.service_name,
            self.service_id.key,
            self.invocation_uuid,
            self.entry_index
        )
    }
}

// Extension methods to the OutboxMessage type
pub(crate) trait OutboxMessageExt {
    fn from_response_sink(
        callee: &FullInvocationId,
        response_sink: ServiceInvocationResponseSink,
        result: ResponseResult,
    ) -> OutboxMessage;

    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage;
}

impl OutboxMessageExt for OutboxMessage {
    fn from_response_sink(
        callee: &FullInvocationId,
        response_sink: ServiceInvocationResponseSink,
        result: ResponseResult,
    ) -> OutboxMessage {
        match response_sink {
            ServiceInvocationResponseSink::PartitionProcessor {
                entry_index,
                caller,
            } => OutboxMessage::ServiceResponse(InvocationResponse {
                id: MaybeFullInvocationId::Full(caller),
                entry_index,
                result,
            }),
            ServiceInvocationResponseSink::Ingress(ingress_dispatcher_id) => {
                OutboxMessage::IngressResponse {
                    ingress_dispatcher_id,
                    full_invocation_id: callee.clone(),
                    response: result,
                }
            }
            ServiceInvocationResponseSink::NewInvocation {
                target,
                method,
                caller_context,
            } => {
                OutboxMessage::ServiceInvocation(ServiceInvocation::new(
                    target,
                    method,
                    // Methods receiving responses MUST accept this input type
                    restate_pb::restate::internal::ServiceInvocationSinkRequest {
                        response: Some(result.into()),
                        caller_context,
                    }
                    .encode_to_vec(),
                    Source::Service(callee.clone()),
                    None,
                    SpanRelation::None,
                ))
            }
        }
    }

    fn from_awakeable_completion(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        result: ResponseResult,
    ) -> OutboxMessage {
        OutboxMessage::ServiceResponse(InvocationResponse {
            entry_index,
            result,
            id: MaybeFullInvocationId::Partial(invocation_id),
        })
    }
}
