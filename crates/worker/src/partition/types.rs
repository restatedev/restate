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
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::{EntryIndex, InvocationId};
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
    pub full_invocation_id: FullInvocationId,
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
        let sid = Clone::clone(&full_invocation_id.service_id);
        Self {
            full_invocation_id,
            wake_up_time,
            entry_index,
            value: Timer::CompleteSleepEntry(sid),
        }
    }

    pub(crate) fn new_invoke(
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        service_invocation: ServiceInvocation,
    ) -> Self {
        let sid = Clone::clone(&full_invocation_id.service_id);
        Self {
            full_invocation_id,
            wake_up_time,
            entry_index,
            value: Timer::Invoke(sid.clone(), service_invocation),
        }
    }

    pub(crate) fn display_key(&self) -> TimerKeyDisplay {
        return TimerKeyDisplay(&self.full_invocation_id, &self.entry_index);
    }
}

impl Hash for TimerValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.full_invocation_id, state);
        Hash::hash(&self.wake_up_time, state);
        Hash::hash(&self.entry_index, state);
        // We don't hash the value field.
    }
}

impl PartialEq for TimerValue {
    fn eq(&self, other: &Self) -> bool {
        self.full_invocation_id == other.full_invocation_id
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
            .then_with(|| {
                let service_id = &self.full_invocation_id.service_id;
                let invocation_id = &self.full_invocation_id.invocation_uuid;

                let other_service_id = &other.full_invocation_id.service_id;
                let other_invocation_id = &other.full_invocation_id.invocation_uuid;

                service_id
                    .service_name
                    .cmp(&other_service_id.service_name)
                    .then_with(|| service_id.key.cmp(&other_service_id.key))
                    .then_with(|| invocation_id.cmp(other_invocation_id))
            })
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
pub(crate) struct TimerKeyDisplay<'a>(pub(crate) &'a FullInvocationId, pub(crate) &'a EntryIndex);

impl<'a> fmt::Display for TimerKeyDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{:?}][{}]({})",
            self.0.service_id.service_name, self.0.service_id.key, self.0.invocation_uuid, self.1
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
