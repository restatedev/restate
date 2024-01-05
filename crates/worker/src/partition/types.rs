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
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::{EntryIndex, InvocationId, WithPartitionKey};
use restate_types::invocation::{
    InvocationResponse, MaybeFullInvocationId, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, Source, SpanRelation,
};
use restate_types::time::MillisSinceEpoch;
use std::fmt;
use std::hash::{Hash, Hasher};

pub(crate) type InvokerEffect = restate_invoker_api::Effect;
pub(crate) type InvokerEffectKind = restate_invoker_api::EffectKind;

#[derive(Debug, Clone)]
pub struct TimerValue {
    timer_key: TimerKeyWrapper,
    value: Timer,
}

impl TimerValue {
    pub fn new(timer_key: TimerKey, value: Timer) -> Self {
        Self {
            timer_key: TimerKeyWrapper(timer_key),
            value,
        }
    }

    pub(crate) fn new_sleep(
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> Self {
        let timer_key = TimerKeyWrapper(TimerKey {
            invocation_uuid: full_invocation_id.invocation_uuid,
            timestamp: wake_up_time.as_u64(),
            journal_index: entry_index,
        });

        Self {
            timer_key,
            value: Timer::CompleteSleepEntry(full_invocation_id.service_id),
        }
    }

    pub(crate) fn new_invoke(
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        service_invocation: ServiceInvocation,
    ) -> Self {
        let timer_key = TimerKeyWrapper(TimerKey {
            invocation_uuid: full_invocation_id.invocation_uuid,
            timestamp: wake_up_time.as_u64(),
            journal_index: entry_index,
        });

        Self {
            timer_key,
            value: Timer::Invoke(full_invocation_id.service_id, service_invocation),
        }
    }

    pub fn into_inner(self) -> (TimerKey, Timer) {
        (self.timer_key.0, self.value)
    }

    pub fn key(&self) -> &TimerKey {
        &self.timer_key.0
    }

    pub fn value(&self) -> &Timer {
        &self.value
    }

    pub fn invocation_id(&self) -> InvocationId {
        InvocationId::new(
            self.value.service_id().partition_key(),
            self.timer_key.0.invocation_uuid,
        )
    }

    pub fn wake_up_time(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::from(self.timer_key.0.timestamp)
    }
}

impl Hash for TimerValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.timer_key, state);
        // We don't hash the value field.
    }
}

impl PartialEq for TimerValue {
    fn eq(&self, other: &Self) -> bool {
        self.timer_key == other.timer_key
    }
}

impl Eq for TimerValue {}

/// New type wrapper to implement [`restate_timer::TimerKey`] for [`TimerKey`].
///
/// # Important
/// We use the [`TimerKey`] to read the timers in an absolute order. The timer service
/// relies on this order in order to process each timer exactly once. That is the
/// reason why the in-memory and in-rocksdb ordering of the TimerKey needs to be exactly
/// the same.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerKeyWrapper(TimerKey);

impl TimerKeyWrapper {
    pub fn into_inner(self) -> TimerKey {
        self.0
    }
}

impl restate_timer::Timer for TimerValue {
    type TimerKey = TimerKeyWrapper;

    fn timer_key(&self) -> Self::TimerKey {
        self.timer_key.clone()
    }
}

impl restate_timer::TimerKey for TimerKeyWrapper {
    fn wake_up_time(&self) -> MillisSinceEpoch {
        MillisSinceEpoch::from(self.0.timestamp)
    }
}

impl fmt::Display for TimerKeyWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", TimerKeyDisplay(&self.0))
    }
}

// Helper to display timer key
#[derive(Debug)]
pub(crate) struct TimerKeyDisplay<'a>(pub &'a TimerKey);

impl<'a> fmt::Display for TimerKeyDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]({})", self.0.invocation_uuid, self.0.journal_index)
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
