// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TODO(slinkydeveloper) this file will need a cleanup when we remove protocol version <= 3

use crate::errors::InvocationError;
use std::ops::RangeInclusive;

pub const MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION: ServiceProtocolVersion =
    ServiceProtocolVersion::V1;
pub const MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION: ServiceProtocolVersion =
    ServiceProtocolVersion::V6;

pub const MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION: ServiceProtocolVersion =
    ServiceProtocolVersion::V5;
pub const MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION: ServiceProtocolVersion =
    MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION;

pub const MAX_SERVICE_PROTOCOL_VERSION_VALUE: i32 = i32::MAX;

include!(concat!(env!("OUT_DIR"), "/dev.restate.service.protocol.rs"));

impl ServiceProtocolVersion {
    pub fn as_repr(&self) -> i32 {
        i32::from(*self)
    }

    pub fn is_acceptable_for_discovery(min_version: i32, max_version: i32) -> bool {
        min_version <= i32::from(MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION)
            && max_version >= i32::from(MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION)
    }

    pub fn is_supported_for_inflight_invocation(&self) -> bool {
        MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION <= *self
            && *self <= MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION
    }

    /// Pick the version to use for running an invocation
    pub fn pick(
        deployment_supported_versions: &RangeInclusive<i32>,
    ) -> Option<ServiceProtocolVersion> {
        if *deployment_supported_versions.start()
            <= i32::from(MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION)
            && *deployment_supported_versions.end()
                >= i32::from(MIN_INFLIGHT_SERVICE_PROTOCOL_VERSION)
        {
            ServiceProtocolVersion::try_from(std::cmp::min(
                *deployment_supported_versions.end(),
                i32::from(MAX_INFLIGHT_SERVICE_PROTOCOL_VERSION),
            ))
            .ok()
        } else {
            None
        }
    }
}

impl From<ErrorMessage> for InvocationError {
    fn from(value: ErrorMessage) -> Self {
        if value.description.is_empty() {
            InvocationError::new(value.code, value.message)
        } else {
            InvocationError::new(value.code, value.message).with_stacktrace(value.description)
        }
    }
}

impl From<Header> for crate::invocation::Header {
    fn from(value: Header) -> Self {
        Self::new(value.key, value.value)
    }
}

impl From<crate::invocation::Header> for Header {
    fn from(value: crate::invocation::Header) -> Self {
        Self {
            key: value.name.into(),
            value: value.value.into(),
        }
    }
}

/// This module implements conversions back and forth from proto messages to [`journal::Entry`] model.
/// These are used by the [`codec::ProtobufRawEntryCodec`].
mod pb_into {
    use super::*;
    use crate::identifiers::{IdempotencyId, ServiceId};

    use crate::journal::{
        AttachInvocationEntry, AttachInvocationTarget, AwakeableEntry, CancelInvocationEntry,
        CancelInvocationTarget, ClearStateEntry, CompleteAwakeableEntry, CompletePromiseEntry,
        CompleteResult, CompletionResult, Entry, EntryResult, GetCallInvocationIdEntry,
        GetCallInvocationIdResult, GetInvocationOutputEntry, GetPromiseEntry, GetStateEntry,
        GetStateKeysEntry, GetStateKeysResult, InputEntry, InvokeEntry, InvokeRequest,
        OneWayCallEntry, OutputEntry, PeekPromiseEntry, RunEntry, SetStateEntry, SleepEntry,
        SleepResult,
    };

    impl TryFrom<InputEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: InputEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Input(InputEntry {
                value: msg.value,
                headers: msg.headers.into_iter().map(Into::into).collect(),
            }))
        }
    }

    impl TryFrom<OutputEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: OutputEntryMessage) -> Result<Self, Self::Error> {
            Ok(Entry::Output(OutputEntry {
                result: match msg.result.ok_or("result")? {
                    output_entry_message::Result::Value(r) => EntryResult::Success(r),
                    output_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                },
            }))
        }
    }

    impl TryFrom<GetStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: GetStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::GetState(GetStateEntry {
                key: msg.key,
                value: msg.result.map(|v| match v {
                    get_state_entry_message::Result::Empty(_) => CompletionResult::Empty,
                    get_state_entry_message::Result::Value(b) => CompletionResult::Success(b),
                    get_state_entry_message::Result::Failure(failure) => {
                        CompletionResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<SetStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: SetStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::SetState(SetStateEntry {
                key: msg.key,
                value: msg.value,
            }))
        }
    }

    impl TryFrom<ClearStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: ClearStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::ClearState(ClearStateEntry { key: msg.key }))
        }
    }

    impl TryFrom<GetStateKeysEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: GetStateKeysEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::GetStateKeys(GetStateKeysEntry {
                value: msg.result.map(|v| match v {
                    get_state_keys_entry_message::Result::Value(b) => {
                        GetStateKeysResult::Result(b.keys)
                    }
                    get_state_keys_entry_message::Result::Failure(failure) => {
                        GetStateKeysResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<ClearAllStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(_: ClearAllStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::ClearAllState)
        }
    }

    impl TryFrom<GetPromiseEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: GetPromiseEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::GetPromise(GetPromiseEntry {
                key: msg.key.into(),
                value: msg.result.map(|v| match v {
                    get_promise_entry_message::Result::Value(b) => EntryResult::Success(b),
                    get_promise_entry_message::Result::Failure(failure) => {
                        EntryResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<PeekPromiseEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: PeekPromiseEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::PeekPromise(PeekPromiseEntry {
                key: msg.key.into(),
                value: msg.result.map(|v| match v {
                    peek_promise_entry_message::Result::Empty(_) => CompletionResult::Empty,
                    peek_promise_entry_message::Result::Value(b) => CompletionResult::Success(b),
                    peek_promise_entry_message::Result::Failure(failure) => {
                        CompletionResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<CompletePromiseEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: CompletePromiseEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::CompletePromise(CompletePromiseEntry {
                key: msg.key.into(),
                completion: match msg.completion.ok_or("completion")? {
                    complete_promise_entry_message::Completion::CompletionValue(b) => {
                        EntryResult::Success(b)
                    }
                    complete_promise_entry_message::Completion::CompletionFailure(failure) => {
                        EntryResult::Failure(failure.code.into(), failure.message.into())
                    }
                },
                value: msg.result.map(|v| match v {
                    complete_promise_entry_message::Result::Empty(_) => CompleteResult::Done,
                    complete_promise_entry_message::Result::Failure(failure) => {
                        CompleteResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<SleepEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: SleepEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Sleep(SleepEntry {
                wake_up_time: msg.wake_up_time,
                result: msg.result.map(|r| match r {
                    sleep_entry_message::Result::Empty(_) => SleepResult::Fired,
                    sleep_entry_message::Result::Failure(failure) => {
                        SleepResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<CallEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: CallEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Call(InvokeEntry {
                request: InvokeRequest {
                    service_name: msg.service_name.into(),
                    handler_name: msg.handler_name.into(),
                    parameter: msg.parameter,
                    headers: msg.headers.into_iter().map(Into::into).collect(),
                    key: msg.key.into(),
                    idempotency_key: msg.idempotency_key.map(|k| k.into()),
                },
                result: msg.result.map(|v| match v {
                    call_entry_message::Result::Value(r) => EntryResult::Success(r),
                    call_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<OneWayCallEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: OneWayCallEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::OneWayCall(OneWayCallEntry {
                request: InvokeRequest {
                    service_name: msg.service_name.into(),
                    handler_name: msg.handler_name.into(),
                    parameter: msg.parameter,
                    headers: msg.headers.into_iter().map(Into::into).collect(),
                    key: msg.key.into(),
                    idempotency_key: msg.idempotency_key.map(|k| k.into()),
                },
                invoke_time: msg.invoke_time,
            }))
        }
    }

    impl TryFrom<AwakeableEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: AwakeableEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Awakeable(AwakeableEntry {
                result: msg.result.map(|v| match v {
                    awakeable_entry_message::Result::Value(r) => EntryResult::Success(r),
                    awakeable_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<CompleteAwakeableEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: CompleteAwakeableEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::CompleteAwakeable(CompleteAwakeableEntry {
                id: msg.id.into(),
                result: match msg.result.ok_or("result")? {
                    complete_awakeable_entry_message::Result::Value(r) => EntryResult::Success(r),
                    complete_awakeable_entry_message::Result::Failure(Failure {
                        code,
                        message,
                    }) => EntryResult::Failure(code.into(), message.into()),
                },
            }))
        }
    }

    impl TryFrom<RunEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: RunEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Run(RunEntry {
                result: match msg.result.ok_or("result")? {
                    run_entry_message::Result::Value(r) => EntryResult::Success(r),
                    run_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                },
            }))
        }
    }

    impl TryFrom<CancelInvocationEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: CancelInvocationEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::CancelInvocation(CancelInvocationEntry {
                target: match msg.target.ok_or("target")? {
                    cancel_invocation_entry_message::Target::InvocationId(s) => {
                        CancelInvocationTarget::InvocationId(s.into())
                    }
                    cancel_invocation_entry_message::Target::CallEntryIndex(i) => {
                        CancelInvocationTarget::CallEntryIndex(i)
                    }
                },
            }))
        }
    }

    impl TryFrom<GetCallInvocationIdEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: GetCallInvocationIdEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::GetCallInvocationId(GetCallInvocationIdEntry {
                call_entry_index: msg.call_entry_index,
                result: msg.result.map(|v| match v {
                    get_call_invocation_id_entry_message::Result::Value(r) => {
                        GetCallInvocationIdResult::InvocationId(r)
                    }
                    get_call_invocation_id_entry_message::Result::Failure(Failure {
                        code,
                        message,
                    }) => GetCallInvocationIdResult::Failure(code.into(), message.into()),
                }),
            }))
        }
    }

    impl TryFrom<AttachInvocationEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(
            AttachInvocationEntryMessage { target, result, .. }: AttachInvocationEntryMessage,
        ) -> Result<Self, Self::Error> {
            Ok(Self::AttachInvocation(AttachInvocationEntry {
                target: match target.ok_or("target")? {
                    attach_invocation_entry_message::Target::InvocationId(id) => {
                        AttachInvocationTarget::InvocationId(id.into())
                    }
                    attach_invocation_entry_message::Target::CallEntryIndex(idx) => {
                        AttachInvocationTarget::CallEntryIndex(idx)
                    }
                    attach_invocation_entry_message::Target::IdempotentRequestTarget(id) => {
                        AttachInvocationTarget::IdempotentRequest(id.into())
                    }
                    attach_invocation_entry_message::Target::WorkflowTarget(id) => {
                        AttachInvocationTarget::Workflow(id.into())
                    }
                },
                result: result.map(|v| match v {
                    attach_invocation_entry_message::Result::Value(r) => EntryResult::Success(r),
                    attach_invocation_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<GetInvocationOutputEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(
            GetInvocationOutputEntryMessage { target, result, .. }: GetInvocationOutputEntryMessage,
        ) -> Result<Self, Self::Error> {
            Ok(Self::GetInvocationOutput(GetInvocationOutputEntry {
                target: match target.ok_or("target")? {
                    get_invocation_output_entry_message::Target::InvocationId(id) => {
                        AttachInvocationTarget::InvocationId(id.into())
                    }
                    get_invocation_output_entry_message::Target::CallEntryIndex(idx) => {
                        AttachInvocationTarget::CallEntryIndex(idx)
                    }
                    get_invocation_output_entry_message::Target::IdempotentRequestTarget(id) => {
                        AttachInvocationTarget::IdempotentRequest(id.into())
                    }
                    get_invocation_output_entry_message::Target::WorkflowTarget(id) => {
                        AttachInvocationTarget::Workflow(id.into())
                    }
                },
                result: result.map(|v| match v {
                    get_invocation_output_entry_message::Result::Empty(_) => {
                        CompletionResult::Empty
                    }
                    get_invocation_output_entry_message::Result::Value(r) => {
                        CompletionResult::Success(r)
                    }
                    get_invocation_output_entry_message::Result::Failure(Failure {
                        code,
                        message,
                    }) => CompletionResult::Failure(code.into(), message.into()),
                }),
            }))
        }
    }

    impl From<IdempotentRequestTarget> for IdempotencyId {
        fn from(
            IdempotentRequestTarget {
                service_name,
                service_key,
                handler_name,
                idempotency_key,
            }: IdempotentRequestTarget,
        ) -> Self {
            IdempotencyId::new(
                service_name.into(),
                service_key.map(Into::into),
                handler_name.into(),
                idempotency_key.into(),
            )
        }
    }

    impl From<WorkflowTarget> for ServiceId {
        fn from(
            WorkflowTarget {
                workflow_name,
                workflow_key,
            }: WorkflowTarget,
        ) -> Self {
            ServiceId::new(workflow_name, workflow_key)
        }
    }
}
