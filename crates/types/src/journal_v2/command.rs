// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::{IdempotencyId, InvocationId, ServiceId};
use crate::invocation::{Header, InvocationQuery, InvocationTarget, ServiceInvocationSpanContext};
use crate::journal_v2::raw::{RawEntry, TryFromEntry, TryFromEntryError};
use crate::journal_v2::{
    CompletionId, Encoder, Entry, EntryMetadata, EntryType, Failure, GetStateResult, SignalId,
    SignalResult,
};
use crate::time::MillisSinceEpoch;
use bytes::Bytes;
use bytestring::ByteString;
use enum_dispatch::enum_dispatch;
use std::fmt;
use std::time::Duration;

#[enum_dispatch(EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(CommandType))]
#[strum_discriminants(derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    Input(InputCommand),
    Output(OutputCommand),
    GetLazyState(GetLazyStateCommand),
    SetState(SetStateCommand),
    ClearState(ClearStateCommand),
    ClearAllState(ClearAllStateCommand),
    GetLazyStateKeys(GetLazyStateKeysCommand),
    GetEagerState(GetEagerStateCommand),
    GetEagerStateKeys(GetEagerStateKeysCommand),
    GetPromise(GetPromiseCommand),
    PeekPromise(PeekPromiseCommand),
    CompletePromise(CompletePromiseCommand),
    Sleep(SleepCommand),
    Call(CallCommand),
    OneWayCall(OneWayCallCommand),
    SendNotification(SendNotificationCommand),
    Run(RunCommand),
    AttachInvocation(AttachInvocationCommand),
    GetInvocationOutput(GetInvocationOutputCommand),
}

impl Command {
    pub fn encode<E: Encoder>(&self) -> RawEntry {
        E::encode_entry(&Entry::Command(self.clone()))
    }
}

impl fmt::Display for CommandType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

// Little macro to reduce boilerplate for TryFromEntry and EntryMetadata.
macro_rules! impl_command_accessors {
    ($ty:ident -> []) => {
        // End of macro
    };
    ($ty:ident -> [@from_entry $($tail:tt)*]) => {
        impl TryFromEntry for paste::paste! { [< $ty Command >] } {
            fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
                match entry {
                    Entry::Command(Command::$ty(e)) => Ok(e),
                    e => Err(TryFromEntryError {
                        expected: EntryType::Command(CommandType::$ty),
                        actual: e.ty(),
                    }),
                }
            }
        }
        impl From<paste::paste! { [< $ty Command >] }> for Entry {
            fn from(v: paste::paste! { [< $ty Command >] }) -> Self {
                Self::Command(v.into())
            }
        }
        impl_command_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@metadata $($tail:tt)*]) => {
        impl EntryMetadata for paste::paste! { [< $ty Command >] } {
            fn ty(&self) -> EntryType {
                EntryType::Command(CommandType::$ty)
            }
        }
        impl_command_accessors!($ty -> [$($tail)*]);
    };

    // Entrypoints of the macro
    ($ty:ident: [$($tokens:tt)*]) => {
        impl_command_accessors!($ty -> [$($tokens)*]);
    };
    ($ty:ident) => {
        impl_command_accessors!($ty -> [@metadata @from_entry]);
    };
}

// --- Actual implementation of individual commands

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputCommand {
    pub headers: Vec<Header>,
    pub payload: Bytes,
    pub name: ByteString,
}
impl_command_accessors!(Input);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputCommand {
    pub result: OutputResult,
    pub name: ByteString,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputResult {
    Success(Bytes),
    Failure(Failure),
}
impl_command_accessors!(Output);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLazyStateCommand {
    pub key: ByteString,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetLazyState);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetStateCommand {
    pub key: ByteString,
    pub value: Bytes,
    pub name: ByteString,
}
impl_command_accessors!(SetState);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearStateCommand {
    pub key: ByteString,
    pub name: ByteString,
}
impl_command_accessors!(ClearState);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearAllStateCommand {
    pub name: ByteString,
}
impl_command_accessors!(ClearAllState);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetLazyStateKeysCommand {
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetLazyStateKeys);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetEagerStateCommand {
    pub key: ByteString,
    pub result: GetStateResult,
    pub name: ByteString,
}
impl_command_accessors!(GetEagerState);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetEagerStateKeysCommand {
    pub state_keys: Vec<String>,
    pub name: ByteString,
}
impl_command_accessors!(GetEagerStateKeys);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPromiseCommand {
    pub key: ByteString,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetPromise);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeekPromiseCommand {
    pub key: ByteString,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(PeekPromise);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletePromiseCommand {
    pub key: ByteString,
    pub value: CompletePromiseValue,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletePromiseValue {
    Success(Bytes),
    Failure(Failure),
}
impl_command_accessors!(CompletePromise);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleepCommand {
    pub wake_up_time: MillisSinceEpoch,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(Sleep);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallRequest {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub span_context: ServiceInvocationSpanContext,
    pub parameter: Bytes,
    pub headers: Vec<Header>,
    pub idempotency_key: Option<ByteString>,
    pub completion_retention_duration: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallCommand {
    pub request: CallRequest,
    pub invocation_id_completion_id: CompletionId,
    pub result_completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(Call);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneWayCallCommand {
    pub request: CallRequest,
    pub invoke_time: MillisSinceEpoch,
    pub invocation_id_completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(OneWayCall);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendNotificationCommand {
    pub target_invocation_id: InvocationId,
    pub signal_id: SignalId,
    pub result: SignalResult,
    pub name: ByteString,
}
impl_command_accessors!(SendNotification);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCommand {
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(Run);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachInvocationTarget {
    InvocationId(InvocationId),
    IdempotentRequest(IdempotencyId),
    Workflow(ServiceId),
}
impl From<AttachInvocationTarget> for InvocationQuery {
    fn from(value: AttachInvocationTarget) -> Self {
        match value {
            AttachInvocationTarget::InvocationId(iid) => Self::Invocation(iid),
            AttachInvocationTarget::IdempotentRequest(iid) => Self::IdempotencyId(iid),
            AttachInvocationTarget::Workflow(wfid) => Self::Workflow(wfid),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachInvocationCommand {
    pub target: AttachInvocationTarget,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(AttachInvocation);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetInvocationOutputCommand {
    pub target: AttachInvocationTarget,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetInvocationOutput);
