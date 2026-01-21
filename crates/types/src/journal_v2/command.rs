// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};

use crate::errors::IdDecodeError;
use crate::identifiers::{
    AwakeableIdentifier, ExternalSignalIdentifier, IdempotencyId, InvocationId, ServiceId,
};
use crate::invocation::{Header, InvocationQuery, InvocationTarget, ServiceInvocationSpanContext};
use crate::journal_v2::raw::{TryFromEntry, TryFromEntryError};
use crate::journal_v2::{
    CompletionId, Entry, EntryMetadata, EntryType, Failure, GetStateResult, SignalId, SignalResult,
};
use crate::time::MillisSinceEpoch;

#[enum_dispatch]
pub trait CommandMetadata {
    fn related_completion_ids(&self) -> Vec<CompletionId>;
    fn name(&self) -> &str;
}

#[enum_dispatch(EntryMetadata, CommandMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants, Serialize, Deserialize)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(CommandType))]
#[strum_discriminants(derive(
    serde::Serialize,
    serde::Deserialize,
    strum::EnumString,
    strum::IntoStaticStr
))]
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
    SendSignal(SendSignalCommand),
    Run(RunCommand),
    AttachInvocation(AttachInvocationCommand),
    GetInvocationOutput(GetInvocationOutputCommand),
    CompleteAwakeable(CompleteAwakeableCommand),
}

impl fmt::Display for CommandType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl TryFromEntry for Command {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
        match entry {
            Entry::Command(cmd) => Ok(cmd),
            e => Err(TryFromEntryError {
                expected: EntryType::Command(CommandType::Input),
                actual: e.ty(),
            }),
        }
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
    ($ty:ident -> [@no_completion $($tail:tt)*]) => {
        impl CommandMetadata for paste::paste! { [< $ty Command >] } {
            fn related_completion_ids(&self) -> Vec<CompletionId> {
                vec![]
            }

            fn name(&self) -> &str {
                &self.name
            }
        }
        impl_command_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@result_completion $($tail:tt)*]) => {
        impl CommandMetadata for paste::paste! { [< $ty Command >] } {
            fn related_completion_ids(&self) -> Vec<CompletionId> {
                vec![self.completion_id]
            }

            fn name(&self) -> &str {
                &self.name
            }
        }
        impl_command_accessors!($ty -> [$($tail)*]);
    };

    // Entrypoint of the macro
    ($ty:ident: [$($tokens:tt)*]) => {
        impl_command_accessors!($ty -> [$($tokens)*]);
    };
}

// --- Actual implementation of individual commands

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputCommand {
    pub headers: Vec<Header>,
    pub payload: Bytes,
    pub name: ByteString,
}
impl_command_accessors!(Input -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputCommand {
    pub result: OutputResult,
    pub name: ByteString,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutputResult {
    Success(Bytes),
    Failure(Failure),
}
impl_command_accessors!(Output -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetLazyStateCommand {
    pub key: ByteString,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetLazyState -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetStateCommand {
    pub key: ByteString,
    pub value: Bytes,
    pub name: ByteString,
}
impl_command_accessors!(SetState -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClearStateCommand {
    pub key: ByteString,
    pub name: ByteString,
}
impl_command_accessors!(ClearState -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClearAllStateCommand {
    pub name: ByteString,
}
impl_command_accessors!(ClearAllState -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetLazyStateKeysCommand {
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetLazyStateKeys -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetEagerStateCommand {
    pub key: ByteString,
    pub result: GetStateResult,
    pub name: ByteString,
}
impl_command_accessors!(GetEagerState -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetEagerStateKeysCommand {
    // todo: check if this can be ByteString
    pub state_keys: Vec<String>,
    pub name: ByteString,
}
impl_command_accessors!(GetEagerStateKeys -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPromiseCommand {
    pub key: ByteString,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetPromise -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeekPromiseCommand {
    pub key: ByteString,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(PeekPromise -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletePromiseCommand {
    pub key: ByteString,
    pub value: CompletePromiseValue,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompletePromiseValue {
    Success(Bytes),
    Failure(Failure),
}
impl_command_accessors!(CompletePromise -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SleepCommand {
    pub wake_up_time: MillisSinceEpoch,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(Sleep -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallRequest {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub span_context: ServiceInvocationSpanContext,
    pub parameter: Bytes,
    pub headers: Vec<Header>,
    pub idempotency_key: Option<ByteString>,
    pub completion_retention_duration: Duration,
    // Since v1.4.0. Messages older than v1.4.0 will have zero retention as that
    // matches the default behaviour of <= 1.3.x.
    #[serde(default)]
    pub journal_retention_duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallCommand {
    pub request: CallRequest,
    pub invocation_id_completion_id: CompletionId,
    pub result_completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(Call -> [@metadata @from_entry]);
impl CommandMetadata for CallCommand {
    fn related_completion_ids(&self) -> Vec<CompletionId> {
        vec![self.invocation_id_completion_id, self.result_completion_id]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OneWayCallCommand {
    pub request: CallRequest,
    pub invoke_time: MillisSinceEpoch,
    pub invocation_id_completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(OneWayCall -> [@metadata @from_entry]);
impl CommandMetadata for OneWayCallCommand {
    fn related_completion_ids(&self) -> Vec<CompletionId> {
        vec![self.invocation_id_completion_id]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendSignalCommand {
    pub target_invocation_id: InvocationId,
    pub signal_id: SignalId,
    pub result: SignalResult,
    pub name: ByteString,
}
impl_command_accessors!(SendSignal -> [@metadata @from_entry @no_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCommand {
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(Run -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
impl From<InvocationQuery> for AttachInvocationTarget {
    fn from(value: InvocationQuery) -> Self {
        match value {
            InvocationQuery::Invocation(iid) => Self::InvocationId(iid),
            InvocationQuery::IdempotencyId(iid) => Self::IdempotentRequest(iid),
            InvocationQuery::Workflow(wfid) => Self::Workflow(wfid),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachInvocationCommand {
    pub target: AttachInvocationTarget,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(AttachInvocation -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetInvocationOutputCommand {
    pub target: AttachInvocationTarget,
    pub completion_id: CompletionId,
    pub name: ByteString,
}
impl_command_accessors!(GetInvocationOutput -> [@metadata @from_entry @result_completion]);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteAwakeableCommand {
    pub id: CompleteAwakeableId,
    pub result: CompleteAwakeableResult,
    pub name: ByteString,
}
impl_command_accessors!(CompleteAwakeable -> [@metadata @from_entry @no_completion]);

#[derive(
    Debug, Clone, PartialEq, Eq, serde_with::SerializeDisplay, serde_with::DeserializeFromStr,
)]
pub enum CompleteAwakeableId {
    Old(AwakeableIdentifier),
    New(ExternalSignalIdentifier),
}

impl fmt::Display for CompleteAwakeableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompleteAwakeableId::Old(id) => fmt::Display::fmt(id, f),
            CompleteAwakeableId::New(id) => fmt::Display::fmt(id, f),
        }
    }
}

impl FromStr for CompleteAwakeableId {
    type Err = IdDecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        AwakeableIdentifier::from_str(s)
            .map(CompleteAwakeableId::Old)
            .or_else(|_| ExternalSignalIdentifier::from_str(s).map(CompleteAwakeableId::New))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompleteAwakeableResult {
    Success(Bytes),
    Failure(Failure),
}

#[cfg(any(test, feature = "test-util"))]
mod test_util {
    use super::*;

    impl CallRequest {
        pub fn mock(invocation_id: InvocationId, invocation_target: InvocationTarget) -> Self {
            Self {
                invocation_id,
                invocation_target,
                span_context: Default::default(),
                parameter: Default::default(),
                headers: vec![],
                idempotency_key: None,
                completion_retention_duration: Default::default(),
                journal_retention_duration: Default::default(),
            }
        }
    }
}
