// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::identifiers::InvocationId;
use crate::invocation::InvocationTarget;
use crate::journal_v2::{
    AttachInvocationTarget, CommandType, CompleteAwakeableId, CompletionId, EntryMetadata,
    EntryType, NotificationId, NotificationMetadata, NotificationType, SignalId,
};
use crate::time::MillisSinceEpoch;
use bytestring::ByteString;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};

/// Lite data model, exposing fewer info about the entries.
/// This is exposed in SQL to avoid sending around full entry payloads, when only some fields of the entry are needed.
#[enum_dispatch(EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum EntryLite {
    Command(CommandLite),
    Notification(NotificationLite),
}

// ---- Commands

#[enum_dispatch(EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum CommandLite {
    Input(InputCommandLite),
    Output(OutputCommandLite),
    GetLazyState(GetLazyStateCommandLite),
    SetState(SetStateCommandLite),
    ClearState(ClearStateCommandLite),
    ClearAllState(ClearAllStateCommandLite),
    GetLazyStateKeys(GetLazyStateKeysCommandLite),
    GetEagerState(GetEagerStateCommandLite),
    GetEagerStateKeys(GetEagerStateKeysCommandLite),
    GetPromise(GetPromiseCommandLite),
    PeekPromise(PeekPromiseCommandLite),
    CompletePromise(CompletePromiseCommandLite),
    Sleep(SleepCommandLite),
    Call(CallCommandLite),
    OneWayCall(OneWayCallCommandLite),
    SendSignal(SendSignalCommandLite),
    Run(RunCommandLite),
    AttachInvocation(AttachInvocationCommandLite),
    GetInvocationOutput(GetInvocationOutputCommandLite),
    CompleteAwakeable(CompleteAwakeableCommandLite),
}

// Little macro to reduce boilerplate for TryFromEntry and EntryMetadata.
macro_rules! impl_command_accessors {
    ($ty:ident -> []) => {
        // End of macro
    };
    ($ty:ident -> [@from_entry $($tail:tt)*]) => {
        impl From<paste::paste! { [< $ty CommandLite >] }> for EntryLite {
            fn from(v: paste::paste! { [< $ty CommandLite >] }) -> Self {
                Self::Command(v.into())
            }
        }
        impl_command_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@metadata $($tail:tt)*]) => {
        impl EntryMetadata for paste::paste! { [< $ty CommandLite >] } {
            fn ty(&self) -> EntryType {
                EntryType::Command(CommandType::$ty)
            }
        }
        impl_command_accessors!($ty -> [$($tail)*]);
    };

    // Entrypoint of the macro
    ($ty:ident: [$($tokens:tt)*]) => {
        impl_command_accessors!($ty -> [$($tokens)*]);
    };
    ($ty:ident) => {
        impl_command_accessors!($ty -> [@metadata @from_entry]);
    };
}

// --- Actual implementation of individual commands

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InputCommandLite {}
impl_command_accessors!(Input);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutputCommandLite {
    pub result: OutputResultLite,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OutputResultLite {
    Success,
    Failure,
}
impl_command_accessors!(Output);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GetLazyStateCommandLite {
    pub key: ByteString,
    pub completion_id: CompletionId,
}
impl_command_accessors!(GetLazyState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SetStateCommandLite {
    pub key: ByteString,
}
impl_command_accessors!(SetState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClearStateCommandLite {
    pub key: ByteString,
}
impl_command_accessors!(ClearState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClearAllStateCommandLite {}
impl_command_accessors!(ClearAllState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GetLazyStateKeysCommandLite {
    pub completion_id: CompletionId,
}
impl_command_accessors!(GetLazyStateKeys);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GetEagerStateCommandLite {
    pub key: ByteString,
    pub result: GetStateResultLite,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum GetStateResultLite {
    Void,
    Success,
}
impl_command_accessors!(GetEagerState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GetEagerStateKeysCommandLite {}
impl_command_accessors!(GetEagerStateKeys);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GetPromiseCommandLite {
    pub key: ByteString,
    pub completion_id: CompletionId,
}
impl_command_accessors!(GetPromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeekPromiseCommandLite {
    pub key: ByteString,
    pub completion_id: CompletionId,
}
impl_command_accessors!(PeekPromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompletePromiseCommandLite {
    pub key: ByteString,
    pub completion_id: CompletionId,
}
impl_command_accessors!(CompletePromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SleepCommandLite {
    pub wake_up_time: MillisSinceEpoch,
    pub completion_id: CompletionId,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub name: ByteString,
}
impl_command_accessors!(Sleep);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CallCommandLite {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub invocation_id_completion_id: CompletionId,
    pub result_completion_id: CompletionId,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub name: ByteString,
}
impl_command_accessors!(Call);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OneWayCallCommandLite {
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub invoke_time: MillisSinceEpoch,
    pub invocation_id_completion_id: CompletionId,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub name: ByteString,
}
impl_command_accessors!(OneWayCall);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SendSignalCommandLite {
    pub target_invocation_id: InvocationId,
    pub signal_id: SignalId,
    pub result: SignalResultLite,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SignalResultLite {
    Void,
    Success,
    Failure,
}
impl_command_accessors!(SendSignal);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunCommandLite {
    pub completion_id: CompletionId,
    #[serde(default, skip_serializing_if = "str::is_empty")]
    pub name: ByteString,
}
impl_command_accessors!(Run);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AttachInvocationCommandLite {
    pub target: AttachInvocationTarget,
    pub completion_id: CompletionId,
}
impl_command_accessors!(AttachInvocation);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GetInvocationOutputCommandLite {
    pub target: AttachInvocationTarget,
    pub completion_id: CompletionId,
}
impl_command_accessors!(GetInvocationOutput);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CompleteAwakeableCommandLite {
    pub id: CompleteAwakeableId,
    pub result: CompleteAwakeableResultLite,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompleteAwakeableResultLite {
    Success,
    Failure,
}
impl_command_accessors!(CompleteAwakeable);

// --- Notification lite

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NotificationLite {
    pub ty: NotificationType,
    pub id: NotificationId,
    pub result: NotificationResultLite,
}

impl EntryMetadata for NotificationLite {
    fn ty(&self) -> EntryType {
        EntryType::Notification(self.ty)
    }
}

impl NotificationMetadata for NotificationLite {
    fn id(&self) -> NotificationId {
        self.id.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum NotificationResultLite {
    Void,
    Success,
    Failure,
    StateKeys,
    InvocationId,
}
