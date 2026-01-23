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

use bytes::Bytes;
use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};

use crate::identifiers::InvocationId;
use crate::journal_v2::raw::{RawEntry, TryFromEntry, TryFromEntryError};
use crate::journal_v2::{
    CompletionId, Encoder, Entry, EntryMetadata, EntryType, Failure, SignalIndex, SignalName,
};

/// See [`Notification`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NotificationId {
    CompletionId(CompletionId),
    SignalIndex(SignalIndex),
    SignalName(SignalName),
}

impl NotificationId {
    pub const fn for_completion(id: CompletionId) -> Self {
        Self::CompletionId(id)
    }

    pub fn for_signal(signal_id: SignalId) -> Self {
        match signal_id {
            SignalId::Index(idx) => NotificationId::SignalIndex(idx),
            SignalId::Name(n) => NotificationId::SignalName(n),
        }
    }
}

impl From<SignalId> for NotificationId {
    fn from(value: SignalId) -> Self {
        NotificationId::for_signal(value)
    }
}

impl fmt::Display for NotificationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationId::SignalIndex(idx) => write!(f, "SignalIndex: {idx}"),
            NotificationId::SignalName(name) => write!(f, "SignalName: {name}"),
            NotificationId::CompletionId(idx) => write!(f, "CompletionId: {idx}"),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NotificationType {
    Completion(CompletionType),
    Signal,
}

impl fmt::Display for NotificationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotificationType::Completion(cmp) => fmt::Display::fmt(cmp, f),
            e => fmt::Debug::fmt(e, f),
        }
    }
}

impl From<CompletionType> for NotificationType {
    fn from(value: CompletionType) -> Self {
        NotificationType::Completion(value)
    }
}

impl From<CompletionType> for EntryType {
    fn from(value: CompletionType) -> Self {
        EntryType::Notification(value.into())
    }
}

#[enum_dispatch]
pub trait NotificationMetadata {
    fn id(&self) -> NotificationId;
}

#[enum_dispatch(NotificationMetadata, EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Notification {
    Completion(Completion),
    Signal(Signal),
}

impl Notification {
    pub fn new_completion(completion: impl Into<Completion>) -> Self {
        Self::Completion(completion.into())
    }

    pub fn new_signal(signal: Signal) -> Self {
        Self::Signal(signal)
    }

    pub fn encode<E: Encoder>(self) -> RawEntry {
        E::encode_entry(Entry::Notification(self))
    }
}

#[enum_dispatch(NotificationMetadata, EntryMetadata)]
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumDiscriminants, Serialize, Deserialize)]
#[strum_discriminants(vis(pub))]
#[strum_discriminants(name(CompletionType))]
#[strum_discriminants(derive(
    serde::Serialize,
    serde::Deserialize,
    strum::EnumString,
    strum::IntoStaticStr
))]
pub enum Completion {
    GetLazyState(GetLazyStateCompletion),
    GetLazyStateKeys(GetLazyStateKeysCompletion),
    GetPromise(GetPromiseCompletion),
    PeekPromise(PeekPromiseCompletion),
    CompletePromise(CompletePromiseCompletion),
    Sleep(SleepCompletion),
    CallInvocationId(CallInvocationIdCompletion),
    Call(CallCompletion),
    Run(RunCompletion),
    AttachInvocation(AttachInvocationCompletion),
    GetInvocationOutput(GetInvocationOutputCompletion),
}

impl fmt::Display for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<Completion> for Entry {
    fn from(value: Completion) -> Self {
        Entry::Notification(Notification::Completion(value))
    }
}

// Little macro to reduce boilerplate for TryFromEntry and EntryMetadata.
macro_rules! impl_completion_accessors {
    ($ty:ident -> []) => {
        // End of macro
    };
    ($ty:ident -> [@from_entry $($tail:tt)*]) => {
        impl TryFromEntry for paste::paste! { [< $ty Completion >] } {
            fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
                match entry {
                    Entry::Notification(Notification::Completion(Completion::$ty(e))) => Ok(e),
                    e => Err(TryFromEntryError {
                        expected: EntryType::Notification(NotificationType::Completion(CompletionType::$ty)),
                        actual: e.ty(),
                    }),
                }
            }
        }
        impl From<paste::paste! { [< $ty Completion >] }> for Entry {
            fn from(v: paste::paste! { [< $ty Completion >] }) -> Self {
                Self::Notification(v.into())
            }
        }
        impl From<paste::paste! { [< $ty Completion >] }> for Notification {
            fn from(v: paste::paste! { [< $ty Completion >] }) -> Self {
                Self::Completion(v.into())
            }
        }
        impl_completion_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@entry_metadata $($tail:tt)*]) => {
        impl EntryMetadata for paste::paste! { [< $ty Completion >] } {
            fn ty(&self) -> EntryType {
                EntryType::Notification(NotificationType::Completion(CompletionType::$ty))
            }
        }
        impl_completion_accessors!($ty -> [$($tail)*]);
    };
    ($ty:ident -> [@notification_metadata $($tail:tt)*]) => {
        impl NotificationMetadata for paste::paste! { [< $ty Completion >] } {
            fn id(&self) -> NotificationId {
                NotificationId::CompletionId(self.completion_id)
            }
        }
        impl_completion_accessors!($ty -> [$($tail)*]);
    };

    // Entrypoints of the macro
    ($ty:ident: [$($tokens:tt)*]) => {
        impl_completion_accessors!($ty -> [$($tokens)*]);
    };
    ($ty:ident) => {
        impl_completion_accessors!($ty -> [@entry_metadata @notification_metadata @from_entry]);
    };
}

// --- Actual implementation of individual notifications

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetLazyStateCompletion {
    pub completion_id: CompletionId,
    pub result: GetStateResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GetStateResult {
    Void,
    Success(Bytes),
}
impl_completion_accessors!(GetLazyState);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetLazyStateKeysCompletion {
    pub completion_id: CompletionId,
    pub state_keys: Vec<String>,
}
impl_completion_accessors!(GetLazyStateKeys);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetPromiseCompletion {
    pub completion_id: CompletionId,
    pub result: GetPromiseResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GetPromiseResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(GetPromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeekPromiseCompletion {
    pub completion_id: CompletionId,
    pub result: PeekPromiseResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeekPromiseResult {
    Void,
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(PeekPromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletePromiseCompletion {
    pub completion_id: CompletionId,
    pub result: CompletePromiseResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompletePromiseResult {
    Void,
    Failure(Failure),
}
impl_completion_accessors!(CompletePromise);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SleepCompletion {
    pub completion_id: CompletionId,
}
impl_completion_accessors!(Sleep);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallInvocationIdCompletion {
    pub completion_id: CompletionId,
    pub invocation_id: InvocationId,
}
impl_completion_accessors!(CallInvocationId);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallCompletion {
    pub completion_id: CompletionId,
    pub result: CallResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(Call);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCompletion {
    pub completion_id: CompletionId,
    pub result: RunResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(Run);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachInvocationCompletion {
    pub completion_id: CompletionId,
    pub result: AttachInvocationResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttachInvocationResult {
    Success(Bytes),
    Failure(Failure),
}
impl_completion_accessors!(AttachInvocation);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetInvocationOutputCompletion {
    pub completion_id: CompletionId,
    pub result: GetInvocationOutputResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, bilrost::Oneof)]
pub enum GetInvocationOutputResult {
    Void,
    #[bilrost(2)]
    Success(Bytes),
    #[bilrost(3)]
    Failure(Failure),
}
impl_completion_accessors!(GetInvocationOutput);

// Signal

#[repr(u32)]
#[derive(Debug, strum::FromRepr)]
pub enum BuiltInSignal {
    Cancel = 1,
}

pub const CANCEL_SIGNAL: Notification = Notification::Signal(Signal::new(
    SignalId::for_builtin_signal(BuiltInSignal::Cancel),
    SignalResult::Void,
));

pub const CANCEL_NOTIFICATION_ID: NotificationId =
    NotificationId::SignalIndex(BuiltInSignal::Cancel as u32);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalId {
    Index(SignalIndex),
    Name(SignalName),
}

impl fmt::Display for SignalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignalId::Index(idx) => {
                if let Some(built_in_signal) = BuiltInSignal::from_repr(*idx) {
                    write!(f, "{built_in_signal:?}")
                } else {
                    write!(f, "index {idx}")
                }
            }
            SignalId::Name(name) => write!(f, "{name}"),
        }
    }
}

impl SignalId {
    pub const fn for_builtin_signal(signal: BuiltInSignal) -> Self {
        Self::for_index(signal as u32)
    }

    pub const fn for_index(id: SignalIndex) -> Self {
        Self::Index(id)
    }

    pub fn for_name(id: SignalName) -> Self {
        Self::Name(id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub id: SignalId,
    pub result: SignalResult,
}

impl Signal {
    pub const fn new(id: SignalId, result: SignalResult) -> Self {
        Self { id, result }
    }
}

impl EntryMetadata for Signal {
    fn ty(&self) -> EntryType {
        EntryType::Notification(NotificationType::Signal)
    }
}

impl NotificationMetadata for Signal {
    fn id(&self) -> NotificationId {
        self.id.clone().into()
    }
}

impl TryFromEntry for Signal {
    fn try_from(entry: Entry) -> Result<Self, TryFromEntryError> {
        match entry {
            Entry::Notification(Notification::Signal(e)) => Ok(e),
            e => Err(TryFromEntryError {
                expected: EntryType::Notification(NotificationType::Signal),
                actual: e.ty(),
            }),
        }
    }
}

impl From<Signal> for Entry {
    fn from(v: Signal) -> Self {
        Self::Notification(v.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignalResult {
    Void,
    Success(Bytes),
    Failure(Failure),
}
