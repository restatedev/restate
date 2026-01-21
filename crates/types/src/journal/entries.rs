// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Restate journal is represented by Restate entries, each of them recording a specific action taken by the user code.

use super::*;

use crate::errors::{InvocationError, InvocationErrorCode};
use crate::identifiers::{EntryIndex, IdempotencyId, ServiceId};
use crate::invocation::Header;
use crate::time::MillisSinceEpoch;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    // IO
    Input(InputEntry),
    Output(OutputEntry),

    // State access
    GetState(GetStateEntry),
    SetState(SetStateEntry),
    ClearState(ClearStateEntry),
    GetStateKeys(GetStateKeysEntry),
    ClearAllState,
    GetPromise(GetPromiseEntry),
    PeekPromise(PeekPromiseEntry),
    CompletePromise(CompletePromiseEntry),

    // Syscalls
    Sleep(SleepEntry),
    Call(InvokeEntry),
    OneWayCall(OneWayCallEntry),
    Awakeable(AwakeableEntry),
    CompleteAwakeable(CompleteAwakeableEntry),
    Run(RunEntry),
    CancelInvocation(CancelInvocationEntry),
    GetCallInvocationId(GetCallInvocationIdEntry),
    AttachInvocation(AttachInvocationEntry),
    GetInvocationOutput(GetInvocationOutputEntry),
    Custom(Bytes),
}

impl Entry {
    pub fn input(result: impl Into<Bytes>) -> Self {
        Entry::Input(InputEntry {
            headers: vec![],
            value: result.into(),
        })
    }

    pub fn output(result: EntryResult) -> Self {
        Entry::Output(OutputEntry { result })
    }

    pub fn get_state(key: impl Into<Bytes>, value: Option<CompletionResult>) -> Self {
        Entry::GetState(GetStateEntry {
            key: key.into(),
            value,
        })
    }

    pub fn set_state(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Entry::SetState(SetStateEntry {
            key: key.into(),
            value: value.into(),
        })
    }

    pub fn clear_state(key: impl Into<Bytes>) -> Self {
        Entry::ClearState(ClearStateEntry { key: key.into() })
    }

    pub fn get_state_keys(value: Option<GetStateKeysResult>) -> Self {
        Entry::GetStateKeys(GetStateKeysEntry { value })
    }

    pub fn clear_all_state() -> Self {
        Entry::ClearAllState
    }

    pub fn invoke(request: InvokeRequest, result: Option<EntryResult>) -> Self {
        Entry::Call(InvokeEntry { request, result })
    }

    pub fn background_invoke(
        request: InvokeRequest,
        invoke_time: Option<MillisSinceEpoch>,
    ) -> Self {
        Entry::OneWayCall(OneWayCallEntry {
            request,
            invoke_time: invoke_time.map(|t| t.as_u64()).unwrap_or_default(),
        })
    }

    pub fn complete_awakeable(id: impl Into<ByteString>, result: EntryResult) -> Self {
        Entry::CompleteAwakeable(CompleteAwakeableEntry {
            id: id.into(),
            result,
        })
    }

    pub fn awakeable(result: Option<EntryResult>) -> Self {
        Entry::Awakeable(AwakeableEntry { result })
    }

    pub fn cancel_invocation(target: CancelInvocationTarget) -> Entry {
        Entry::CancelInvocation(CancelInvocationEntry { target })
    }

    pub fn get_call_invocation_id(
        call_entry_index: EntryIndex,
        result: Option<GetCallInvocationIdResult>,
    ) -> Entry {
        Entry::GetCallInvocationId(GetCallInvocationIdEntry {
            call_entry_index,
            result,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    pub entry_index: EntryIndex,
    pub result: CompletionResult,
}

impl Completion {
    pub fn new(entry_index: EntryIndex, result: CompletionResult) -> Self {
        Self {
            entry_index,
            result,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionResult {
    Empty,
    Success(Bytes),
    Failure(InvocationErrorCode, ByteString),
}

impl From<ResponseResult> for CompletionResult {
    fn from(value: ResponseResult) -> Self {
        match value {
            ResponseResult::Success(bytes) => CompletionResult::Success(bytes),
            ResponseResult::Failure(e) => CompletionResult::Failure(e.code(), e.message().into()),
        }
    }
}

impl From<EntryResult> for CompletionResult {
    fn from(value: EntryResult) -> Self {
        match value {
            EntryResult::Success(s) => CompletionResult::Success(s),
            EntryResult::Failure(c, m) => CompletionResult::Failure(c, m),
        }
    }
}

impl From<&InvocationError> for CompletionResult {
    fn from(value: &InvocationError) -> Self {
        CompletionResult::Failure(value.code(), value.message().into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    Input,
    Output,
    GetState,
    SetState,
    ClearState,
    GetStateKeys,
    ClearAllState,
    GetPromise,
    PeekPromise,
    CompletePromise,
    Sleep,
    Call,
    OneWayCall,
    Awakeable,
    CompleteAwakeable,
    Run,
    CancelInvocation,
    GetCallInvocationId,
    AttachInvocation,
    GetInvocationOutput,
    Custom,
}

impl fmt::Display for EntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryResult {
    Success(Bytes),
    Failure(InvocationErrorCode, ByteString),
}

impl From<EntryResult> for ResponseResult {
    fn from(value: EntryResult) -> Self {
        match value {
            EntryResult::Success(bytes) => ResponseResult::Success(bytes),
            EntryResult::Failure(code, error_msg) => {
                ResponseResult::Failure(InvocationError::new(code, error_msg))
            }
        }
    }
}

pub trait CompletableEntry: private::Sealed {
    /// Returns true if the entry is completed.
    fn is_completed(&self) -> bool;
}

mod private {
    use super::*;

    pub trait Sealed {}
    impl Sealed for GetStateEntry {}
    impl Sealed for GetStateKeysEntry {}
    impl Sealed for GetPromiseEntry {}
    impl Sealed for PeekPromiseEntry {}
    impl Sealed for CompletePromiseEntry {}
    impl Sealed for SleepEntry {}
    impl Sealed for InvokeEntry {}
    impl Sealed for AwakeableEntry {}
    impl Sealed for GetCallInvocationIdEntry {}
    impl Sealed for AttachInvocationEntry {}
    impl Sealed for GetInvocationOutputEntry {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputEntry {
    pub headers: Vec<Header>,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputEntry {
    pub result: EntryResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetStateEntry {
    pub key: Bytes,
    pub value: Option<CompletionResult>,
}

impl CompletableEntry for GetStateEntry {
    fn is_completed(&self) -> bool {
        self.value.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetStateEntry {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearStateEntry {
    pub key: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetStateKeysResult {
    Result(Vec<Bytes>),
    Failure(InvocationErrorCode, ByteString),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetStateKeysEntry {
    pub value: Option<GetStateKeysResult>,
}

impl CompletableEntry for GetStateKeysEntry {
    fn is_completed(&self) -> bool {
        self.value.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetPromiseEntry {
    pub key: ByteString,
    pub value: Option<EntryResult>,
}

impl CompletableEntry for GetPromiseEntry {
    fn is_completed(&self) -> bool {
        self.value.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeekPromiseEntry {
    pub key: ByteString,
    pub value: Option<CompletionResult>,
}

impl CompletableEntry for PeekPromiseEntry {
    fn is_completed(&self) -> bool {
        self.value.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletePromiseEntry {
    pub key: ByteString,
    pub completion: EntryResult,
    pub value: Option<CompleteResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompleteResult {
    Done,
    Failure(InvocationErrorCode, ByteString),
}

impl CompletableEntry for CompletePromiseEntry {
    fn is_completed(&self) -> bool {
        self.value.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SleepResult {
    Fired,
    Failure(InvocationErrorCode, ByteString),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SleepEntry {
    pub wake_up_time: u64,
    pub result: Option<SleepResult>,
}

impl CompletableEntry for SleepEntry {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvokeRequest {
    pub service_name: ByteString,
    pub handler_name: ByteString,
    pub parameter: Bytes,
    pub headers: Vec<Header>,
    /// Empty if service call.
    /// The reason this is not Option<ByteString> is that it cannot be distinguished purely from the message
    /// whether the key is none or empty.
    pub key: ByteString,
    pub idempotency_key: Option<ByteString>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvokeEntry {
    pub request: InvokeRequest,
    pub result: Option<EntryResult>,
}

impl CompletableEntry for InvokeEntry {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneWayCallEntry {
    pub request: InvokeRequest,
    pub invoke_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwakeableEntry {
    pub result: Option<EntryResult>,
}

impl CompletableEntry for AwakeableEntry {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteAwakeableEntry {
    pub id: ByteString,
    pub result: EntryResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunEntry {
    pub result: EntryResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelInvocationTarget {
    InvocationId(ByteString),
    CallEntryIndex(EntryIndex),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelInvocationEntry {
    pub target: CancelInvocationTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetCallInvocationIdResult {
    InvocationId(String),
    Failure(InvocationErrorCode, ByteString),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetCallInvocationIdEntry {
    pub call_entry_index: EntryIndex,
    pub result: Option<GetCallInvocationIdResult>,
}

impl CompletableEntry for GetCallInvocationIdEntry {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachInvocationEntry {
    pub target: AttachInvocationTarget,
    pub result: Option<EntryResult>,
}

impl CompletableEntry for AttachInvocationEntry {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachInvocationTarget {
    InvocationId(ByteString),
    CallEntryIndex(EntryIndex),
    IdempotentRequest(IdempotencyId),
    Workflow(ServiceId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetInvocationOutputEntry {
    pub target: AttachInvocationTarget,
    pub result: Option<CompletionResult>,
}

impl CompletableEntry for GetInvocationOutputEntry {
    fn is_completed(&self) -> bool {
        self.result.is_some()
    }
}
