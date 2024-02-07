// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use crate::errors::{InvocationError, UserErrorCode};
use crate::identifiers::EntryIndex;
use crate::time::MillisSinceEpoch;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    // IO
    PollInputStream(PollInputStreamEntry),
    OutputStream(OutputStreamEntry),

    // State access
    GetState(GetStateEntry),
    SetState(SetStateEntry),
    ClearState(ClearStateEntry),
    GetStateKeys(GetStateKeysEntry),
    ClearAllState,

    // Syscalls
    Sleep(SleepEntry),
    Invoke(InvokeEntry),
    BackgroundInvoke(BackgroundInvokeEntry),
    Awakeable(AwakeableEntry),
    CompleteAwakeable(CompleteAwakeableEntry),
    Custom(Bytes),
}

impl Entry {
    pub fn poll_input_stream(result: impl Into<Bytes>) -> Self {
        Entry::PollInputStream(PollInputStreamEntry {
            result: EntryResult::Success(result.into()),
        })
    }

    pub fn output_stream(result: EntryResult) -> Self {
        Entry::OutputStream(OutputStreamEntry { result })
    }

    pub fn get_state(key: impl Into<Bytes>, value: Option<GetStateResult>) -> Self {
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
        Entry::Invoke(InvokeEntry { request, result })
    }

    pub fn background_invoke(
        request: InvokeRequest,
        invoke_time: Option<MillisSinceEpoch>,
    ) -> Self {
        Entry::BackgroundInvoke(BackgroundInvokeEntry {
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
    Failure(UserErrorCode, ByteString),
}

impl From<ResponseResult> for CompletionResult {
    fn from(value: ResponseResult) -> Self {
        match value {
            ResponseResult::Success(bytes) => CompletionResult::Success(bytes),
            ResponseResult::Failure(error_code, error_msg) => {
                CompletionResult::Failure(error_code, error_msg)
            }
        }
    }
}

impl From<&InvocationError> for CompletionResult {
    fn from(value: &InvocationError) -> Self {
        CompletionResult::Failure(UserErrorCode::from(value.code()), value.message().into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryType {
    PollInputStream,
    OutputStream,
    GetState,
    SetState,
    ClearState,
    GetStateKeys,
    ClearAllState,
    Sleep,
    Invoke,
    BackgroundInvoke,
    Awakeable,
    CompleteAwakeable,
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
    Failure(UserErrorCode, ByteString),
}

impl From<EntryResult> for ResponseResult {
    fn from(value: EntryResult) -> Self {
        match value {
            EntryResult::Success(bytes) => ResponseResult::Success(bytes),
            EntryResult::Failure(code, error_msg) => ResponseResult::Failure(code, error_msg),
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
    impl Sealed for SleepEntry {}
    impl Sealed for InvokeEntry {}
    impl Sealed for AwakeableEntry {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollInputStreamEntry {
    pub result: EntryResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputStreamEntry {
    pub result: EntryResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetStateResult {
    Empty,
    Result(Bytes),
    Failure(UserErrorCode, ByteString),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetStateEntry {
    pub key: Bytes,
    pub value: Option<GetStateResult>,
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
    Failure(UserErrorCode, ByteString),
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
pub enum SleepResult {
    Fired,
    Failure(UserErrorCode, ByteString),
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
    pub method_name: ByteString,
    pub parameter: Bytes,
}

impl InvokeRequest {
    pub fn new(
        service_name: impl Into<ByteString>,
        method_name: impl Into<ByteString>,
        parameter: impl Into<Bytes>,
    ) -> Self {
        InvokeRequest {
            service_name: service_name.into(),
            method_name: method_name.into(),
            parameter: parameter.into(),
        }
    }
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
pub struct BackgroundInvokeEntry {
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
