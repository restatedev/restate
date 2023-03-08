//! This crate defines the journal model.

use bytes::Bytes;
use bytestring::ByteString;
use common::types::{EntryIndex, ResponseResult};

pub mod raw;

mod entries;
pub use entries::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    // IO
    PollInputStream(PollInputStreamEntry),
    OutputStream(OutputStreamEntry),

    // State access
    GetState(GetStateEntry),
    SetState(SetStateEntry),
    ClearState(ClearStateEntry),

    // Syscalls
    Sleep(SleepEntry),
    Invoke(InvokeEntry),
    BackgroundInvoke(BackgroundInvokeEntry),
    Awakeable(AwakeableEntry),
    CompleteAwakeable(CompleteAwakeableEntry),
    Custom(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionResult {
    Ack,
    Empty,
    Success(Bytes),
    Failure(i32, ByteString),
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
