//! This crate defines the journal model.

use bytes::Bytes;
use bytestring::ByteString;
use common::types::{EntryIndex, ResponseResult};

pub mod raw;

mod entries;
pub use entries::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    // IO
    PollInputStream,
    OutputStream,

    // State access
    GetState,
    SetState,
    ClearState,

    // Syscalls
    Sleep,
    Invoke,
    BackgroundInvoke,
    Awakeable,
    CompleteAwakeable,

    // Unknown
    Custom(u16),
}

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
