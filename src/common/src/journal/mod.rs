//! This crate defines the journal model.

use crate::types::{CompletionResult, EntryIndex, ResponseResult};
use bytes::Bytes;
use bytestring::ByteString;

mod enriched;
pub mod raw;
pub use enriched::EntryEnricher;
mod entries;
pub use entries::*;
#[cfg(feature = "mocks")]
pub mod mocks;

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
