//! This crate defines the journal model.

use bytes::Bytes;
use bytestring::ByteString;

pub mod raw;

mod entries;
pub use entries::*;

pub type JournalRevision = u32;

pub type EntryIndex = u32;

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
    Unknown(u16),
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
    Unknown(Bytes),
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
