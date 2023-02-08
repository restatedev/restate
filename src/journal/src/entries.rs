use super::*;
use common::types::EntryIndex;

pub trait CompletableEntry: private::Sealed {
    fn is_completed(&self) -> bool;
}

mod private {
    use super::*;

    pub trait Sealed {}
    impl Sealed for GetStateEntry {}
    impl Sealed for SleepEntry {}
    impl Sealed for InvokeEntry {}
    impl Sealed for AwakeableEntry {}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryResult {
    Success(Bytes),
    Failure(i32, ByteString),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollInputStreamEntry {
    pub result: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputStreamEntry {
    pub result: EntryResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GetStateValue {
    Empty,
    Value(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetStateEntry {
    pub key: Bytes,
    pub value: Option<GetStateValue>,
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
pub struct SleepEntry {
    pub wake_up_time: i64,
    pub fired: bool,
}

impl CompletableEntry for SleepEntry {
    fn is_completed(&self) -> bool {
        self.fired
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvokeRequest {
    pub service_name: ByteString,
    pub method_name: ByteString,
    pub parameter: Bytes,
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
pub struct BackgroundInvokeEntry(pub InvokeRequest);

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
    pub service_name: ByteString,
    pub instance_key: Bytes,
    pub invocation_id: Bytes,
    pub entry_index: EntryIndex,

    pub result: EntryResult,
}
