use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawEntryHeader {
    PollInputStream { is_completed: bool },
    OutputStream,
    GetState { is_completed: bool },
    SetState,
    ClearState,
    Sleep { is_completed: bool },
    Invoke { is_completed: bool },
    BackgroundInvoke,
    Awakeable { is_completed: bool },
    CompleteAwakeable,
    Custom { code: u16, requires_ack: bool },
}

impl RawEntryHeader {
    fn as_entry_type(&self) -> EntryType {
        match self {
            RawEntryHeader::PollInputStream { .. } => EntryType::PollInputStream,
            RawEntryHeader::OutputStream => EntryType::OutputStream,
            RawEntryHeader::GetState { .. } => EntryType::GetState,
            RawEntryHeader::SetState => EntryType::SetState,
            RawEntryHeader::ClearState => EntryType::ClearState,
            RawEntryHeader::Sleep { .. } => EntryType::Sleep,
            RawEntryHeader::Invoke { .. } => EntryType::Invoke,
            RawEntryHeader::BackgroundInvoke => EntryType::BackgroundInvoke,
            RawEntryHeader::Awakeable { .. } => EntryType::Awakeable,
            RawEntryHeader::CompleteAwakeable => EntryType::CompleteAwakeable,
            RawEntryHeader::Custom { code, .. } => EntryType::Custom(*code),
        }
    }

    pub fn is_completed(&self) -> Option<bool> {
        match self {
            RawEntryHeader::PollInputStream { is_completed } => Some(*is_completed),
            RawEntryHeader::OutputStream => None,
            RawEntryHeader::GetState { is_completed } => Some(*is_completed),
            RawEntryHeader::SetState => None,
            RawEntryHeader::ClearState => None,
            RawEntryHeader::Sleep { is_completed } => Some(*is_completed),
            RawEntryHeader::Invoke { is_completed } => Some(*is_completed),
            RawEntryHeader::BackgroundInvoke => None,
            RawEntryHeader::Awakeable { is_completed } => Some(*is_completed),
            RawEntryHeader::CompleteAwakeable => None,
            RawEntryHeader::Custom { .. } => None,
        }
    }

    pub fn mark_completed(&mut self) {
        match self {
            RawEntryHeader::PollInputStream { is_completed } => *is_completed = true,
            RawEntryHeader::OutputStream => {}
            RawEntryHeader::GetState { is_completed } => *is_completed = true,
            RawEntryHeader::SetState => {}
            RawEntryHeader::ClearState => {}
            RawEntryHeader::Sleep { is_completed } => *is_completed = true,
            RawEntryHeader::Invoke { is_completed } => *is_completed = true,
            RawEntryHeader::BackgroundInvoke => {}
            RawEntryHeader::Awakeable { is_completed } => *is_completed = true,
            RawEntryHeader::CompleteAwakeable => {}
            RawEntryHeader::Custom { .. } => {}
        }
    }
}

/// This struct represents a serialized journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry {
    pub header: RawEntryHeader,
    pub entry: Bytes,
}

impl RawEntry {
    pub fn new(header: RawEntryHeader, entry: Bytes) -> Self {
        Self { header, entry }
    }

    pub fn entry_type(&self) -> EntryType {
        self.header.as_entry_type()
    }

    pub fn into_inner(self) -> (RawEntryHeader, Bytes) {
        (self.header, self.entry)
    }
}

pub trait RawEntryCodec {
    type Error;

    fn deserialize(entry: &RawEntry) -> Result<Entry, Self::Error>;

    fn write_completion(
        entry: &mut RawEntry,
        completion_result: CompletionResult,
    ) -> Result<(), Self::Error>;
}
