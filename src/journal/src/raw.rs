use super::*;
use common::utils::GenericError;

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

    pub fn into_inner(self) -> (RawEntryHeader, Bytes) {
        (self.header, self.entry)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Cannot decode {ty:?}. {kind:?}")]
pub struct RawEntryCodecError {
    ty: RawEntryHeader,
    kind: ErrorKind,
}

impl RawEntryCodecError {
    pub fn new(ty: RawEntryHeader, kind: ErrorKind) -> Self {
        Self { ty, kind }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ErrorKind {
    #[error("failed to decode: {source:?}")]
    Decode { source: Option<GenericError> },
    #[error("Field '{0}' is missing")]
    MissingField(&'static str),
}

pub trait RawEntryCodec {
    fn deserialize(entry: &RawEntry) -> Result<Entry, RawEntryCodecError>;

    fn write_completion(
        entry: &mut RawEntry,
        completion_result: CompletionResult,
    ) -> Result<(), RawEntryCodecError>;
}
