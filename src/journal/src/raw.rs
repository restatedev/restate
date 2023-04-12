use super::*;
use common::types::{EnrichedEntryHeader, RawEntry};
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
}

impl Header for RawEntryHeader {
    fn is_completed(&self) -> Option<bool> {
        self.is_completed()
    }

    fn mark_completed(&mut self) {
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

    fn to_entry_type(&self) -> EntryType {
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
            RawEntryHeader::Custom { .. } => EntryType::Custom,
        }
    }
}

impl Header for EnrichedEntryHeader {
    fn is_completed(&self) -> Option<bool> {
        match self {
            EnrichedEntryHeader::PollInputStream { is_completed } => Some(*is_completed),
            EnrichedEntryHeader::OutputStream => None,
            EnrichedEntryHeader::GetState { is_completed } => Some(*is_completed),
            EnrichedEntryHeader::SetState => None,
            EnrichedEntryHeader::ClearState => None,
            EnrichedEntryHeader::Sleep { is_completed } => Some(*is_completed),
            EnrichedEntryHeader::Invoke { is_completed, .. } => Some(*is_completed),
            EnrichedEntryHeader::BackgroundInvoke { .. } => None,
            EnrichedEntryHeader::Awakeable { is_completed } => Some(*is_completed),
            EnrichedEntryHeader::CompleteAwakeable => None,
            EnrichedEntryHeader::Custom { .. } => None,
        }
    }

    fn mark_completed(&mut self) {
        match self {
            EnrichedEntryHeader::PollInputStream { is_completed } => *is_completed = true,
            EnrichedEntryHeader::OutputStream => {}
            EnrichedEntryHeader::GetState { is_completed } => *is_completed = true,
            EnrichedEntryHeader::SetState => {}
            EnrichedEntryHeader::ClearState => {}
            EnrichedEntryHeader::Sleep { is_completed } => *is_completed = true,
            EnrichedEntryHeader::Invoke { is_completed, .. } => *is_completed = true,
            EnrichedEntryHeader::BackgroundInvoke { .. } => {}
            EnrichedEntryHeader::Awakeable { is_completed } => *is_completed = true,
            EnrichedEntryHeader::CompleteAwakeable => {}
            EnrichedEntryHeader::Custom { .. } => {}
        }
    }

    fn to_entry_type(&self) -> EntryType {
        match self {
            EnrichedEntryHeader::PollInputStream { .. } => EntryType::PollInputStream,
            EnrichedEntryHeader::OutputStream => EntryType::OutputStream,
            EnrichedEntryHeader::GetState { .. } => EntryType::GetState,
            EnrichedEntryHeader::SetState => EntryType::SetState,
            EnrichedEntryHeader::ClearState => EntryType::ClearState,
            EnrichedEntryHeader::Sleep { .. } => EntryType::Sleep,
            EnrichedEntryHeader::Invoke { .. } => EntryType::Invoke,
            EnrichedEntryHeader::BackgroundInvoke { .. } => EntryType::BackgroundInvoke,
            EnrichedEntryHeader::Awakeable { .. } => EntryType::Awakeable,
            EnrichedEntryHeader::CompleteAwakeable => EntryType::CompleteAwakeable,
            EnrichedEntryHeader::Custom { .. } => EntryType::Custom,
        }
    }
}

impl From<EnrichedEntryHeader> for RawEntryHeader {
    fn from(value: EnrichedEntryHeader) -> Self {
        match value {
            EnrichedEntryHeader::PollInputStream { is_completed } => {
                RawEntryHeader::PollInputStream { is_completed }
            }
            EnrichedEntryHeader::OutputStream => RawEntryHeader::OutputStream,
            EnrichedEntryHeader::GetState { is_completed } => {
                RawEntryHeader::GetState { is_completed }
            }
            EnrichedEntryHeader::SetState => RawEntryHeader::SetState,
            EnrichedEntryHeader::ClearState => RawEntryHeader::ClearState,
            EnrichedEntryHeader::Sleep { is_completed } => RawEntryHeader::Sleep { is_completed },
            EnrichedEntryHeader::Invoke { is_completed, .. } => {
                RawEntryHeader::Invoke { is_completed }
            }
            EnrichedEntryHeader::BackgroundInvoke { .. } => RawEntryHeader::BackgroundInvoke,
            EnrichedEntryHeader::Awakeable { is_completed } => {
                RawEntryHeader::Awakeable { is_completed }
            }
            EnrichedEntryHeader::CompleteAwakeable => RawEntryHeader::CompleteAwakeable,
            EnrichedEntryHeader::Custom { code, requires_ack } => {
                RawEntryHeader::Custom { code, requires_ack }
            }
        }
    }
}

pub type PlainRawEntry = RawEntry<RawEntryHeader>;

#[derive(Debug, thiserror::Error)]
#[error("Cannot decode {ty:?}. {kind:?}")]
pub struct RawEntryCodecError {
    ty: EntryType,
    kind: ErrorKind,
}

impl RawEntryCodecError {
    pub fn new(ty: EntryType, kind: ErrorKind) -> Self {
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
    fn serialize_as_unary_input_entry(input_message: Bytes) -> PlainRawEntry;

    fn deserialize<H: Header>(entry: &RawEntry<H>) -> Result<Entry, RawEntryCodecError>;

    fn write_completion<H: Header>(
        entry: &mut RawEntry<H>,
        completion_result: CompletionResult,
    ) -> Result<(), RawEntryCodecError>;
}

pub trait Header {
    fn is_completed(&self) -> Option<bool>;

    fn mark_completed(&mut self);

    fn to_entry_type(&self) -> EntryType;
}
