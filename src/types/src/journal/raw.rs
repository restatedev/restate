//! Raw entries carry the serialized representation of entries.

use super::*;

use std::fmt::Debug;

pub type PlainRawEntry = RawEntry<RawEntryHeader>;

/// Defines a [RawEntry] header.
pub trait EntryHeader {
    fn is_completed(&self) -> Option<bool>;

    fn mark_completed(&mut self);

    fn to_entry_type(&self) -> EntryType;
}

/// This struct represents a serialized journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEntry<H> {
    // TODO can we get rid of these pub here?
    pub header: H,
    pub entry: Bytes,
}

impl<H> RawEntry<H> {
    pub const fn new(header: H, entry: Bytes) -> Self {
        Self { header, entry }
    }

    pub fn into_inner(self) -> (H, Bytes) {
        (self.header, self.entry)
    }
}

impl<H: EntryHeader> RawEntry<H> {
    pub fn ty(&self) -> EntryType {
        self.header.to_entry_type()
    }
}

/// This struct represents headers as they are received from the wire.
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

impl EntryHeader for RawEntryHeader {
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

// -- Codec for RawEntry

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
    Decode {
        #[source]
        source: Option<anyhow::Error>,
    },
    #[error("Field '{0}' is missing")]
    MissingField(&'static str),
}

pub trait RawEntryCodec {
    fn serialize_as_unary_input_entry(input_message: Bytes) -> PlainRawEntry;

    fn deserialize<H: EntryHeader>(entry: &RawEntry<H>) -> Result<Entry, RawEntryCodecError>;

    fn write_completion<H: EntryHeader + Debug>(
        entry: &mut RawEntry<H>,
        completion_result: CompletionResult,
    ) -> Result<(), RawEntryCodecError>;
}
