use crate::raw::{Header, PlainRawEntry, RawEntryHeader};
use crate::EntryType;
use restate_common::types::ServiceInvocationSpanContext;

// Re-exports that should be moved here. See https://github.com/restatedev/restate/issues/420
pub use restate_common::types::{EnrichedEntryHeader, EnrichedRawEntry};

pub trait EntryEnricher {
    fn enrich_entry(
        &self,
        entry: PlainRawEntry,
        invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, anyhow::Error>;
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
