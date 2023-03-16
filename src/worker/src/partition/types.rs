use bytes::Bytes;
use bytestring::ByteString;
use common::types::{EntryIndex, InvocationId, ServiceInvocationId};
use invoker::InvokerError;
use journal::raw::{Header, RawEntry, RawEntryHeader};
use journal::EntryType;
use std::collections::HashSet;

#[derive(Debug)]
pub(crate) struct InvokerEffect {
    pub(crate) service_invocation_id: ServiceInvocationId,
    pub(crate) kind: InvokerEffectKind,
}

impl InvokerEffect {
    pub(super) fn new(service_invocation_id: ServiceInvocationId, kind: InvokerEffectKind) -> Self {
        Self {
            service_invocation_id,
            kind,
        }
    }
}

#[derive(Debug)]
pub(crate) enum InvokerEffectKind {
    JournalEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    },
    Suspended {
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    End,
    Failed {
        error_code: i32,
        error: InvokerError,
    },
}

pub(crate) type EnrichedRawEntry = RawEntry<EnrichedEntryHeader>;

/// Enriched variant of the [`RawEntryHeader`] to store additional runtime specific information
/// for the journal entries.
#[derive(Debug, Clone)]
pub(crate) enum EnrichedEntryHeader {
    PollInputStream {
        is_completed: bool,
    },
    OutputStream,
    GetState {
        is_completed: bool,
    },
    SetState,
    ClearState,
    Sleep {
        is_completed: bool,
    },
    Invoke {
        is_completed: bool,
        // None if invoke entry is completed by service endpoint
        resolution_result: Option<ResolutionResult>,
    },
    BackgroundInvoke {
        resolution_result: ResolutionResult,
    },
    Awakeable {
        is_completed: bool,
    },
    CompleteAwakeable,
    Custom {
        code: u16,
        requires_ack: bool,
    },
}

/// Result of the target service resolution
#[derive(Debug, Clone)]
pub(crate) enum ResolutionResult {
    Success {
        invocation_id: InvocationId,
        service_key: Bytes,
    },
    Failure {
        error_code: i32,
        error: ByteString,
    },
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

#[derive(Debug)]
pub(crate) struct TimerEffect {
    pub service_invocation_id: ServiceInvocationId,
    pub entry_index: EntryIndex,
    pub timestamp: u64,
}
