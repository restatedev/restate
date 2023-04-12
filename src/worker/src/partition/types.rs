use common::types::{
    EntryIndex, MillisSinceEpoch, RawEntry, ResolutionResult, ServiceInvocationId,
};
use invoker::InvokerError;
use journal::raw::{Header, RawEntryHeader};
use journal::EntryType;
use std::cmp::Ordering;
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

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub(crate) struct TimerValue {
    pub service_invocation_id: ServiceInvocationId,
    pub wake_up_time: MillisSinceEpoch,
    pub entry_index: EntryIndex,
}

impl TimerValue {
    pub(crate) fn new(
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        wake_up_time: MillisSinceEpoch,
    ) -> Self {
        Self {
            service_invocation_id,
            entry_index,
            wake_up_time,
        }
    }
}

impl PartialOrd for TimerValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wake_up_time
            .cmp(&other.wake_up_time)
            .then_with(|| {
                self.service_invocation_id
                    .service_id
                    .cmp(&other.service_invocation_id.service_id)
            })
            .then_with(|| self.entry_index.cmp(&other.entry_index))
            .then_with(|| {
                self.service_invocation_id
                    .invocation_id
                    .cmp(&other.service_invocation_id.invocation_id)
            })
    }
}

impl timer::Timer for TimerValue {
    type TimerKey = TimerValue;

    fn timer_key(&self) -> Self::TimerKey {
        self.clone()
    }
}

impl timer::TimerKey for TimerValue {
    fn wake_up_time(&self) -> MillisSinceEpoch {
        self.wake_up_time
    }
}
