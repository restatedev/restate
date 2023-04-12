use common::types::{EnrichedRawEntry, EntryIndex, MillisSinceEpoch, ServiceInvocationId};
use invoker::InvokerError;
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
