use restate_common::types::{
    EnrichedRawEntry, EntryIndex, MillisSinceEpoch, ServiceInvocation, ServiceInvocationId, Timer,
};
use restate_invoker::InvokerError;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

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

#[derive(Debug, Clone)]
pub(crate) struct TimerValue {
    pub service_invocation_id: ServiceInvocationId,
    pub wake_up_time: MillisSinceEpoch,
    pub entry_index: EntryIndex,
    pub value: Timer,
}

impl TimerValue {
    pub(crate) fn new_sleep(
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> Self {
        Self {
            service_invocation_id,
            wake_up_time,
            entry_index,
            value: Timer::CompleteSleepEntry,
        }
    }

    pub(crate) fn new_invoke(
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        service_invocation: ServiceInvocation,
    ) -> Self {
        Self {
            service_invocation_id,
            wake_up_time,
            entry_index,
            value: Timer::Invoke(service_invocation),
        }
    }
}

impl Hash for TimerValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.service_invocation_id, state);
        Hash::hash(&self.wake_up_time, state);
        Hash::hash(&self.entry_index, state);
        // We don't hash the value field.
    }
}

impl PartialEq for TimerValue {
    fn eq(&self, other: &Self) -> bool {
        self.service_invocation_id == other.service_invocation_id
            && self.wake_up_time == other.wake_up_time
            && self.entry_index == other.entry_index
    }
}

impl Eq for TimerValue {}

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

impl restate_timer::Timer for TimerValue {
    type TimerKey = TimerValue;

    fn timer_key(&self) -> Self::TimerKey {
        self.clone()
    }
}

impl restate_timer::TimerKey for TimerValue {
    fn wake_up_time(&self) -> MillisSinceEpoch {
        self.wake_up_time
    }
}
