use restate_common::types::{
    EnrichedRawEntry, EntryIndex, MillisSinceEpoch, ServiceInvocation, ServiceInvocationId, Timer,
};
use restate_invoker::InvokerError;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
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
        error: Box<dyn InvokerError + Send + Sync + 'static>,
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

    pub(crate) fn display_key(&self) -> TimerKeyDisplay {
        return TimerKeyDisplay(&self.service_invocation_id, &self.entry_index);
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

// We use the TimerKey to read the timers in an absolute order. The timer service
// relies on this order in order to process each timer exactly once. That is the
// reason why the ordering of the TimerValue and how the TimerKey is laid out in
// RocksDB need to be exactly the same.
//
// TODO: https://github.com/restatedev/restate/issues/394
impl Ord for TimerValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wake_up_time
            .cmp(&other.wake_up_time)
            .then_with(|| {
                let service_id = &self.service_invocation_id.service_id;
                let invocation_id = &self.service_invocation_id.invocation_id;

                let other_service_id = &other.service_invocation_id.service_id;
                let other_invocation_id = &other.service_invocation_id.invocation_id;

                service_id
                    .service_name
                    .cmp(&other_service_id.service_name)
                    .then_with(|| service_id.key.cmp(&other_service_id.key))
                    .then_with(|| invocation_id.cmp(other_invocation_id))
            })
            .then_with(|| self.entry_index.cmp(&other.entry_index))
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

// Helper to display timer key
#[derive(Debug)]
pub(crate) struct TimerKeyDisplay<'a>(
    pub(crate) &'a ServiceInvocationId,
    pub(crate) &'a EntryIndex,
);

impl<'a> fmt::Display for TimerKeyDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{:?}][{}]({})",
            self.0.service_id.service_name, self.0.service_id.key, self.0.invocation_id, self.1
        )
    }
}
