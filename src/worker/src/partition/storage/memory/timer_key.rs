use common::types::{EntryIndex, ServiceId};
use std::borrow::Borrow;
use std::cmp::Ordering;

#[derive(Debug, Eq, Clone, PartialEq)]
pub struct TimerKey {
    wake_up_time: u64,
    service_id: Option<ServiceId>,
    entry_index: Option<EntryIndex>,
}

impl TimerKey {
    pub fn new(wake_up_time: u64, service_id: ServiceId, entry_index: EntryIndex) -> Self {
        Self {
            wake_up_time,
            service_id: Some(service_id),
            entry_index: Some(entry_index),
        }
    }

    pub fn from_wake_up_time(wake_up_time: u64) -> Self {
        Self {
            wake_up_time,
            service_id: None,
            entry_index: None,
        }
    }

    pub fn into_inner(self) -> (u64, Option<ServiceId>, Option<EntryIndex>) {
        (self.wake_up_time, self.service_id, self.entry_index)
    }
}

impl PartialOrd for TimerKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimerKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wake_up_time
            .cmp(&other.wake_up_time)
            .then_with(|| self.service_id.cmp(&other.service_id))
            .then_with(|| self.entry_index.cmp(&other.entry_index))
    }
}

pub trait TimerKeyRef {
    fn service_id(&self) -> Option<&ServiceId>;
    fn wake_up_time(&self) -> u64;
    fn entry_index(&self) -> Option<EntryIndex>;
}

impl TimerKeyRef for TimerKey {
    fn service_id(&self) -> Option<&ServiceId> {
        self.service_id.as_ref()
    }

    fn wake_up_time(&self) -> u64 {
        self.wake_up_time
    }

    fn entry_index(&self) -> Option<EntryIndex> {
        self.entry_index
    }
}

impl PartialEq for dyn TimerKeyRef + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.service_id().eq(&other.service_id())
            && self.wake_up_time() == other.wake_up_time()
            && self.entry_index() == other.entry_index()
    }
}

impl Eq for dyn TimerKeyRef + '_ {}

impl PartialOrd for dyn TimerKeyRef + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn TimerKeyRef + '_ {
    fn cmp(&self, other: &Self) -> Ordering {
        self.wake_up_time()
            .cmp(&other.wake_up_time())
            .then_with(|| self.service_id().cmp(&other.service_id()))
            .then_with(|| self.entry_index().cmp(&other.entry_index()))
    }
}

impl<'a> Borrow<dyn TimerKeyRef + 'a> for TimerKey {
    fn borrow(&self) -> &(dyn TimerKeyRef + 'a) {
        self
    }
}

impl TimerKeyRef for (&ServiceId, u64, EntryIndex) {
    fn service_id(&self) -> Option<&ServiceId> {
        Some(self.0)
    }

    fn wake_up_time(&self) -> u64 {
        self.1
    }

    fn entry_index(&self) -> Option<EntryIndex> {
        Some(self.2)
    }
}
