use common::types::{EntryIndex, ServiceId};
use std::borrow::Borrow;
use std::cmp::Ordering;

#[derive(Debug, Eq, Clone, PartialEq)]
pub struct TimerKey {
    service_id: ServiceId,
    wake_up_time: u64,
    entry_index: EntryIndex,
}

impl TimerKey {
    pub fn new(service_id: ServiceId, wake_up_time: u64, entry_index: EntryIndex) -> Self {
        Self {
            service_id,
            wake_up_time,
            entry_index,
        }
    }

    pub fn into_inner(self) -> (ServiceId, u64, EntryIndex) {
        (self.service_id, self.wake_up_time, self.entry_index)
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
    fn service_id(&self) -> &ServiceId;
    fn wake_up_time(&self) -> u64;
    fn entry_index(&self) -> EntryIndex;
}

impl TimerKeyRef for TimerKey {
    fn service_id(&self) -> &ServiceId {
        &self.service_id
    }

    fn wake_up_time(&self) -> u64 {
        self.wake_up_time
    }

    fn entry_index(&self) -> EntryIndex {
        self.entry_index
    }
}

impl PartialEq for dyn TimerKeyRef + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.service_id().eq(other.service_id())
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
            .then_with(|| self.service_id().cmp(other.service_id()))
            .then_with(|| self.entry_index().cmp(&other.entry_index()))
    }
}

impl<'a> Borrow<dyn TimerKeyRef + 'a> for TimerKey {
    fn borrow(&self) -> &(dyn TimerKeyRef + 'a) {
        self
    }
}

impl TimerKeyRef for (&ServiceId, u64, EntryIndex) {
    fn service_id(&self) -> &ServiceId {
        self.0
    }

    fn wake_up_time(&self) -> u64 {
        self.1
    }

    fn entry_index(&self) -> EntryIndex {
        self.2
    }
}
