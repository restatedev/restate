use bytes::Bytes;
use common::types::{EntryIndex, Response, ServiceId, ServiceInvocation, ServiceInvocationId};
use journal::raw::RawEntry;
use journal::Completion;

pub(crate) enum OutboxMessage {
    Invocation(ServiceInvocation),
    Response(Response),
}

#[derive(Debug, Default)]
pub(crate) struct Effects;

impl Effects {
    pub(crate) fn clear(&mut self) {}

    pub(crate) fn invoke_service(&mut self, service_invocation: ServiceInvocation) {}

    pub(crate) fn resume_service(&mut self, service_id: ServiceId) {}

    pub(crate) fn suspend_service(&mut self, service_id: ServiceId) {}

    pub(crate) fn enqueue_into_inbox(
        &mut self,
        seq_number: u64,
        service_invocation: ServiceInvocation,
    ) {
    }

    pub(crate) fn enqueue_into_outbox(&mut self, seq_number: u64, message: OutboxMessage) {}

    pub(crate) fn set_state(&mut self, service_id: ServiceId, key: Bytes, value: Bytes) {}

    pub(crate) fn clear_state(&mut self, service_id: ServiceId, key: Bytes) {}

    pub(crate) fn register_timer(
        &mut self,
        wake_up_time: i64,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) {
    }

    pub(crate) fn delete_timer(
        &mut self,
        wake_up_time: i64,
        service_id: ServiceId,
        entry_index: EntryIndex,
    ) {
    }

    pub(crate) fn append_awakeable_entry(
        &mut self,
        service_id: ServiceId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) {
    }

    pub(crate) fn append_journal_entry(
        &mut self,
        service_id: ServiceId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) {
    }

    pub(crate) fn truncate_outbox(&mut self, index: u64) {}

    pub(crate) fn store_and_forward_completion(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
    }

    pub(crate) fn store_completion(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
    }

    pub(crate) fn drop_journal(&mut self, service_id: ServiceId) {}

    pub(crate) fn pop_inbox(&mut self, service_id: ServiceId) {}

    pub(crate) fn mark_service_instance_as_free(&mut self, service_id: ServiceId) {}
}
