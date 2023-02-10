use futures::{Stream, stream};
use common::types::{InvocationId, ServiceId, ServiceInvocation, ServiceInvocationId};
use journal::JournalRevision;
use storage_api::StorageReader;

#[derive(Debug, PartialEq)]
pub(super) enum InvocationStatus {
    Invoked(InvocationId),
    Suspended(InvocationId),
    Free,
}

pub(super) struct StorageReaderHelper {}

impl StorageReaderHelper {
    pub(super) fn new<S: StorageReader>(_storage: &S) -> Self {
        StorageReaderHelper {}
    }

    pub(super) fn get_invocation_status(&self, service_id: &ServiceId) -> InvocationStatus {
        InvocationStatus::Free
    }

    pub(super) fn peek_inbox(&self, service_id: &ServiceId) -> Option<ServiceInvocation> {
        None
    }

    pub(super) fn get_journal_revision(&self, service_id: &ServiceId) -> JournalRevision {
        0
    }

    pub(super) fn get_journal_length(&self, service_id: &ServiceId) -> u32 {
        0
    }

    pub fn scan_invoked_invocations(&self) -> impl Stream<Item = ServiceInvocationId> {
        stream::empty()
    }
}
