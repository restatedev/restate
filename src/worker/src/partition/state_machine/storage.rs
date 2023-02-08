use common::types::{InvocationId, ServiceId, ServiceInvocation};
use journal::JournalRevision;
use storage_api::StorageReader;

#[derive(Debug, PartialEq)]
pub(super) enum InvocationStatus {
    Invoked(InvocationId),
    Suspended(InvocationId),
    Free,
}

pub(super) struct StorageHelper {}

impl StorageHelper {
    pub(super) fn new<S: StorageReader>(_storage: &S) -> Self {
        StorageHelper {}
    }

    pub(super) fn invocation_status(&self, service_id: &ServiceId) -> InvocationStatus {
        InvocationStatus::Free
    }

    pub(super) fn peek_inbox(&self, service_id: &ServiceId) -> Option<ServiceInvocation> {
        None
    }

    pub(super) fn journal_revision(&self, service_id: &ServiceId) -> JournalRevision {
        0
    }

    pub(super) fn journal_length(&self, service_id: &ServiceId) -> u32 {
        0
    }
}
