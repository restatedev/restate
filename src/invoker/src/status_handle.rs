use codederror::Code;
use restate_common::errors::{InvocationError, InvocationErrorCode};
use restate_common::types::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, ServiceInvocationId};
use std::fmt;
use std::future::Future;
use std::time::SystemTime;

// -- Status data structure

#[derive(Debug)]
pub struct InvocationStatusReport(
    pub(crate) ServiceInvocationId,
    pub(crate) PartitionLeaderEpoch,
    pub(crate) Arc<InvocationStatusReportInner>,
);

impl InvocationStatusReport {
    pub fn service_invocation_id(&self) -> &ServiceInvocationId {
        &self.0
    }

    pub fn partition_id(&self) -> PartitionId {
        self.1 .0
    }

    pub fn leader_epoch(&self) -> LeaderEpoch {
        self.1 .1
    }

    pub fn in_flight(&self) -> bool {
        self.2.in_flight
    }

    pub fn retry_count(&self) -> usize {
        self.2.start_count
    }

    pub fn last_start_at(&self) -> SystemTime {
        self.2.last_start_at
    }

    pub fn last_retry_attempt_failure(&self) -> Option<&InvocationErrorReport> {
        self.2.last_retry_attempt_failure.as_ref()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct InvocationStatusReportInner {
    pub(crate) in_flight: bool,
    pub(crate) start_count: usize,
    pub(crate) last_start_at: SystemTime,
    pub(crate) last_retry_attempt_failure: Option<InvocationErrorReport>,
}

impl Default for InvocationStatusReportInner {
    fn default() -> Self {
        Self {
            in_flight: false,
            start_count: 0,
            last_start_at: SystemTime::now(),
            last_retry_attempt_failure: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct InvocationErrorReport {
    pub(crate) err: InvocationError,
    pub(crate) doc_error_code: Option<&'static Code>,
}

impl InvocationErrorReport {
    pub fn invocation_error_code(&self) -> InvocationErrorCode {
        self.err.code()
    }

    pub fn doc_error_code(&self) -> Option<&'static Code> {
        self.doc_error_code
    }

    pub fn display_err(&self) -> impl fmt::Debug + '_ {
        &self.err
    }
}

/// Struct to access the status of the invocations currently handled by the invoker
pub trait StatusHandle {
    type Iterator: Iterator<Item = InvocationStatusReport>;
    type Future: Future<Output = Self::Iterator>;

    /// This method returns a snapshot of the status of all the invocations currently being processed by this invoker.
    ///
    /// The data returned by this method is eventually consistent.
    fn read_status(&self) -> Self::Future;
}
