use super::service::{Input, OtherInputCommand};
use super::*;

use codederror::{Code, CodedError};
use restate_common::errors::InvocationError;
use restate_common::types::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, ServiceInvocationId};
use std::fmt;
use std::time::SystemTime;
use tokio::sync::mpsc;

// -- Status data structure

#[derive(Debug)]
pub struct InvocationStatusReport(
    pub(crate) ServiceInvocationId,
    pub(crate) PartitionLeaderEpoch,
    pub(crate) InvocationStatusReportInner,
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
    err: InvocationError,
    doc_error_code: Option<&'static Code>,
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

impl<InvokerCodedError: InvokerError + CodedError> From<&InvokerCodedError>
    for InvocationErrorReport
{
    fn from(value: &InvokerCodedError) -> Self {
        InvocationErrorReport {
            err: value.to_invocation_error(),
            doc_error_code: value.code(),
        }
    }
}

// -- Status reader

/// Struct to access the status of the invocations currently handled by the invoker
pub struct InvokerStatusReader(pub(crate) mpsc::UnboundedSender<Input<OtherInputCommand>>);

impl InvokerStatusReader {
    /// This method returns a snapshot of the status of all the invocations currently being processed by this invoker.
    ///
    /// The data returned by this method is eventually consistent.
    pub async fn read_status(&self) -> impl Iterator<Item = InvocationStatusReport> {
        let (cmd, rx) = restate_futures_util::command::Command::prepare(());
        if self
            .0
            .send(Input {
                // TODO we should perhaps change the data structure here,
                //  as partition has no meaning for this command.
                partition: (0, 0),
                inner: OtherInputCommand::ReadStatus(cmd),
            })
            .is_err()
        {
            return itertools::Either::Left(std::iter::empty());
        }

        if let Ok(status_vec) = rx.await {
            itertools::Either::Right(status_vec.into_iter())
        } else {
            itertools::Either::Left(std::iter::empty())
        }
    }
}
