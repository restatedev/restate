// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::Code;
use restate_types::errors::{InvocationError, InvocationErrorCode};
use restate_types::identifiers::{DeploymentId, FullInvocationId, PartitionKey};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionLeaderEpoch};
use std::fmt;
use std::future::Future;
use std::ops::RangeInclusive;
use std::time::SystemTime;

// -- Status data structure

#[derive(Debug, Clone)]
pub struct InvocationStatusReportInner {
    pub in_flight: bool,
    pub start_count: usize,
    pub last_start_at: SystemTime,
    pub last_retry_attempt_failure: Option<InvocationErrorReport>,
    pub next_retry_at: Option<SystemTime>,
    pub last_attempt_deployment_id: Option<DeploymentId>,
}

impl Default for InvocationStatusReportInner {
    fn default() -> Self {
        Self {
            in_flight: false,
            start_count: 0,
            last_start_at: SystemTime::now(),
            last_retry_attempt_failure: None,
            next_retry_at: None,
            last_attempt_deployment_id: None,
        }
    }
}

#[derive(Debug)]
pub struct InvocationStatusReport(
    FullInvocationId,
    PartitionLeaderEpoch,
    InvocationStatusReportInner,
);

impl InvocationStatusReport {
    pub fn new(
        fid: FullInvocationId,
        partition: PartitionLeaderEpoch,
        report: InvocationStatusReportInner,
    ) -> Self {
        Self(fid, partition, report)
    }

    pub fn full_invocation_id(&self) -> &FullInvocationId {
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

    pub fn next_retry_at(&self) -> Option<SystemTime> {
        self.2.next_retry_at
    }

    pub fn last_retry_attempt_failure(&self) -> Option<&InvocationErrorReport> {
        self.2.last_retry_attempt_failure.as_ref()
    }

    pub fn last_attempt_deployment_id(&self) -> Option<&DeploymentId> {
        self.2.last_attempt_deployment_id.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct InvocationErrorReport {
    err: InvocationError,
    doc_error_code: Option<&'static Code>,
}

impl InvocationErrorReport {
    pub fn new(err: InvocationError, doc_error_code: Option<&'static Code>) -> Self {
        InvocationErrorReport {
            err,
            doc_error_code,
        }
    }

    pub fn invocation_error_code(&self) -> InvocationErrorCode {
        self.err.code()
    }

    pub fn doc_error_code(&self) -> Option<&'static Code> {
        self.doc_error_code
    }

    pub fn display_err(&self) -> impl fmt::Display + '_ {
        &self.err
    }
}

/// Struct to access the status of the invocations currently handled by the invoker
pub trait StatusHandle {
    type Iterator: Iterator<Item = InvocationStatusReport> + Send;
    type Future: Future<Output = Self::Iterator> + Send;

    /// This method returns a snapshot of the status of all the invocations currently being processed by this invoker,
    /// filtered by the partition key range
    ///
    /// The data returned by this method is eventually consistent.
    fn read_status(&self, keys: RangeInclusive<PartitionKey>) -> Self::Future;
}
