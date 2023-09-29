// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use restate_invoker_api::status_handle::{InvocationStatusReport, InvocationStatusReportInner};

use std::time::SystemTime;

#[derive(Default, Debug)]
pub(super) struct InvocationStatusStore(
    HashMap<PartitionLeaderEpoch, HashMap<FullInvocationId, InvocationStatusReportInner>>,
);

impl InvocationStatusStore {
    pub(super) fn status_for_partition(
        &self,
        partition_leader_epoch: PartitionLeaderEpoch,
    ) -> impl Iterator<Item = InvocationStatusReport> + '_ {
        self.0
            .get(&partition_leader_epoch)
            .into_iter()
            .flat_map(move |hash| {
                hash.iter().map(move |(fid, report)| {
                    InvocationStatusReport::new(fid.clone(), partition_leader_epoch, report.clone())
                })
            })
    }

    // -- Methods used by the invoker to notify the status

    pub(super) fn on_start(&mut self, partition: PartitionLeaderEpoch, fid: FullInvocationId) {
        let report = self
            .0
            .entry(partition)
            .or_insert_with(Default::default)
            .entry(fid)
            .or_insert_with(Default::default);
        report.start_count += 1;
        report.last_start_at = SystemTime::now();
        report.in_flight = true;
    }

    pub(super) fn on_end(&mut self, partition: &PartitionLeaderEpoch, fid: &FullInvocationId) {
        if let Some(inner) = self.0.get_mut(partition) {
            inner.remove(fid);
            if inner.is_empty() {
                self.0.remove(partition);
            }
        }
    }

    pub(super) fn on_failure(
        &mut self,
        partition: PartitionLeaderEpoch,
        fid: FullInvocationId,
        reason: InvocationErrorReport,
    ) {
        let report = self
            .0
            .entry(partition)
            .or_insert_with(Default::default)
            .entry(fid)
            .or_insert_with(Default::default);
        report.in_flight = false;
        report.last_retry_attempt_failure = Some(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl InvocationStatusStore {
        pub fn resolve_invocation(
            &self,
            partition: PartitionLeaderEpoch,
            fid: &FullInvocationId,
        ) -> Option<InvocationStatusReport> {
            self.0.get(&partition).and_then(|inner| {
                inner.get(fid).map(|report| {
                    InvocationStatusReport::new(fid.clone(), partition, report.clone())
                })
            })
        }
    }
}
