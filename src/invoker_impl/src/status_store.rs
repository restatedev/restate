use super::*;

use restate_invoker_api::status_handle::{InvocationStatusReport, InvocationStatusReportInner};
use std::time::SystemTime;

#[derive(Default, Debug)]
pub(super) struct InvocationStatusStore(
    HashMap<PartitionLeaderEpoch, HashMap<ServiceInvocationId, InvocationStatusReportInner>>,
);

impl InvocationStatusStore {
    pub(super) fn iter(&self) -> impl Iterator<Item = InvocationStatusReport> + '_ {
        self.0
            .iter()
            .flat_map(|(partition_leader_epoch, inner_map)| {
                inner_map.iter().map(move |(sid, report)| {
                    InvocationStatusReport::new(
                        sid.clone(),
                        *partition_leader_epoch,
                        report.clone(),
                    )
                })
            })
    }

    // -- Methods used by the invoker to notify the status

    pub(super) fn on_start(&mut self, partition: PartitionLeaderEpoch, sid: ServiceInvocationId) {
        let report = self
            .0
            .entry(partition)
            .or_insert_with(Default::default)
            .entry(sid)
            .or_insert_with(Default::default);
        report.start_count += 1;
        report.last_start_at = SystemTime::now();
        report.in_flight = true;
    }

    pub(super) fn on_end(&mut self, partition: &PartitionLeaderEpoch, sid: &ServiceInvocationId) {
        if let Some(inner) = self.0.get_mut(partition) {
            inner.remove(sid);
            if inner.is_empty() {
                self.0.remove(partition);
            }
        }
    }

    pub(super) fn on_failure(
        &mut self,
        partition: PartitionLeaderEpoch,
        sid: ServiceInvocationId,
        reason: InvocationErrorReport,
    ) {
        let report = self
            .0
            .entry(partition)
            .or_insert_with(Default::default)
            .entry(sid)
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
            sid: &ServiceInvocationId,
        ) -> Option<InvocationStatusReport> {
            self.0.get(&partition).and_then(|inner| {
                inner.get(sid).map(|report| {
                    InvocationStatusReport::new(sid.clone(), partition, report.clone())
                })
            })
        }
    }
}
