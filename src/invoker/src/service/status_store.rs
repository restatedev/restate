use super::*;

use crate::status::InvocationStatusReportInner;
use std::sync::Weak;

#[derive(Default)]
pub(super) struct InvocationStatusStore(
    HashMap<PartitionLeaderEpoch, HashMap<ServiceInvocationId, Arc<InvocationStatusReportInner>>>,
);

impl InvocationStatusStore {
    pub(super) fn weak_iter(
        &self,
    ) -> impl Iterator<
        Item = (
            ServiceInvocationId,
            PartitionLeaderEpoch,
            Weak<InvocationStatusReportInner>,
        ),
    > + '_ {
        self.0
            .iter()
            .flat_map(|(partition_leader_epoch, inner_map)| {
                inner_map.iter().map(move |(sid, report)| {
                    (sid.clone(), *partition_leader_epoch, Arc::downgrade(report))
                })
            })
    }

    // -- Methods used by the invoker to notify the status

    pub(super) fn on_start(&mut self, partition: PartitionLeaderEpoch, sid: ServiceInvocationId) {
        let report = Arc::make_mut(
            self.0
                .entry(partition)
                .or_insert_with(Default::default)
                .entry(sid)
                .or_insert_with(Default::default),
        );
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
        reason: impl Into<InvocationErrorReport>,
    ) {
        let report = Arc::make_mut(
            self.0
                .entry(partition)
                .or_insert_with(Default::default)
                .entry(sid)
                .or_insert_with(Default::default),
        );
        report.in_flight = false;
        report.last_retry_attempt_failure = Some(reason.into());
    }
}
