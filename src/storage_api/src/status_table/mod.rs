use crate::{GetFuture, GetStream, PutFuture};
use common::types::{InvocationStatus, PartitionKey, ServiceId, ServiceInvocationId};
use std::ops::RangeInclusive;

pub trait StatusTable {
    fn put_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> PutFuture;

    fn get_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetFuture<Option<InvocationStatus>>;

    fn delete_invocation_status(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> PutFuture;

    fn invoked_invocations(
        &mut self,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> GetStream<ServiceInvocationId>;
}
