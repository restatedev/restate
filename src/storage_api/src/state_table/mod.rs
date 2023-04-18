use crate::{GetFuture, GetStream, PutFuture};
use bytes::Bytes;
use restate_common::types::{PartitionKey, ServiceId};

pub trait StateTable {
    fn put_user_state(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn delete_user_state(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn get_user_state(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> GetFuture<Option<Bytes>>;

    fn get_all_user_states(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetStream<(Bytes, Bytes)>;
}
