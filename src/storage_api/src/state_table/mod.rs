use crate::{GetFuture, GetStream, PutFuture};
use bytes::Bytes;
use restate_types::identifiers::ServiceId;

pub trait StateTable {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> GetFuture<Option<Bytes>>;

    fn get_all_user_states(&mut self, service_id: &ServiceId) -> GetStream<(Bytes, Bytes)>;
}
