use crate::keys::{define_table_key, TableKey};
use crate::TableKind::State;
use crate::{GetFuture, PutFuture, RocksDBTransaction};
use crate::{Result, TableScan, TableScanIterationDecision};
use bytes::Bytes;
use bytestring::ByteString;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::{ready, GetStream, StorageError};
use restate_types::identifiers::{PartitionKey, ServiceId};

define_table_key!(
    State,
    StateKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes,
        state_key: Bytes
    )
);

#[inline]
fn write_state_entry_key(service_id: &ServiceId, state_key: impl AsRef<[u8]>) -> StateKey {
    StateKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .state_key(state_key.as_ref().to_vec().into())
}

fn user_state_key_from_slice(key: &[u8]) -> Result<Bytes> {
    let mut key = Bytes::copy_from_slice(key);
    let key = StateKey::deserialize_from(&mut key)?;
    let key = key
        .state_key
        .ok_or_else(|| StorageError::DataIntegrityError)?;

    Ok(key)
}

impl StateTable for RocksDBTransaction {
    fn put_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture {
        let key = write_state_entry_key(service_id, state_key);
        self.put_kv(key, state_value.as_ref());
        ready()
    }

    fn delete_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> PutFuture {
        let key = write_state_entry_key(service_id, state_key);
        self.delete_key(&key);
        ready()
    }

    fn get_user_state(
        &mut self,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> GetFuture<Option<Bytes>> {
        let key = write_state_entry_key(service_id, state_key);
        self.get_blocking(key, move |_k, v| Ok(v.map(Bytes::copy_from_slice)))
    }

    fn get_all_user_states(&mut self, service_id: &ServiceId) -> GetStream<(Bytes, Bytes)> {
        let key = StateKey::default()
            .partition_key(service_id.partition_key())
            .service_name(service_id.service_name.clone())
            .service_key(service_id.key.clone());

        self.for_each_key_value(TableScan::KeyPrefix(key), |k, v| {
            TableScanIterationDecision::Emit(decode_user_state_key_value(k, v))
        })
    }
}

fn decode_user_state_key_value(k: &[u8], v: &[u8]) -> Result<(Bytes, Bytes)> {
    let user_key = user_state_key_from_slice(k)?;
    let user_value = Bytes::copy_from_slice(v);
    Ok((user_key, user_value))
}

#[cfg(test)]
mod tests {
    use crate::keys::TableKey;
    use crate::state_table::{user_state_key_from_slice, write_state_entry_key};
    use bytes::{Bytes, BytesMut};
    use restate_types::identifiers::ServiceId;

    static EMPTY: Bytes = Bytes::from_static(b"");

    fn state_entry_key(service_id: &ServiceId, state_key: &Bytes) -> BytesMut {
        write_state_entry_key(service_id, state_key).serialize()
    }

    #[test]
    fn keys_sort_services() {
        assert!(
            state_entry_key(&ServiceId::with_partition_key(1337, "svc-1", ""), &EMPTY)
                < state_entry_key(&ServiceId::with_partition_key(1337, "svc-2", ""), &EMPTY)
        );
    }

    #[test]
    fn keys_sort_same_services_but_different_keys() {
        assert!(
            state_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "a"), &EMPTY)
                < state_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "b"), &EMPTY)
        );
    }

    #[test]
    fn keys_sort_same_services_and_keys_but_different_states() {
        let a = state_entry_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            &Bytes::from_static(b"a"),
        );
        let b = state_entry_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            &Bytes::from_static(b"b"),
        );
        assert!(a < b);
    }

    #[test]
    fn user_state_key_can_be_extracted() {
        let a = state_entry_key(
            &ServiceId::with_partition_key(1337, "svc-1", "key-a"),
            &Bytes::from_static(b"seen_count"),
        );

        assert_eq!(
            user_state_key_from_slice(&a).unwrap(),
            Bytes::from_static(b"seen_count")
        );
    }
}
