use crate::composite_keys::{read_delimited, skip_delimited, write_delimited};
use crate::Result;
use crate::TableKind::State;
use crate::{GetFuture, PutFuture, RocksDBTransaction};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytestring::ByteString;
use restate_common::types::{PartitionKey, ServiceId};
use restate_storage_api::state_table::StateTable;
use restate_storage_api::{ready, GetStream};

#[derive(Debug, PartialEq)]
pub struct StateKeyComponents {
    pub partition_key: Option<PartitionKey>,
    pub service_name: Option<ByteString>,
    pub service_key: Option<Bytes>,
    pub state_key: Option<Bytes>,
}

impl StateKeyComponents {
    pub(crate) fn to_bytes(&self, bytes: &mut BytesMut) -> Option<()> {
        self.partition_key
            .map(|partition_key| bytes.put_u64(partition_key))?;
        self.service_name
            .as_ref()
            .map(|s| write_delimited(s, bytes))?;
        self.service_key
            .as_ref()
            .map(|s| write_delimited(s, bytes))?;
        self.state_key.as_ref().map(|s| write_delimited(s, bytes))
    }

    pub(crate) fn from_bytes(bytes: &mut Bytes) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            partition_key: bytes.has_remaining().then(|| bytes.get_u64()),
            service_name: bytes
                .has_remaining()
                .then(|| {
                    read_delimited(bytes)
                        // SAFETY: this is safe since the service name was constructed from a ByteString.
                        .map(|bytes| unsafe { ByteString::from_bytes_unchecked(bytes) })
                })
                .transpose()?,
            service_key: bytes
                .has_remaining()
                .then(|| read_delimited(bytes))
                .transpose()?,
            state_key: bytes
                .has_remaining()
                .then(|| read_delimited(bytes))
                .transpose()?,
        })
    }
}

#[test]
fn key_round_trip() {
    let key = StateKeyComponents {
        partition_key: Some(1),
        service_name: Some(ByteString::from("name")),
        service_key: Some(Bytes::from("key")),
        state_key: Some(Bytes::from("key")),
    };
    let mut bytes = BytesMut::new();
    key.to_bytes(&mut bytes);
    assert_eq!(
        bytes,
        BytesMut::from(b"\0\0\0\0\0\0\0\x01\x04name\x03key\x03key".as_slice())
    );
    assert_eq!(
        StateKeyComponents::from_bytes(&mut bytes.freeze()).expect("key parsing failed"),
        key
    );
}

#[inline]
fn write_state_entry_key(
    key: &mut BytesMut,
    partition_key: PartitionKey,
    service_id: &ServiceId,
    state_key: impl AsRef<[u8]>,
) {
    key.put_u64(partition_key);
    write_delimited(&service_id.service_name, key);
    write_delimited(&service_id.key, key);
    write_delimited(state_key, key);
}

#[inline]
fn write_states_key(key: &mut BytesMut, partition_key: PartitionKey, service_id: &ServiceId) {
    key.put_u64(partition_key);
    write_delimited(&service_id.service_name, key);
    write_delimited(&service_id.key, key);
}

#[inline]
fn user_state_key_from_slice(key: &[u8]) -> crate::Result<Bytes> {
    let mut key = Bytes::copy_from_slice(key);
    _ = key.get_u64(); // partition_key
    skip_delimited(&mut key)?; // service_name
    skip_delimited(&mut key)?; // key
    let user_key = read_delimited(&mut key)?;

    Ok(user_key)
}

impl StateTable for RocksDBTransaction {
    fn put_user_state(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture {
        write_state_entry_key(self.key_buffer(), partition_key, service_id, state_key);
        self.put_value_using_key_buffer(State, state_value);
        ready()
    }

    fn delete_user_state(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> PutFuture {
        write_state_entry_key(self.key_buffer(), partition_key, service_id, state_key);
        self.delete_key_buffer(State);
        ready()
    }

    fn get_user_state(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        state_key: impl AsRef<[u8]>,
    ) -> GetFuture<Option<Bytes>> {
        write_state_entry_key(self.key_buffer(), partition_key, service_id, state_key);
        let key = self.clone_key_buffer();

        self.spawn_blocking(move |db| db.get_owned(State, key))
    }

    fn get_all_user_states(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetStream<(Bytes, Bytes)> {
        write_states_key(self.key_buffer(), partition_key, service_id);
        let key = self.clone_key_buffer();

        self.spawn_background_scan(move |db, tx| {
            let mut iterator = db.prefix_iterator(State, key.clone());
            iterator.seek(&key);
            while let Some((k, v)) = iterator.item() {
                let res = decode_user_state_key_value(k, v);
                if tx.blocking_send(res).is_err() {
                    break;
                }
                iterator.next();
            }
        })
    }
}

fn decode_user_state_key_value(k: &[u8], v: &[u8]) -> crate::Result<(Bytes, Bytes)> {
    let user_key = user_state_key_from_slice(k)?;
    let user_value = Bytes::copy_from_slice(v);

    Ok((user_key, user_value))
}

#[cfg(test)]
mod tests {
    use crate::state_table::{user_state_key_from_slice, write_state_entry_key, write_states_key};
    use bytes::{Bytes, BytesMut};
    use restate_common::types::{PartitionKey, ServiceId};

    static EMPTY: Bytes = Bytes::from_static(b"");

    #[inline]
    fn state_entry_key(
        partition_key: PartitionKey,
        service_id: &ServiceId,
        state_key: &Bytes,
    ) -> BytesMut {
        let mut key = BytesMut::new();
        write_state_entry_key(&mut key, partition_key, service_id, state_key);
        key
    }

    #[inline]
    fn states_key(partition_key: PartitionKey, service_id: &ServiceId) -> BytesMut {
        let mut key = BytesMut::new();
        write_states_key(&mut key, partition_key, service_id);
        key
    }

    #[test]
    fn key_covers_all_entries_of_a_service() {
        let prefix_key = states_key(1337, &ServiceId::new("svc-1", "key-a"));

        let low_key = state_entry_key(1337, &ServiceId::new("svc-1", "key-a"), &EMPTY);
        assert!(low_key.starts_with(&prefix_key));

        let high_key = state_entry_key(1337, &ServiceId::new("svc-1", "key-a"), &EMPTY);
        assert!(high_key.starts_with(&prefix_key));
    }

    #[test]
    fn keys_sort_services() {
        assert!(
            state_entry_key(1337, &ServiceId::new("svc-1", ""), &EMPTY)
                < state_entry_key(1337, &ServiceId::new("svc-2", ""), &EMPTY)
        );
    }

    #[test]
    fn keys_sort_same_services_but_different_keys() {
        assert!(
            state_entry_key(1337, &ServiceId::new("svc-1", "a"), &EMPTY)
                < state_entry_key(1337, &ServiceId::new("svc-1", "b"), &EMPTY)
        );
    }

    #[test]
    fn keys_sort_same_services_and_keys_but_different_states() {
        let a = state_entry_key(
            1337,
            &ServiceId::new("svc-1", "key-a"),
            &Bytes::from_static(b"a"),
        );
        let b = state_entry_key(
            1337,
            &ServiceId::new("svc-1", "key-a"),
            &Bytes::from_static(b"b"),
        );
        assert!(a < b);
    }

    #[test]
    fn user_state_key_can_be_extracted() {
        let a = state_entry_key(
            1337,
            &ServiceId::new("svc-1", "key-a"),
            &Bytes::from_static(b"seen_count"),
        );

        assert_eq!(
            user_state_key_from_slice(&a).unwrap(),
            Bytes::from_static(b"seen_count")
        );
    }
}
