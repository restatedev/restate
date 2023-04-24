use crate::composite_keys::{u64_pair, u64_pair_from_slice};
use crate::TableKind::Deduplication;
use crate::{GetFuture, GetStream, PutFuture, RocksDBStorage, RocksDBTransaction};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use restate_common::types::PartitionId;
use restate_storage_api::deduplication_table::DeduplicationTable;
use restate_storage_api::ready;
use tokio::sync::mpsc::Sender;

#[derive(Debug, PartialEq)]
pub struct DeduplicationKeyComponents {
    pub partition_id: Option<PartitionId>,
    pub producing_partition_id: Option<PartitionId>,
}

impl DeduplicationKeyComponents {
    pub(crate) fn to_bytes(&self, bytes: &mut BytesMut) -> Option<()> {
        self.partition_id
            .map(|partition_id| bytes.put_u64(partition_id))?;
        self.producing_partition_id
            .map(|producing_partition_id| bytes.put_u64(producing_partition_id))
    }

    pub(crate) fn from_bytes(bytes: &mut Bytes) -> Self {
        Self {
            partition_id: bytes.has_remaining().then(|| bytes.get_u64()),
            producing_partition_id: bytes.has_remaining().then(|| bytes.get_u64()),
        }
    }
}

#[test]
fn key_round_trip() {
    let key = DeduplicationKeyComponents {
        partition_id: Some(1),
        producing_partition_id: Some(1),
    };
    let mut bytes = BytesMut::new();
    key.to_bytes(&mut bytes);
    assert_eq!(
        bytes,
        BytesMut::from(b"\0\0\0\0\0\0\0\x01\0\0\0\0\0\0\0\x01".as_slice())
    );
    assert_eq!(
        DeduplicationKeyComponents::from_bytes(&mut bytes.freeze()),
        key
    );
}

impl DeduplicationTable for RocksDBTransaction {
    fn get_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
    ) -> GetFuture<Option<u64>> {
        self.spawn_blocking(move |db| {
            let key = u64_pair(partition_id, producing_partition_id);
            let maybe_sequence_number_slice = db.get(Deduplication, key)?;
            let maybe_sequence_number = maybe_sequence_number_slice.map(|slice| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(slice.as_ref());
                u64::from_be_bytes(buf)
            });

            Ok(maybe_sequence_number)
        })
    }

    fn put_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
        sequence_number: u64,
    ) -> PutFuture {
        let key = u64_pair(partition_id, producing_partition_id);
        self.put_kv(Deduplication, key, sequence_number.to_be_bytes());
        ready()
    }

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> GetStream<(PartitionId, u64)> {
        self.spawn_background_scan(move |db, tx| {
            find_all_producing_sequences_for_partition(db, tx, partition_id)
        })
    }
}

fn find_all_producing_sequences_for_partition(
    db: RocksDBStorage,
    tx: Sender<crate::Result<(PartitionId, u64)>>,
    partition_id: PartitionId,
) {
    let start_key = partition_id.to_be_bytes();
    let mut iterator = db.prefix_iterator(Deduplication, start_key);
    iterator.seek(start_key);
    while let Some((k, v)) = iterator.item() {
        // read the second component of the key.
        let (_, producing_partition_id) = u64_pair_from_slice(k);

        // read out the value
        let mut buf = [0u8; 8];
        buf.copy_from_slice(v);
        let sequence_number = u64::from_be_bytes(buf);

        if tx
            .blocking_send(Ok((producing_partition_id, sequence_number)))
            .is_err()
        {
            break;
        }
        iterator.next();
    }
}
