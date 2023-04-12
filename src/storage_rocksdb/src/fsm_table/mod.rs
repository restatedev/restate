use crate::composite_keys::{u64_pair, u64_pair_from_slice};
use crate::TableKind::PartitionStateMachine;
use crate::{GetFuture, GetStream, PutFuture, RocksDBStorage, RocksDBTransaction};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use restate_common::types::PartitionId;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::ready;
use tokio::sync::mpsc::Sender;

pub struct PartitionStateMachineKeyComponents {
    pub partition_id: Option<PartitionId>,
    pub state_id: Option<u64>,
}

impl PartitionStateMachineKeyComponents {
    pub(crate) fn to_bytes(&self, bytes: &mut BytesMut) -> Option<()> {
        self.partition_id
            .map(|partition_id| bytes.put_u64(partition_id))?;
        self.state_id.map(|state_id| bytes.put_u64(state_id))
    }

    pub(crate) fn from_bytes(bytes: &mut Bytes) -> Self {
        Self {
            partition_id: bytes.has_remaining().then(|| bytes.get_u64()),
            state_id: bytes.has_remaining().then(|| bytes.get_u64()),
        }
    }
}

impl FsmTable for RocksDBTransaction {
    fn get(&mut self, partition_id: PartitionId, state_id: u64) -> GetFuture<Option<Bytes>> {
        let key = u64_pair(partition_id, state_id);
        self.spawn_blocking(move |db| db.get_owned(PartitionStateMachine, key))
    }

    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture {
        let key = u64_pair(partition_id, state_id);
        self.put_kv(PartitionStateMachine, key, state_value);
        ready()
    }

    fn clear(&mut self, partition_id: PartitionId, state_id: u64) -> PutFuture {
        let key = u64_pair(partition_id, state_id);
        self.delete_key(PartitionStateMachine, key);
        ready()
    }

    fn get_all_states(&mut self, partition_id: PartitionId) -> GetStream<(u64, Bytes)> {
        self.spawn_background_scan(move |db, tx| find_all_fsm_vars(db, tx, partition_id))
    }
}

fn find_all_fsm_vars(
    db: RocksDBStorage,
    tx: Sender<crate::Result<(u64, Bytes)>>,
    partition_id: PartitionId,
) {
    let start_key = partition_id.to_be_bytes();
    let mut iterator = db.prefix_iterator(PartitionStateMachine, start_key);
    iterator.seek(start_key);
    while let Some((k, v)) = iterator.item() {
        let (_, state_id) = u64_pair_from_slice(k);
        if tx
            .blocking_send(Ok((state_id, Bytes::copy_from_slice(v))))
            .is_err()
        {
            break;
        }
        iterator.next();
    }
}
