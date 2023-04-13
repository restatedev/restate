use crate::{GetFuture, PutFuture};
use common::types::{OutboxMessage, PartitionId};
use std::ops::Range;

pub trait OutboxTable {
    fn add_message(
        &mut self,
        partition_id: PartitionId,
        message_index: u64,
        outbox_message: OutboxMessage,
    ) -> PutFuture;

    fn get_next_outbox_message(
        &mut self,
        partition_id: PartitionId,
        next_sequence_number: u64,
    ) -> GetFuture<Option<(u64, OutboxMessage)>>;

    fn truncate_outbox(
        &mut self,
        partition_id: PartitionId,
        seq_to_truncate: Range<u64>,
    ) -> PutFuture;
}
