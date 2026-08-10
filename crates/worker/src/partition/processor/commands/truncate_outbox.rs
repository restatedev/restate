// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_bifrost::DataRecord;
use restate_partition_store::PartitionStoreTransaction;
use restate_wal_protocol::v2::commands::TruncateOutboxCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use super::{ApplyPartitionCommand, NextStep};
use crate::partition::ProcessorError;
use crate::partition::processor::{HasOutboxMut, OutboxMut};

pub struct TruncateOutboxContext<'a, 'b, P> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub processor: P,
}

impl<P: HasOutboxMut> ApplyPartitionCommand<TruncateOutboxCommand>
    for TruncateOutboxContext<'_, '_, P>
{
    async fn apply(
        &mut self,
        command: DataRecord<Envelope<TruncateOutboxCommand>>,
    ) -> Result<NextStep, ProcessorError> {
        let lsn = command.seq();
        let (header, truncate) = command.into_inner().split()?;

        self.processor
            .outbox_mut()
            .truncate_outbox_to(self.txn, truncate.index)?;
        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::ops::RangeInclusive;

    use restate_bifrost::DataRecord;
    use restate_core::TaskCenter;
    use restate_partition_store::{PartitionStore, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::fsm_table::WriteFsmTable;
    use restate_storage_api::outbox_table::{OutboxMessage, ReadOutboxTable, WriteOutboxTable};
    use restate_types::SemanticRestateVersion;
    use restate_types::invocation::ServiceInvocation;
    use restate_types::logs::{Keys, Lsn, SequenceNumber};
    use restate_types::message::MessageIndex;
    use restate_types::partitions::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::time::NanosSinceEpoch;
    use restate_wal_protocol::v2::{self, Command};

    use super::{ApplyPartitionCommand, TruncateOutboxCommand, TruncateOutboxContext};
    use crate::partition::processor::{
        HasOutbox, HasOutboxMut, OutboxAccess, OutboxMut, ProcessorRawContext,
    };

    async fn open_store() -> PartitionStore {
        RocksDbManager::init();
        // The test harness shuts the node down after the body runs; hook RocksDB
        // teardown onto that instead of shutting the manager down in every test.
        TaskCenter::set_on_shutdown(Box::pin(async {
            RocksDbManager::get().shutdown().await;
        }));
        PartitionStoreManager::create(true)
            .await
            .unwrap()
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap()
    }

    fn mock_outbox_message() -> OutboxMessage {
        OutboxMessage::ServiceInvocation(Box::new(ServiceInvocation::mock()))
    }

    async fn populate_outbox(storage: &mut PartitionStore, range: RangeInclusive<MessageIndex>) {
        let next_sequence_number = range.end() + 1;
        let message = mock_outbox_message();
        let mut txn = storage.transaction();
        for index in range {
            txn.put_outbox_message(index, &message).unwrap();
        }
        txn.put_outbox_seq_number(next_sequence_number).unwrap();
        txn.commit().await.unwrap();
    }

    /// Drives a `TruncateOutbox` record through the partition-command handler and
    /// commits the transaction, mirroring the `apply_partition_command` dispatch.
    async fn truncate(
        processor: &mut ProcessorRawContext,
        storage: &mut PartitionStore,
        index: MessageIndex,
    ) {
        let envelope = TruncateOutboxCommand::test_envelope(TruncateOutboxCommand { index });
        let record = DataRecord::new(
            NanosSinceEpoch::RESTATE_EPOCH,
            Keys::None,
            Lsn::OLDEST,
            envelope,
        );

        let mut txn = storage.transaction();
        TruncateOutboxContext {
            txn: &mut txn,
            processor,
        }
        .apply(record.map(v2::Envelope::into_typed))
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    #[restate_core::test]
    async fn initializes_outbox_head_from_storage() {
        let mut storage = open_store().await;

        let mut processor =
            ProcessorRawContext::create(SemanticRestateVersion::current(), &mut storage)
                .await
                .unwrap();
        assert_eq!(processor.outbox().outbox_tail(), 0);

        let mut txn = storage.transaction();
        processor
            .outbox_mut()
            .enqueue(&mut txn, &mock_outbox_message())
            .unwrap();
        txn.commit().await.unwrap();
        drop(txn);
        truncate(&mut processor, &mut storage, 0).await;
        assert_matches!(storage.get_outbox_message(0).await, Ok(None));

        populate_outbox(&mut storage, 3..=5).await;
        let mut processor =
            ProcessorRawContext::create(SemanticRestateVersion::current(), &mut storage)
                .await
                .unwrap();
        assert_eq!(processor.outbox().outbox_tail(), 6);

        truncate(&mut processor, &mut storage, 4).await;
        assert_matches!(storage.get_outbox_message(3).await, Ok(None));
        assert_matches!(storage.get_outbox_message(4).await, Ok(None));
        assert_matches!(storage.get_outbox_message(5).await, Ok(Some(_)));
    }

    #[restate_core::test]
    async fn truncates_and_reuses_outbox() {
        let mut storage = open_store().await;
        populate_outbox(&mut storage, 3..=5).await;
        let mut processor =
            ProcessorRawContext::create(SemanticRestateVersion::current(), &mut storage)
                .await
                .unwrap();

        // Truncating already-truncated outbox should be a no-op
        truncate(&mut processor, &mut storage, 2).await;

        truncate(&mut processor, &mut storage, 4).await;

        assert_matches!(storage.get_outbox_message(3).await, Ok(None));
        assert_matches!(storage.get_outbox_message(4).await, Ok(None));
        assert_matches!(storage.get_outbox_message(5).await, Ok(Some(_)));
        assert_eq!(processor.outbox().outbox_tail(), 6);

        truncate(&mut processor, &mut storage, 5).await;

        assert_matches!(storage.get_outbox_message(5).await, Ok(None));
        assert_eq!(processor.outbox().outbox_tail(), 6);

        let mut txn = storage.transaction();
        processor
            .outbox_mut()
            .enqueue(&mut txn, &mock_outbox_message())
            .unwrap();
        txn.commit().await.unwrap();
        drop(txn);

        assert_matches!(storage.get_outbox_message(6).await, Ok(Some(_)));
        assert_eq!(processor.outbox().outbox_tail(), 7);

        truncate(&mut processor, &mut storage, 6).await;

        assert_matches!(storage.get_outbox_message(6).await, Ok(None));
        assert_eq!(processor.outbox().outbox_tail(), 7);

        // Simulating a restart
        // The Outbox is fully truncated at this point
        let mut processor =
            ProcessorRawContext::create(SemanticRestateVersion::current(), &mut storage)
                .await
                .unwrap();
        assert_eq!(processor.outbox().outbox_tail(), 7);

        let mut txn = storage.transaction();
        processor
            .outbox_mut()
            .enqueue(&mut txn, &mock_outbox_message())
            .unwrap();
        txn.commit().await.unwrap();
        drop(txn);

        assert_matches!(storage.get_outbox_message(7).await, Ok(Some(_)));
        truncate(&mut processor, &mut storage, 7).await;
        assert_matches!(storage.get_outbox_message(7).await, Ok(None));
        assert_eq!(processor.outbox().outbox_tail(), 8);
    }
}
