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
    use std::sync::Arc;

    use googletest::prelude::*;

    use restate_bifrost::DataRecord;
    use restate_core::TaskCenter;
    use restate_partition_store::{PartitionStore, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::outbox_table::ReadOutboxTable;
    use restate_types::logs::{Keys, Lsn, SequenceNumber};
    use restate_types::message::MessageIndex;
    use restate_types::partitions::{Partition, PersistedFeatures};
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::time::NanosSinceEpoch;
    use restate_wal_protocol::v2::{self, Command};

    use super::{ApplyPartitionCommand, TruncateOutboxCommand, TruncateOutboxContext};
    use crate::partition::processor::{HasOutbox, OutboxAccess, ProcessorRawContext};

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

    fn empty_processor() -> ProcessorRawContext {
        ProcessorRawContext::new(
            Arc::new(Partition::new(PartitionId::MIN, KeyRange::FULL)),
            PersistedFeatures::default(),
        )
    }

    /// Drives a `TruncateOutbox` record through the partition-command handler and
    /// commits the transaction, mirroring the `apply_partition_command` dispatch.
    async fn truncate(
        processor: &mut ProcessorRawContext,
        storage: &mut PartitionStore,
        index: MessageIndex,
    ) {
        let envelope = TruncateOutboxCommand::test_envelope(TruncateOutboxCommand {
            index,
            partition_key_range: Keys::None,
        });
        let record = DataRecord::new(
            NanosSinceEpoch::UNIX_EPOCH,
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
    async fn truncate_outbox_from_empty() {
        let mut storage = open_store().await;
        let mut processor = empty_processor();

        // An outbox message with index 0 has been processed and must now be truncated.
        truncate(&mut processor, &mut storage, 0).await;

        assert_that!(storage.get_outbox_message(0).await.unwrap(), none());
        // The head catches up to the next available sequence number on truncation. Since we
        // don't know in advance whether we'll be asked to truncate more than one message, we
        // track the head as the next position beyond the last truncation point. Leaving it as
        // None is only safe while the outbox is known to be empty.
        assert_that!(processor.outbox().outbox_head(), some(eq(1)));
    }

    #[restate_core::test]
    async fn truncate_outbox_with_gap() {
        let mut storage = open_store().await;
        let mut processor = empty_processor();
        // The outbox holds [3..=5]; the whole range is truncated after message 5 is processed.
        processor.seed_outbox_in_memory(5, Some(3));

        truncate(&mut processor, &mut storage, 5).await;

        assert_that!(storage.get_outbox_message(3).await.unwrap(), none());
        assert_that!(storage.get_outbox_message(4).await.unwrap(), none());
        assert_that!(storage.get_outbox_message(5).await.unwrap(), none());
        assert_that!(processor.outbox().outbox_head(), some(eq(6)));
    }
}
