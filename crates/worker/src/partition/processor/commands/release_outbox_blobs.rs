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
use restate_wal_protocol::v2::commands::ReleaseOutboxBlobsCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use super::{ApplyPartitionCommand, NextStep};
use crate::partition::ProcessorError;
use crate::partition::processor::{HasOutboxMut, OutboxMut};

pub struct ReleaseOutboxBlobsContext<'a, 'b, P> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub processor: P,
}

impl<P: HasOutboxMut> ApplyPartitionCommand<ReleaseOutboxBlobsCommand>
    for ReleaseOutboxBlobsContext<'_, '_, P>
{
    async fn apply(
        &mut self,
        command: DataRecord<Envelope<ReleaseOutboxBlobsCommand>>,
    ) -> Result<NextStep, ProcessorError> {
        let lsn = command.seq();
        let (header, command) = command.into_inner().split()?;

        self.processor.outbox_mut().release_blobs(
            self.txn,
            command.releases.into_iter().map(|r| (r.index, r.blob)),
        )?;
        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use restate_bifrost::DataRecord;
    use restate_clock::UniqueTimestamp;
    use restate_clock::time::MillisSinceEpoch;
    use restate_core::TaskCenter;
    use restate_partition_store::{PartitionStore, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::blob_table::{BlobBuilder, RawParts, ReadBlobTable, WriteBlobTable};
    use restate_storage_api::fsm_table::ReadFsmTable;
    use restate_types::SemanticRestateVersion;
    use restate_types::blob::BlobId;
    use restate_types::identifiers::InvocationId;
    use restate_types::logs::{Keys, Lsn, SequenceNumber};
    use restate_types::message::MessageIndex;
    use restate_types::partitions::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::time::NanosSinceEpoch;
    use restate_wal_protocol::v2::commands::OutboxBlobRelease;
    use restate_wal_protocol::v2::{self, Command};

    use super::{ApplyPartitionCommand, ReleaseOutboxBlobsCommand, ReleaseOutboxBlobsContext};
    use crate::partition::processor::ProcessorRawContext;

    async fn open_store() -> PartitionStore {
        RocksDbManager::init();
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

    async fn create_processor(storage: &mut PartitionStore) -> ProcessorRawContext {
        ProcessorRawContext::create(SemanticRestateVersion::current(), storage, 16)
            .await
            .unwrap()
    }

    async fn release(
        processor: &mut ProcessorRawContext,
        storage: &mut PartitionStore,
        blob: BlobId,
        indexes: impl IntoIterator<Item = MessageIndex>,
    ) {
        let releases = indexes
            .into_iter()
            .map(|index| OutboxBlobRelease { index, blob })
            .collect();
        let envelope =
            ReleaseOutboxBlobsCommand::test_envelope(ReleaseOutboxBlobsCommand { releases });
        let record = DataRecord::new(
            NanosSinceEpoch::RESTATE_EPOCH,
            Keys::None,
            Lsn::OLDEST,
            envelope,
        );

        let mut txn = storage.transaction();
        ReleaseOutboxBlobsContext {
            txn: &mut txn,
            processor,
        }
        .apply(record.map(v2::Envelope::into_typed))
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    async fn ref_count(storage: &mut PartitionStore, blob: BlobId) -> Option<u16> {
        storage
            .get_blob::<RawParts>(blob)
            .await
            .unwrap()
            .map(|(_, ref_count, _)| ref_count)
    }

    #[restate_core::test]
    async fn releases_each_outbox_index_once() {
        let mut storage = open_store().await;
        let now = UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::now());
        let blob = BlobId::generate_output_blob_id(now, 1, &InvocationId::mock_random());

        let mut txn = storage.transaction();
        txn.put_blob(
            blob,
            BlobBuilder::default()
                .with_ref_count(NonZeroU16::new(3).unwrap())
                .build(&b"payload"[..]),
        )
        .unwrap();
        txn.commit().await.unwrap();
        drop(txn);

        let mut processor = create_processor(&mut storage).await;
        release(&mut processor, &mut storage, blob, [5, 6]).await;
        assert_eq!(ref_count(&mut storage, blob).await, Some(1));
        assert_eq!(
            storage.get_released_outbox_blob_index().await.unwrap(),
            Some(6)
        );

        // A new leader re-proposes index 6 after re-shuffling; only index 7 is released.
        // The watermark must survive a restart.
        let mut processor = create_processor(&mut storage).await;
        release(&mut processor, &mut storage, blob, [6, 7]).await;
        assert_eq!(ref_count(&mut storage, blob).await, None);
        assert_eq!(
            storage.get_released_outbox_blob_index().await.unwrap(),
            Some(7)
        );
    }
}
