// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::{ControlFlow, RangeBounds};

use rocksdb::ReadOptions;
use zerocopy::IntoBytes;

use restate_rocksdb::{IterAction, Priority};
use restate_storage_api::StorageError;
use restate_storage_api::filter::Filter;
use restate_storage_api::index::EntryByService;
use restate_types::sharding::KeyRange;

use crate::keys::IndexKeyPrefix;
use crate::keys::filter::KeyMatch;
use crate::{PartitionStore, Result, break_on_err};

use super::{EntryByStageService, EntryByStageServiceKey, EntryByStageServiceKeyView};

impl PartitionStore {
    /// Scans persisted entry-index records within the requested and owned key range.
    ///
    /// Service, stage, canonical-ID, and millisecond timestamp predicates use the
    /// shared ordered-key cursor. Timestamp bounds include all HLC logical counters.
    /// The filter is prepared synchronously; the returned future does not borrow it.
    /// Callback errors fail the scan and `Break(Ok(()))` stops it successfully.
    /// Field boundaries are validated before invoking the callback; only the canonical
    /// ID is decoded here to enforce the key range. Other fields remain lazy.
    ///
    /// This reads the index itself, without primary lookups or a completeness claim.
    pub fn scan_entry_by_service<F>(
        &self,
        range: KeyRange,
        filter: &Filter<EntryByService>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>>
    where
        F: for<'a> FnMut(EntryByStageServiceKeyView<'a>) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        let prefix = IndexKeyPrefix::of::<EntryByStageService>(self.partition_id());
        let cursor =
            EntryByStageServiceKey::prepare_filter(filter)?.into_cursor(prefix.as_bytes())?;
        let range = range.intersect(&self.partition_key_range());
        let future = range
            .zip(cursor)
            .map(|(range, mut cursor)| {
                let scan = cursor.scan().clone();
                let mut opts = ReadOptions::default();
                opts.set_async_io(true);
                self.iterator_controlled_physical(
                    "df-scan-entry-index",
                    Priority::Low,
                    opts,
                    scan,
                    move |(key, value)| {
                        match break_on_err(cursor.evaluate(key))? {
                            KeyMatch::Match => {}
                            KeyMatch::Seek(target) => {
                                return ControlFlow::Continue(IterAction::Seek(target));
                            }
                            KeyMatch::Done => return ControlFlow::Break(Ok(())),
                        }
                        if !value.is_empty() {
                            return ControlFlow::Break(Err(StorageError::DataIntegrityError));
                        }
                        let (_, payload) = break_on_err(IndexKeyPrefix::decode_prefix(key))?;
                        let view = break_on_err(
                            payload.into_decoder::<EntryByStageServiceKey>().take_all(),
                        )?;
                        let id = break_on_err(view.canonical_id.decode())?;
                        // Enforce both the owned and requested partition-key ranges:
                        // - After a partition split, a store imported from a wider-range
                        //   snapshot can still contain entries outside its owned range.
                        // - Query range pruning can request only a subset of this partition,
                        //   excluding otherwise valid entries from this scan.
                        // The physical index prefix alone does not enforce either bound.
                        if !range.contains(&id.partition_key()) {
                            return ControlFlow::Continue(IterAction::Next);
                        }
                        f(view).map_continue(|()| IterAction::Next)
                    },
                )
            })
            .transpose()
            .map_err(|_| StorageError::OperationalError)?;
        Ok(async move {
            match future {
                Some(future) => future.await,
                None => Ok(()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;

    use restate_clock::UniqueTimestamp;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::filter::ValuePredicate;
    use restate_storage_api::index::EntryByServiceClause;
    use restate_storage_api::vqueue_table::Stage;
    use restate_types::identifiers::{BaseEntryId, CanonicalEntryId, InvocationId, InvocationUuid};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::Seq;

    use crate::PartitionStoreManager;
    use crate::index::SecondaryIndexKey;
    use crate::keys::KeyKind;

    use super::*;

    async fn scan(
        store: &PartitionStore,
        range: KeyRange,
        filter: Filter<EntryByService>,
    ) -> Result<Vec<CanonicalEntryId>> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .scan_entry_by_service(range, &filter, move |key| {
                sender.send(key.canonical_id.decode().unwrap()).unwrap();
                ControlFlow::Continue(())
            })?
            .await?;
        let mut result = Vec::new();
        while let Some(id) = receiver.recv().await {
            result.push(id);
        }
        Ok(result)
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scans_enforce_ownership_predicates_and_callback_control() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = PartitionId::MIN;
        let mut store = manager
            .open(&Partition::new(partition, KeyRange::new(100, 199)), None)
            .await
            .unwrap();
        let id = |pk| {
            BaseEntryId::from(InvocationId::from_parts(
                pk,
                InvocationUuid::from_u128(pk as u128),
            ))
            .canonicalize(Seq::new(1))
        };
        let at = UniqueTimestamp::try_from_parts(100, 1).unwrap();
        let mut tx = store.transaction();
        // A narrowed store can inherit foreign locators from a larger snapshot.
        for pk in [90, 110, 150, 250] {
            tx.update_secondary_index(
                None,
                Some(&EntryByStageServiceKey::borrowed(
                    Stage::Inbox,
                    "svc",
                    Reverse(at),
                    id(pk),
                )),
            );
        }
        let mut other_partition = Vec::new();
        EntryByStageServiceKey::borrowed(Stage::Inbox, "svc", Reverse(at), id(180))
            .encode_key(PartitionId::from(7), &mut other_partition);
        tx.raw_put_cf(KeyKind::SecondaryIndex, other_partition, []);
        tx.commit().await.unwrap();
        drop(tx);

        assert_eq!(
            scan(&store, KeyRange::FULL, Filter::All).await.unwrap(),
            [id(110), id(150)]
        );
        assert_eq!(
            scan(&store, KeyRange::new(120, 300), Filter::All)
                .await
                .unwrap(),
            [id(150)]
        );
        assert!(
            scan(&store, KeyRange::new(200, 300), Filter::All)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            scan(&store, KeyRange::FULL, Filter::Empty)
                .await
                .unwrap()
                .is_empty()
        );
        let filter = Filter::All
            .and(EntryByServiceClause::ServiceName(ValuePredicate::Equal(
                "svc".into(),
            )))
            .and(EntryByServiceClause::CanonicalId(ValuePredicate::In(vec![
                id(110),
                id(150),
                id(110),
            ])));
        assert_eq!(
            scan(&store, KeyRange::FULL, filter).await.unwrap(),
            [id(110), id(150)]
        );

        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .scan_entry_by_service(KeyRange::FULL, &Filter::All, move |_| {
                sender.send(()).unwrap();
                ControlFlow::Break(Ok(()))
            })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(receiver.recv().await, Some(()));
        assert_eq!(receiver.recv().await, None);
        let error = store
            .scan_entry_by_service(KeyRange::FULL, &Filter::All, |_| {
                ControlFlow::Break(Err(StorageError::DataIntegrityError))
            })
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(error, StorageError::DataIntegrityError));
    }
}
