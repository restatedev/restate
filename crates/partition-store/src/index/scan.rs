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
use std::sync::Arc;

use rocksdb::ReadOptions;

use restate_rocksdb::{IterAction, Priority};
use restate_storage_api::StorageError;
use restate_storage_query_api::selection::{LiveSelection, Selection};
use restate_storage_query_api::tables::index::{
    BusyVQueue as BusyVQueueTarget, EntryByService, EntryByStage, EntryByVirtualObject,
    EntryNextAtByService, EntryNextAtByStage, EntryNextAtByVirtualObject,
};
use restate_types::sharding::KeyRange;

use crate::keys::header::{KeyHeader, KeySchema};
use crate::keys::selection::{KeyMatch, KeySelection, OrderedKeySchema, PrepareKeySelection};
use crate::keys::{IndexKeyHeader, IndexKeyPrefix, KeyDecode, KeyDecoder};
use crate::stats::StatValueCodec;
use crate::stats::aggregated::StageCounts;
use crate::table::{Instances, ReadMetadata};
use crate::{PartitionStore, Result, break_on_err};

use super::{
    BusyVQueueKey, BusyVQueueKeyView, EntryByStageKey, EntryByStageKeyView, EntryByStageServiceKey,
    EntryByStageServiceKeyView, EntryByVirtualObjectStageKey, EntryByVirtualObjectStageKeyView,
    EntryNextAtByStageKey, EntryNextAtByStageKeyView, EntryNextAtByStageServiceKey,
    EntryNextAtByStageServiceKeyView, EntryNextAtByVirtualObjectStageKey,
    EntryNextAtByVirtualObjectStageKeyView, SecondaryIndexKey,
};

/// Keys visited between checks for a newer live selection. A check reads state shared
/// with the query engine, so it is amortized rather than repeated for every key.
const LIVE_SELECTION_POLL_INTERVAL: u32 = 64;

macro_rules! entry_scan {
    ($method:ident, $target:ty, $key:ty, $view:ident) => {
        /// Scans persisted index entries within the requested and owned key range.
        /// Validates field boundaries and the canonical ID; other values remain lazy.
        /// Supported key predicates, including the latest live selection, run
        /// before materialization. Callers retain residuals for other columns.
        pub fn $method<F>(
            &self,
            range: KeyRange,
            selection: &Selection<$target>,
            live: Option<Box<dyn LiveSelection<$target>>>,
            mut f: F,
        ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>>
        where
            F: for<'a> FnMut($view<'a>) -> ControlFlow<Result<()>> + Send + 'static,
        {
            self.scan_index(
                range,
                Arc::new(<$key>::prepare_candidates(selection)?),
                live.map(|mut live| {
                    move || {
                        live.poll()
                            .map(|selection| <$key>::prepare_candidates(&selection))
                    }
                }),
                move |decoder, value, range| {
                    if !value.is_empty() {
                        return ControlFlow::Break(Err(StorageError::DataIntegrityError));
                    }
                    let view = break_on_err(decoder.take_all())?;
                    let id = break_on_err(view.canonical_id.decode())?;
                    // Enforce both the owned and requested partition-key ranges:
                    // - After a partition split, a store imported from a wider-range
                    //   snapshot can still contain entries outside its owned range.
                    // - Query range pruning can request only a subset of this partition,
                    //   excluding otherwise valid entries from this scan.
                    // The physical index prefix alone does not enforce either bound.
                    if !range.contains(&id.partition_key()) {
                        return ControlFlow::Continue(());
                    }
                    f(view)
                },
            )
        }
    };
}

impl PartitionStore {
    entry_scan!(
        scan_entry_by_service,
        EntryByService,
        EntryByStageServiceKey,
        EntryByStageServiceKeyView
    );
    entry_scan!(
        scan_entry_by_stage,
        EntryByStage,
        EntryByStageKey,
        EntryByStageKeyView
    );
    entry_scan!(
        scan_entry_next_at_by_stage,
        EntryNextAtByStage,
        EntryNextAtByStageKey,
        EntryNextAtByStageKeyView
    );
    entry_scan!(
        scan_entry_next_at_by_service,
        EntryNextAtByService,
        EntryNextAtByStageServiceKey,
        EntryNextAtByStageServiceKeyView
    );
    entry_scan!(
        scan_entry_by_virtual_object,
        EntryByVirtualObject,
        EntryByVirtualObjectStageKey,
        EntryByVirtualObjectStageKeyView
    );
    entry_scan!(
        scan_entry_next_at_by_virtual_object,
        EntryNextAtByVirtualObject,
        EntryNextAtByVirtualObjectStageKey,
        EntryNextAtByVirtualObjectStageKeyView
    );

    /// Scans covering queue-index records, including zero-count queues.
    pub fn scan_busy_vqueues<F>(
        &self,
        range: KeyRange,
        selection: &Selection<BusyVQueueTarget>,
        live: Option<Box<dyn LiveSelection<BusyVQueueTarget>>>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>>
    where
        F: for<'a> FnMut(BusyVQueueKeyView<'a>, StageCounts) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        self.scan_index(
            range,
            Arc::new(BusyVQueueKey::prepare_selection(selection)?),
            live.map(|mut live| {
                move || {
                    live.poll()
                        .map(|selection| BusyVQueueKey::prepare_selection(&selection))
                }
            }),
            move |decoder, value, range| {
                let view = break_on_err(decoder.take_all())?;
                if !range.contains(&break_on_err(view.vqueue_id.decode())?.partition_key()) {
                    return ControlFlow::Continue(());
                }
                let counts = break_on_err(StageCounts::deserialize_from(value))?;
                f(view, counts)
            },
        )
    }

    /// Shares physical scanning and filtering. The callback chooses lazy or owned
    /// decoding and enforces the supplied intersection of requested and owned ranges.
    /// `live` returns a newly prepared live selection when one is available.
    pub(super) fn scan_index<K, L, F>(
        &self,
        range: KeyRange,
        selection: Arc<KeySelection<K>>,
        mut live: Option<L>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, K, L, F>>
    where
        K: SecondaryIndexKey
            + KeyDecode
            + OrderedKeySchema
            + KeySchema<Header = IndexKeyHeader<K::Index>>
            + 'static,
        K::Index: 'static,
        L: FnMut() -> Option<Result<KeySelection<K>>> + Send + 'static,
        F: for<'a> FnMut(KeyDecoder<'a, K>, &'a [u8], KeyRange) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        // A registered index reads its Ready instance, or fails.
        let active = self.active_set();
        let metadata = ReadMetadata {
            partition: self.partition_db().partition(),
            features: self.storage_features(),
            instances: Instances::Store(&active),
        };
        let cursor = selection.cursor(<K::Header as KeyHeader>::scope(&metadata)?)?;
        let range = range.intersect(&self.partition_key_range());
        let future = range
            .zip(cursor)
            .map(|(range, mut cursor)| {
                let scan = cursor.scan().clone();
                let mut opts = ReadOptions::default();
                opts.set_async_io(true);
                let mut visited = 0u32;
                self.iterator_controlled_physical(
                    "df-scan-entry-index",
                    Priority::Low,
                    opts,
                    scan,
                    move |(key, value)| {
                        // Live selections only refine the static plan, so a snapshot that
                        // cannot be prepared is ignored rather than failing the scan.
                        if visited.is_multiple_of(LIVE_SELECTION_POLL_INTERVAL)
                            && let Some(Ok(selection)) = live.as_mut().and_then(|live| live())
                        {
                            cursor.set_live(selection);
                        }
                        visited = visited.wrapping_add(1);
                        match break_on_err(cursor.evaluate(key))? {
                            KeyMatch::Match => {}
                            KeyMatch::Skip => return ControlFlow::Continue(IterAction::Next),
                            KeyMatch::Seek(target) => {
                                return ControlFlow::Continue(IterAction::Seek(target));
                            }
                            KeyMatch::Done => return ControlFlow::Break(Ok(())),
                        }
                        let (_, payload) = break_on_err(IndexKeyPrefix::decode_prefix(key))?;
                        f(payload.into_decoder::<K>(), value, range)
                            .map_continue(|()| IterAction::Next)
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
    use restate_storage_api::vqueue_table::Stage;
    use restate_storage_query_api::selection::ValuePredicate;
    use restate_storage_query_api::tables::index::EntryByServiceClause;
    use restate_types::identifiers::{BaseEntryId, CanonicalEntryId, InvocationId, InvocationUuid};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::Seq;

    use crate::PartitionStoreManager;
    use crate::index::SecondaryIndexKey;
    use crate::keys::KeyKind;
    use crate::keys::header::InstanceScope;

    use super::*;

    async fn scan(
        store: &PartitionStore,
        range: KeyRange,
        filter: Selection<EntryByService>,
    ) -> Result<Vec<CanonicalEntryId>> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        store
            .scan_entry_by_service(range, &filter, None, move |key| {
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
        crate::index::complete_indexes(&mut store).await;
        let scope = crate::index::ready_scope::<crate::index::EntryByStageService>(&store);
        let mut tx = store.transaction();
        // A narrowed store can inherit foreign locators from a larger snapshot.
        for pk in [90, 110, 150, 250] {
            tx.update_secondary_index_with_id(
                scope.id,
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
        EntryByStageServiceKey::borrowed(Stage::Inbox, "svc", Reverse(at), id(180)).encode_key(
            InstanceScope {
                partition: PartitionId::from(7),
                ..scope
            },
            &mut other_partition,
        );
        tx.raw_put_cf(KeyKind::SecondaryIndex, other_partition, []);
        tx.commit().await.unwrap();
        drop(tx);

        assert_eq!(
            scan(&store, KeyRange::FULL, Selection::All).await.unwrap(),
            [id(110), id(150)]
        );
        assert_eq!(
            scan(&store, KeyRange::new(120, 300), Selection::All)
                .await
                .unwrap(),
            [id(150)]
        );
        assert!(
            scan(&store, KeyRange::new(200, 300), Selection::All)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            scan(&store, KeyRange::FULL, Selection::Empty)
                .await
                .unwrap()
                .is_empty()
        );
        let filter = Selection::All
            .and(EntryByServiceClause::ServiceName(ValuePredicate::Equal(
                Some("svc".into()),
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
            .scan_entry_by_service(KeyRange::FULL, &Selection::All, None, move |_| {
                sender.send(()).unwrap();
                ControlFlow::Break(Ok(()))
            })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(receiver.recv().await, Some(()));
        assert_eq!(receiver.recv().await, None);
        let error = store
            .scan_entry_by_service(KeyRange::FULL, &Selection::All, None, |_| {
                ControlFlow::Break(Err(StorageError::DataIntegrityError))
            })
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(error, StorageError::DataIntegrityError));
    }
}
