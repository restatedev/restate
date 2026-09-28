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
use restate_storage_api::filter::{Filter, LiveFilter};
use restate_storage_api::index::{
    BusyVQueue as BusyVQueueTarget, EntryByService, EntryByStage, EntryByVirtualObject,
    EntryNextAtByService, EntryNextAtByStage, EntryNextAtByVirtualObject,
};
use restate_types::sharding::KeyRange;

use crate::keys::filter::{IndexKeySchema, KeyMatch, PreparedKeyFilter};
use crate::keys::{DecodeIndexKey, IndexKeyPrefix, KeyDecoder};
use crate::stats::StatValueCodec;
use crate::stats::aggregated::StageCounts;
use crate::{PartitionStore, Result, break_on_err};

use super::{
    BusyVQueueKey, BusyVQueueKeyView, EntryByStageKey, EntryByStageKeyView, EntryByStageServiceKey,
    EntryByStageServiceKeyView, EntryByVirtualObjectStageKey, EntryByVirtualObjectStageKeyView,
    EntryNextAtByStageKey, EntryNextAtByStageKeyView, EntryNextAtByStageServiceKey,
    EntryNextAtByStageServiceKeyView, EntryNextAtByVirtualObjectStageKey,
    EntryNextAtByVirtualObjectStageKeyView, SecondaryIndexKey,
};

/// Keys visited between checks for a newer live filter. A check reads state shared
/// with the query engine, so it is amortized rather than repeated for every key.
const LIVE_FILTER_POLL_INTERVAL: u32 = 64;

macro_rules! entry_scan {
    ($method:ident, $target:ty, $key:ty, $view:ident) => {
        /// Scans persisted index entries within the requested and owned key range.
        /// Validates field boundaries and the canonical ID; other values remain lazy.
        /// Native predicates, including the latest live filter, run before
        /// materialization. Metrics include empty scan plans.
        pub fn $method<F>(
            &self,
            range: KeyRange,
            filter: &Filter<$target>,
            live: Option<Box<dyn LiveFilter<$target>>>,
            metrics: Option<restate_rocksdb::IteratorMetrics>,
            mut f: F,
        ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>>
        where
            F: for<'a> FnMut($view<'a>) -> ControlFlow<Result<()>> + Send + 'static,
        {
            self.scan_index(
                range,
                <$key>::prepare_filter(filter)?,
                live.map(|mut live| {
                    move || live.poll().map(|filter| <$key>::prepare_filter(&filter))
                }),
                metrics,
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
        filter: &Filter<BusyVQueueTarget>,
        live: Option<Box<dyn LiveFilter<BusyVQueueTarget>>>,
        metrics: Option<restate_rocksdb::IteratorMetrics>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, F>>
    where
        F: for<'a> FnMut(BusyVQueueKeyView<'a>, StageCounts) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        self.scan_index(
            range,
            BusyVQueueKey::prepare_filter(filter)?,
            live.map(|mut live| {
                move || {
                    live.poll()
                        .map(|filter| BusyVQueueKey::prepare_filter(&filter))
                }
            }),
            metrics,
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
    /// `live` returns a newly prepared live filter when one is available.
    fn scan_index<K, L, F>(
        &self,
        range: KeyRange,
        filter: PreparedKeyFilter<K>,
        mut live: Option<L>,
        metrics: Option<restate_rocksdb::IteratorMetrics>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'_, K, L, F>>
    where
        K: SecondaryIndexKey + DecodeIndexKey + IndexKeySchema + 'static,
        L: FnMut() -> Option<Result<PreparedKeyFilter<K>>> + Send + 'static,
        F: for<'a> FnMut(KeyDecoder<'a, K>, &'a [u8], KeyRange) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        let prefix = IndexKeyPrefix::of::<K::Index>(self.partition_id());
        if let Some(metrics) = &metrics {
            metrics.mark_supported();
        }
        let cursor = filter.into_cursor(prefix.as_bytes())?;
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
                    metrics,
                    move |(key, value)| {
                        // Live filters only refine the static plan, so a snapshot that
                        // cannot be prepared is ignored rather than failing the scan.
                        if visited.is_multiple_of(LIVE_FILTER_POLL_INTERVAL)
                            && let Some(Ok(filter)) = live.as_mut().and_then(|live| live())
                        {
                            cursor.set_live(filter);
                        }
                        visited = visited.wrapping_add(1);
                        match break_on_err(cursor.evaluate(key))? {
                            KeyMatch::Match => {}
                            KeyMatch::Seek(target) => {
                                return ControlFlow::Continue(IterAction::Seek(target));
                            }
                            KeyMatch::Done => return ControlFlow::Break(Ok(())),
                        }
                        if break_on_err(cursor.live_rejects(key))? {
                            return ControlFlow::Continue(IterAction::Next);
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
            .scan_entry_by_service(range, &filter, None, None, move |key| {
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
            .scan_entry_by_service(KeyRange::FULL, &Filter::All, None, None, move |_| {
                sender.send(()).unwrap();
                ControlFlow::Break(Ok(()))
            })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(receiver.recv().await, Some(()));
        assert_eq!(receiver.recv().await, None);
        let failed_metrics = restate_rocksdb::IteratorMetrics::default();
        let error = store
            .scan_entry_by_service(
                KeyRange::FULL,
                &Filter::All,
                None,
                Some(failed_metrics.clone()),
                |_| ControlFlow::Break(Err(StorageError::DataIntegrityError)),
            )
            .unwrap()
            .await
            .unwrap_err();
        assert!(matches!(error, StorageError::DataIntegrityError));
        let failed = failed_metrics.snapshot();
        assert_eq!(failed.completed_iterators, 1);
        assert_eq!(failed.keys_visited, 2);

        // Accounting belongs to each operation, even when scans share a store.
        let empty_metrics = restate_rocksdb::IteratorMetrics::default();
        let stopped_metrics = restate_rocksdb::IteratorMetrics::default();
        let empty = store
            .scan_entry_by_service(
                KeyRange::FULL,
                &Filter::Empty,
                None,
                Some(empty_metrics.clone()),
                |_| panic!("empty scan produced a row"),
            )
            .unwrap();
        let stopped = store
            .scan_entry_by_service(
                KeyRange::FULL,
                &Filter::All,
                None,
                Some(stopped_metrics.clone()),
                |_| ControlFlow::Break(Ok(())),
            )
            .unwrap();
        let (empty, stopped) = tokio::join!(empty, stopped);
        empty.unwrap();
        stopped.unwrap();
        let empty = empty_metrics.snapshot();
        assert!(empty.supported);
        assert_eq!(empty.iterators, 0);
        let stopped = stopped_metrics.snapshot();
        assert_eq!(stopped.iterators, 1);
        assert_eq!(stopped.completed_iterators, 1);
        assert_eq!(stopped.keys_visited, 2); // Foreign key 90, then owned key 110.
        assert_eq!(stopped.seeks, 1);
        assert_eq!(stopped.nexts, 1);
    }
}
