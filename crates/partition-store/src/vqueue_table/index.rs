// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, Stage};
use restate_types::config::Configuration;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::vqueues::VQueueId;

use crate::index::{
    BusyVQueueKey, EntryByStage, EntryByStageKey, EntryByStageService, EntryByStageServiceKey,
    EntryByVirtualObjectStage, EntryByVirtualObjectStageKey, EntryNextAtByStage,
    EntryNextAtByStageKey, EntryNextAtByStageService, EntryNextAtByStageServiceKey,
    EntryNextAtByVirtualObjectStage, EntryNextAtByVirtualObjectStageKey, RegisteredIndex,
    SecondaryIndex, SecondaryIndexKey,
};
use crate::keys::header::IndexScope;
use crate::registry::{Family, IndexDefinition, IndexKind};
use crate::stats::StatValueCodec;
use crate::stats::aggregated::StageCounts;
use crate::{PartitionStoreTransaction, StorageAccess};

/// Snapshot only the indexed fields before mutating metadata. Scope and identity
/// are unchanged by metadata updates and can be borrowed when constructing keys.
#[derive(PartialEq, Eq)]
pub(super) struct VQueueIndexState {
    total_non_completed: u64,
    last_modified: UniqueTimestamp,
    counts: StageCounts,
}

impl VQueueIndexState {
    pub(super) fn new(meta: &VQueueMeta) -> Self {
        Self {
            total_non_completed: meta.len(),
            last_modified: meta.stats().last_modified_at(),
            counts: meta.stats().into(),
        }
    }
}

pub(super) fn on_vqueue_change(
    storage: &mut PartitionStoreTransaction<'_>,
    qid: &VQueueId,
    meta: &VQueueMeta,
    old: Option<&VQueueIndexState>,
    new: Option<&VQueueIndexState>,
) {
    if old == new {
        return;
    }
    let old_len = old.map_or(0, |state| state.counts.serialized_len());
    let new_len = new.map_or(0, |state| state.counts.serialized_len());
    let values = {
        let buffer = storage.cleared_value_buffer_mut(old_len + new_len);
        if let Some(old) = old {
            old.counts.serialize_to(buffer);
        }
        if let Some(new) = new {
            new.counts.serialize_to(buffer);
        }
        buffer.split()
    };
    let key = |state: &VQueueIndexState| {
        BusyVQueueKey::borrowed(
            Reverse(state.total_non_completed),
            Reverse(state.last_modified),
            meta.scope().as_ref().map(|scope| scope.as_str()),
            qid.clone(),
        )
    };
    let old = old.map(key);
    let new = new.map(key);
    storage.update_covering_secondary_index(
        old.as_ref().map(|key| (key, &values[..old_len])),
        new.as_ref().map(|key| (key, &values[old_len..])),
    );
}

/// The entry indexes are registered definitions, applied to each maintained index
/// on every entry write (see [`crate::registry`]). A before-image is present only
/// when the index already reflects the entry (Ready, token, or committed walk
/// cursor); an uncovered entry only gets its new key.
macro_rules! entry_index {
    ($(#[$meta:meta])* $definition:ident, $apply:ident, $index:ty, $project:ident, $key:expr) => {
        $(#[$meta])*
        pub(crate) static $definition: IndexDefinition = IndexDefinition {
            name: <$index as RegisteredIndex>::DEFINITION,
            kind: IndexKind::Secondary,
            apply: $apply,
        };

        fn $apply(
            storage: &mut PartitionStoreTransaction<'_>,
            id: u32,
            context: &EntryContext<'_>,
            before: Option<&EntryStateRef<'_>>,
            after: Option<&EntryStateRef<'_>>,
        ) {
            update_index(storage, id, context, before, after, $project, $key);
        }
    };
}

entry_index!(
    /// Stage, service, descending transition time.
    ENTRY_BY_STAGE_SERVICE,
    apply_entry_by_stage_service,
    EntryByStageService,
    transitioned,
    |context, (stage, at, id)| Some(EntryByStageServiceKey::borrowed(
        stage,
        context.target.service(),
        at,
        id
    ))
);
entry_index!(
    /// Stage, descending transition time.
    ENTRY_BY_STAGE,
    apply_entry_by_stage,
    EntryByStage,
    transitioned,
    |_, (stage, at, id)| Some(EntryByStageKey::borrowed(stage, at, id))
);
entry_index!(
    /// Stage, next transition time.
    ENTRY_NEXT_AT_BY_STAGE,
    apply_entry_next_at_by_stage,
    EntryNextAtByStage,
    next_at,
    |_, (stage, at, id)| Some(EntryNextAtByStageKey::borrowed(stage, at, id.seq(), id))
);
entry_index!(
    /// Stage, service, next transition time.
    ENTRY_NEXT_AT_BY_STAGE_SERVICE,
    apply_entry_next_at_by_stage_service,
    EntryNextAtByStageService,
    next_at,
    |context, (stage, at, id)| Some(EntryNextAtByStageServiceKey::borrowed(
        stage,
        context.target.service(),
        at,
        id.seq(),
        id
    ))
);
entry_index!(
    /// Virtual object, stage, descending transition time. Only keyed targets.
    ENTRY_BY_VIRTUAL_OBJECT_STAGE,
    apply_entry_by_virtual_object_stage,
    EntryByVirtualObjectStage,
    transitioned,
    |context, (stage, at, id)| {
        let target = context.target;
        target.virtual_object_key().map(|key| {
            EntryByVirtualObjectStageKey::borrowed(
                target.service(),
                target.scope(),
                key,
                stage,
                at,
                id,
            )
        })
    }
);
entry_index!(
    /// Virtual object, stage, next transition time. Only keyed targets.
    ENTRY_NEXT_AT_BY_VIRTUAL_OBJECT_STAGE,
    apply_entry_next_at_by_virtual_object_stage,
    EntryNextAtByVirtualObjectStage,
    next_at,
    |context, (stage, at, id)| {
        let target = context.target;
        target.virtual_object_key().map(|key| {
            EntryNextAtByVirtualObjectStageKey::borrowed(
                target.service(),
                target.scope(),
                key,
                stage,
                at,
                id.seq(),
                id,
            )
        })
    }
);

/// The entry indexes, released and controlled together.
pub(crate) const ENTRY_INDEXES: Family = Family {
    definitions: &[
        &ENTRY_BY_STAGE_SERVICE,
        &ENTRY_BY_STAGE,
        &ENTRY_NEXT_AT_BY_STAGE,
        &ENTRY_NEXT_AT_BY_STAGE_SERVICE,
        &ENTRY_BY_VIRTUAL_OBJECT_STAGE,
        &ENTRY_NEXT_AT_BY_VIRTUAL_OBJECT_STAGE,
    ],
    maintain: indexes_v1,
    complete: complete_indexes_v1,
    reset: no_reset,
};

fn indexes_v1(config: &Configuration) -> bool {
    config.common.experimental.is_indexes_v1_enabled()
}

fn complete_indexes_v1(config: &Configuration) -> bool {
    config.common.experimental.is_complete_indexes_v1_enabled()
}

/// Disabling the entry indexes has no option yet.
fn no_reset(_: &Configuration) -> bool {
    false
}

type Transitioned = (Stage, Reverse<UniqueTimestamp>, CanonicalEntryId);
type NextAt = (Stage, RoughTimestamp, CanonicalEntryId);

fn transitioned(context: &EntryContext<'_>, entry: &EntryStateRef<'_>) -> Transitioned {
    (
        entry.stage,
        Reverse(entry.stats.transitioned_at),
        entry
            .entry_key
            .to_canonical_entry_id(context.qid.partition_key()),
    )
}

fn next_at(context: &EntryContext<'_>, entry: &EntryStateRef<'_>) -> NextAt {
    (
        entry.stage,
        entry.entry_key.run_at(),
        entry
            .entry_key
            .to_canonical_entry_id(context.qid.partition_key()),
    )
}

/// Compares the index's projections before allocating or encoding keys.
/// Unchanged membership must not overwrite a key and break its SingleDelete
/// lifetime. `key` is `None` for entries outside the index.
fn update_index<'a, P: Eq, K>(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'a>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
    project: fn(&EntryContext<'_>, &EntryStateRef<'_>) -> P,
    key: impl Fn(&EntryContext<'a>, P) -> Option<K>,
) where
    K: SecondaryIndexKey,
    K::Index: SecondaryIndex<Scope = IndexScope>,
{
    let old = before.map(|entry| project(context, entry));
    let new = after.map(|entry| project(context, entry));
    if old == new {
        return;
    }
    let old = old.and_then(|old| key(context, old));
    let new = new.and_then(|new| key(context, new));
    storage.update_secondary_index_with_id(id, old.as_ref(), new.as_ref());
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::vqueue_table::{ReadVQueueTable, StoredEntryRef, WriteVQueueTable};
    use restate_storage_api::{StorageError, Transaction};
    use restate_storage_query_api::scan::BlockingScan;
    use restate_types::identifiers::{BaseEntryId, WithPartitionKey};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::vqueues::EntryTargetExt;

    use crate::fsm_table::get_registry_from_partition_db;
    use crate::keys::selection::KeySelection;
    use crate::keys::{IndexKeyPrefix, KeyKind};
    use crate::registry::{FIRST_INDEX_ID, IndexNotReady};
    use crate::stats::aggregated::backfill::tests::seed;
    use crate::{PartitionStore, PartitionStoreManager};

    use super::*;

    /// Every registered index key, by definition and payload: comparable across
    /// replicas whatever index IDs their registries issued.
    fn keys(store: &PartitionStore) -> BTreeMap<String, Vec<Vec<u8>>> {
        let registry = get_registry_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap_or_default();
        let db = store.partition_db();
        let mut options = rocksdb::ReadOptions::default();
        options.set_total_order_seek(true);
        options.set_iterate_range(rocksdb::PrefixRange(KeyKind::SecondaryIndex.as_bytes()));
        let mut it = db
            .rocksdb()
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(db.cf_handle(), options);
        it.seek_to_first();
        let mut keys = BTreeMap::<_, Vec<_>>::new();
        while let Some(key) = it.key() {
            let (prefix, _) = IndexKeyPrefix::decode_prefix(key).unwrap();
            // The busy-vqueue index keeps its static ID.
            if prefix.index_id_raw() >= FIRST_INDEX_ID {
                let definition = registry
                    .get(prefix.index_id_raw())
                    .unwrap()
                    .definition
                    .to_string();
                keys.entry(definition)
                    .or_default()
                    .push(key[IndexKeyPrefix::SERIALIZED_LENGTH..].to_vec());
            }
            it.next();
        }
        it.status().unwrap();
        keys
    }

    /// The entries of the service index, as a query snapshot reads them.
    fn indexed(store: &PartitionStore) -> crate::Result<usize> {
        let (snapshot, _) = store.query_read(Default::default()).unwrap();
        let mut count = 0;
        <crate::ReadSnapshot as BlockingScan<EntryByStageService>>::scan_blocking(
            &snapshot,
            Arc::new(KeySelection::all()),
            |_| {
                count += 1;
                std::ops::ControlFlow::Continue(())
            },
        )?;
        Ok(count)
    }

    fn not_ready(result: crate::Result<usize>) -> bool {
        matches!(result, Err(StorageError::Generic(err)) if err.downcast_ref::<IndexNotReady>().is_some())
    }

    async fn open(store: &mut PartitionStore, configure: impl Fn(&mut Configuration)) {
        let mut config = Configuration::default();
        // This test requests completion before querying.
        config.worker.storage.enable_eager_migrations_on_startup = true;
        configure(&mut config);
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completed_indexes_match_live_maintenance() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut reference = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        // The reference maintains its indexes before any entry exists; the store
        // completes its indexes over existing entries.
        open(&mut store, |_| {}).await;
        open(&mut reference, |config| {
            config.common.experimental.set_indexes_v1(true)
        })
        .await;
        let (last_id, last_status) = seed(&mut store).await;
        seed(&mut reference).await;
        assert!(keys(&store).is_empty());
        assert!(not_ready(indexed(&store)), "absent_indexes_are_not_ready");
        assert!(
            not_ready(indexed(&reference)),
            "maintained_indexes_are_not_ready"
        );

        for store in [&mut store, &mut reference] {
            open(store, |config| {
                config.common.experimental.set_complete_indexes_v1(true)
            })
            .await;
        }
        let expected = keys(&reference);
        assert_eq!(keys(&store), expected, "completion_matches_maintenance");
        // Every entry is in the four stage indexes; the 75 virtual-object
        // entries (workflows excluded) are also in the two object indexes.
        assert_eq!(
            expected
                .iter()
                .map(|(definition, keys)| (definition.as_str(), keys.len()))
                .collect::<Vec<_>>(),
            [
                ("entry_by_stage/1", 147),
                ("entry_by_stage_service/1", 147),
                ("entry_by_virtual_object_stage/1", 75),
                ("entry_next_at_by_stage/1", 147),
                ("entry_next_at_by_stage_service/1", 147),
                ("entry_next_at_by_virtual_object_stage/1", 75),
            ]
        );
        assert_eq!(indexed(&store).unwrap(), 147);

        // A live delete removes the entry from the completed indexes.
        for store in [&mut store, &mut reference] {
            let mut tx = store.transaction();
            let header = tx
                .get_vqueue_entry_status(&BaseEntryId::from(last_id))
                .await
                .unwrap()
                .unwrap();
            let qid = VQueueId::custom(last_id.partition_key(), "backfill");
            tx.delete_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &last_status.invocation_target().unwrap().entry_target_ref(),
                },
                StoredEntryRef::from_header(&header),
            );
            tx.commit().await.unwrap();
        }
        assert_eq!(keys(&store), keys(&reference));
        assert_eq!(indexed(&store).unwrap(), 146);
    }
}
