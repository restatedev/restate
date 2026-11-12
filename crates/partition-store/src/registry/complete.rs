// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The index backfill job (`index_backfill/1`): completes Backfilling indexes
//! with walks of the entry statuses (see [`crate::walk`]), then makes them Ready.
//!
//! Entries written since an index was registered already list its ID, so a
//! walk adds the contribution of every other entry, in entry-status key order.
//! The indexes of one walk, its members, share its traversal: each entry's
//! status is read and its target resolved once, then applied to every member
//! that does not reflect it yet. Each slice commits its contributions with the walk's
//! cursor, and writes treat every entry up to that cursor as touched, so no entry
//! is contributed twice. Once no entry remains up to the walk's end, the same
//! commit makes its members Ready, unlinks them and removes the walk. Entry
//! headers are never rewritten: once Ready, an index reflects every entry
//! without listing it.

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef, EntryStatusHeader};

use crate::fsm_table::{put_registry, put_walks};
use crate::stats::aggregated::backfill::with_entry_target;
use crate::walk::engine::{self, SliceError, SourceEntry, WalkJob, walk_error};
use crate::walk::{Source, WalkKind};
use crate::{PartitionDb, PartitionStoreTransaction};

use super::{IndexDefinition, IndexRegistry, MaintainedIndexes};

/// Entries per committed slice at open, within the default byte limits.
pub(crate) const CHUNK: usize = 1_000;

/// A Backfilling index: its definition and index ID.
pub(super) type Member = (&'static IndexDefinition, u32);

/// The members of one walk.
pub(crate) struct IndexBackfill {
    members: Vec<Member>,
}

impl IndexBackfill {
    pub(crate) fn new(active: &MaintainedIndexes, walk: u64) -> Result<Self, StorageError> {
        Ok(Self {
            members: active.members(walk)?,
        })
    }
}

impl WalkJob for IndexBackfill {
    fn stage(
        &self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        (_, entry, header): &SourceEntry,
    ) -> Result<(), StorageError> {
        let touched_by = header.touched_by();
        if self.members.iter().all(|(_, id)| touched_by.contains(*id)) {
            return Ok(());
        }
        with_entry_target(db, entry, header.vqueue_id(), header.seq(), |target| {
            let context = EntryContext {
                qid: header.vqueue_id(),
                target,
            };
            let state = EntryStateRef::from_header(header);
            for (definition, id) in &self.members {
                if !touched_by.contains(*id) {
                    (definition.apply)(tx, *id, &context, None, Some(&state));
                }
            }
            Ok(())
        })
        .map_err(|err| StorageError::Generic(err.context(format!("entry {entry}"))))
    }

    fn finish(&self, registry: &mut IndexRegistry) -> Result<(), StorageError> {
        for (_, id) in &self.members {
            registry
                .set_ready(*id)
                .map_err(|err| StorageError::Conversion(err.into()))?;
        }
        Ok(())
    }
}

/// Starts one walk for the unstarted Backfilling `ids`, ending at the last entry
/// now, and links them to it. An empty partition has nothing to walk: the
/// members become Ready in the same commit, and no walk starts. The caller
/// makes a started walk durable before its first slice.
pub(crate) async fn start(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    ids: &[u32],
) -> Result<Option<u64>, SliceError> {
    let active = tx.maintained_indexes();
    let (registry, walks, walk) = engine::stage(tx, |tx| {
        let mut registry = active.registry().clone();
        let mut walks = active.walks().clone();
        let registry_error = |err: super::RegistryError| StorageError::Conversion(err.into());
        let walk = match engine::last_key(db)? {
            None => {
                for id in ids {
                    registry.set_ready(*id).map_err(registry_error)?;
                }
                None
            }
            Some(end) => {
                let walk = walks
                    .start(WalkKind::index_backfill(), Source::EntryStatus, end, None)
                    .map_err(walk_error)?;
                for id in ids {
                    registry.link(*id, walk).map_err(registry_error)?;
                }
                Some(walk)
            }
        };
        put_registry(tx, db.partition().id(), &registry)?;
        put_walks(tx, db.partition().id(), &walks)?;
        Ok((registry, walks, walk))
    })?;
    engine::commit(tx, db, &active, registry, walks).await?;
    Ok(walk)
}

#[cfg(test)]
impl crate::PartitionStore {
    /// Completes one Backfilling index on its own, as an open requesting only
    /// this index does: starts a walk for it unless it has one, then runs it,
    /// updating `registry`. Leaves the installed maintained indexes as they were.
    pub(super) async fn complete_instance(
        &mut self,
        registry: &mut IndexRegistry,
        definition: &'static IndexDefinition,
        id: u32,
        chunk_size: usize,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<(), crate::MigrationError> {
        let db = self.partition_db().clone();
        let mut walks = crate::fsm_table::get_walks_from_partition_db(&db)?.unwrap_or_default();
        super::reconcile::upgrade_format_1(&db, registry, &mut walks)?;
        let previous = db.maintained_indexes();
        db.install_maintained_indexes(std::sync::Arc::new(MaintainedIndexes::new(
            registry,
            &walks,
            &[definition],
        )));
        let mut tx = self.transaction();
        let result: Result<(), crate::MigrationError> = async {
            let walk = match registry.get(id).and_then(|index| index.walk) {
                Some(walk) => Some(walk),
                None => start(&mut tx, &db, &[id])
                    .await
                    .map_err(StorageError::from)?,
            };
            if let Some(walk) = walk {
                engine::run_to_end(&mut tx, &db, walk, chunk_size.into(), cancel).await?;
            }
            Ok(())
        }
        .await;
        drop(tx);
        *registry = db
            .maintained_indexes()
            .expect("installed")
            .registry()
            .clone();
        db.restore_maintained_indexes(previous);
        result
    }
}

/// Up to `limit` entry statuses after the exclusive `resume` key, in key order.
#[cfg(test)]
pub(super) fn read_chunk(
    store: &crate::PartitionStore,
    resume: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<SourceEntry>, StorageError> {
    engine::read_source(store.partition_db(), resume, None, limit, usize::MAX)
        .map(|(chunk, _)| chunk)
}
