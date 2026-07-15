// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::Context;
use bytes::BytesMut;
use tokio::time::Instant;
use tracing::{info, trace, warn};

use restate_clock::UniqueTimestamp;
use restate_partition_store::inbox_table::{self, InboxKey};
use restate_partition_store::invocation_status_table::InvocationStatusKey;
use restate_partition_store::keys::{DecodeTableKey, EncodeTableKeyPrefix};
use restate_partition_store::migrations::MigrationContext;
use restate_partition_store::{PartitionStore, PartitionStoreTransaction, invocation_status_table};
use restate_storage_api::inbox_table::{InboxEntry, WriteInboxTable};
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InvocationLite, InvocationStatus,
    InvocationStatusDiscriminants, ReadInvocationStatusTable, ScheduledInvocation,
    WriteInvocationStatusTable,
};
use restate_storage_api::protobuf_types::{PartitionStoreProtobufValue, ProtobufStorageWrapper};
use restate_storage_api::timer_table::{TimerKey, WriteTimerTable};
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::{EntryMetadata, Stage};
use restate_storage_api::{StorageError, Transaction};
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::UnresolvedFuture;
use restate_types::sharding::{PartitionId, WithPartitionKey};
use restate_types::storage::StorageCodec;
use restate_types::vqueues::EntryId;
use restate_types::{LimitKey, LockName, ServiceName};
use restate_util_string::ReString;
use restate_util_time::DurationExt;

use crate::{VQueue, VQueueEvent, VQueuesMetaCache};

/// 1 MiB
const MAX_TRANSACTION_SIZE: usize = 1024 * 1024;

#[inline]
fn increment_unchecked(bytes: &mut BytesMut) {
    for byte in bytes.iter_mut().rev() {
        if let Some(incremented) = byte.checked_add(1) {
            *byte = incremented;
            return;
        } else {
            *byte = 0;
        }
    }
    unreachable!("failed to find a byte to increment");
}

/// VQueues migration for a range of partition keys.
pub async fn migrate_to_vqueues(
    ctx: &mut MigrationContext<'_>,
    cache: &mut VQueuesMetaCache,
    migration_record_created_at: UniqueTimestamp,
    skip_completed: bool,
) -> Result<(), StorageError> {
    // Design Notes:
    // We need to make the migration of a single invocation atomic. An invocation is either migrated
    // or not. To make this atomic, all mutations necessary to migrate a single invocation must be
    // within a single write batch.
    //
    // The design allows us to continue the migration if it was cancelled halfway safely.
    // Note that the vqueue inbox ordering (entry-key) must be consistent and deterministic between
    // leader and followers.
    let partition_id = ctx.partition_db.partition().id();
    let mut stats = MigrationStats::new(partition_id);
    info!(partition_id = %partition_id, "Starting migration to vqueues");
    migrate_inboxes(&mut stats, migration_record_created_at, cache, ctx).await?;
    migrate_invocations(
        &mut stats,
        migration_record_created_at,
        cache,
        ctx,
        skip_completed,
    )
    .await?;

    // clean up old keyed service status table after we have migrated everything
    restate_partition_store::migrations::migrate_to_locks_table::delete_service_status_data(ctx)?;
    stats.report_finish();
    Ok(())
}

/// Migrate inboxes
async fn migrate_inboxes(
    stats: &mut MigrationStats,
    migration_record_created_at: UniqueTimestamp,
    cache: &mut VQueuesMetaCache,
    ctx: &mut MigrationContext<'_>,
) -> Result<(), StorageError> {
    let mut readopts = rocksdb::ReadOptions::default();
    readopts.set_total_order_seek(true);
    readopts.set_verify_checksums(false);
    readopts.fill_cache(false);

    let start_key = inbox_table::InboxKeyBuilder::default()
        .partition_key(ctx.key_range.start())
        .serialize();
    readopts.set_iterate_lower_bound(start_key);

    let mut end_key = inbox_table::InboxKeyBuilder::default()
        .partition_key(ctx.key_range.end())
        .serialize();

    // safe because we have no key kinds set to [0xff, 0xff]
    increment_unchecked(&mut end_key);
    readopts.set_iterate_upper_bound(end_key);

    let mut partition_store = PartitionStore::from(ctx.partition_db.clone());

    let rocks = ctx.partition_db.rocksdb().clone();
    let mut iterator = rocks
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(ctx.partition_db.cf_handle(), readopts);

    iterator.seek_to_first();

    let mut current_service_id = None;
    let mut current_vqueue_id = None;
    let mut txn = partition_store.transaction();
    while iterator.valid() {
        // Allow tokio to cancel this task if the processor is being cancelled.
        tokio::task::consume_budget().await;
        let (mut key, mut value) = iterator.item().unwrap();
        let key = InboxKey::deserialize_from(&mut key)?;
        let seq = key.sequence_number;
        let inbox_entry = InboxEntry::decode(&mut value)?;

        // Switching service_ids means switching vqueues
        if current_service_id
            .as_ref()
            .is_none_or(|current| current != inbox_entry.service_id())
        {
            current_service_id = Some(inbox_entry.service_id().clone());
            // Inboxed invocations cannot have scopes or limit-keys (prior to vqueues)
            current_vqueue_id = Some(crate::util::generate_vqueue_id(
                key.partition_key,
                None,
                &LimitKey::None,
                true,
                &key.service_name,
                Some(&key.service_key),
            ));
        }
        let qid = current_vqueue_id.as_ref().unwrap();
        // entries in inbox are not "scheduled".
        match inbox_entry {
            InboxEntry::Invocation(service_id, invocation_id) => {
                // find the invocation status
                let invocation_status = txn.get_invocation_status(&invocation_id).await?;
                let mut vqueue =
                    VQueue::<VQueueEvent, _>::get_or_insert_with(qid, &mut txn, cache, || {
                        let lock_name = LockName::new(
                            ServiceName::new(&service_id.service_name),
                            ReString::new(&service_id.key),
                        );
                        VQueueMeta::new(
                            migration_record_created_at,
                            None,
                            LimitKey::None,
                            VQueueLink::Lock(lock_name),
                        )
                    })
                    .await?;
                trace!("Migrating invocation={invocation_id} into qid={qid}");

                let mut inboxed = invocation_status
                    .try_as_inboxed()
                    .expect("inbox entry must be inboxed");

                vqueue.enqueue_new(
                    UniqueTimestamp::from_unix_millis_unchecked(
                        inboxed.metadata.timestamps.creation_time(),
                    ),
                    // We use the original inbox sequence number to preserve ordering as best we
                    // can. One can formulate scenarios where this might diverge than the original
                    // inbox ordering in particular if the leader clock went backwards after restart
                    // by > 1s.
                    seq,
                    inboxed.metadata.execution_time,
                    EntryId::from(invocation_id),
                    EntryMetadata::default(),
                );
                // Now, let's update the invocation status to make it vqueue-powered
                inboxed.metadata.vqueue_id = Some(qid.clone());
                txn.put_invocation_status(&invocation_id, &InvocationStatus::Inboxed(inboxed))?;
            }

            InboxEntry::StateMutation(state_mutation) => {
                // State mutations are difficult to migrate and will be prone to reordering or
                // duplication. We assume that inboxed state mutations are generally rare and
                // since they fail silently, we can afford to drop them during migration.
                //
                // We will log a warning for good measure but the user will need to execute a new
                // state mutation call from the CLI. Note that inboxed state mutations are likely to
                // fail because the virtual object's state is likely to have been changed before
                // this state mutation's hash was computed.
                warn!(
                    "[VQueues Migration] Ignoring inboxed state mutation since it cannot be migrated. \
                Please re-execute the state mutation call on virtual object {}",
                    state_mutation.service_id
                );
            }
        }

        // remove the inbox entry so we don't scan it again if we restarted this migration.
        txn.delete_inbox_entry(current_service_id.as_ref().unwrap(), seq)?;
        // determine whether to commit or continue base on transaction size
        if txn.estimated_size_in_bytes() >= MAX_TRANSACTION_SIZE {
            txn.commit().await?;
        }

        stats.inc_inboxed();
        iterator.next();
    }

    // ensures we didn't stop because of an iterator error
    iterator
        .status()
        .context("iterating over inboxes")
        .map_err(StorageError::Generic)?;

    // in case we have an open transaction, commit it
    txn.commit().await?;

    Ok(())
}

/// Migrate non-inboxed invocations
async fn migrate_invocations(
    stats: &mut MigrationStats,
    migration_record_created_at: UniqueTimestamp,
    cache: &mut VQueuesMetaCache,
    ctx: &mut MigrationContext<'_>,
    skip_completed: bool,
) -> Result<(), StorageError> {
    let mut readopts = rocksdb::ReadOptions::default();
    readopts.set_total_order_seek(true);
    readopts.set_verify_checksums(false);
    readopts.fill_cache(false);

    let start_key = invocation_status_table::InvocationStatusKeyBuilder::default()
        .partition_key(ctx.key_range.start())
        .serialize();
    readopts.set_iterate_lower_bound(start_key);

    let mut end_key = invocation_status_table::InvocationStatusKeyBuilder::default()
        .partition_key(ctx.key_range.end())
        .serialize();

    // safe because we have no key kinds set to [0xff, 0xff]
    increment_unchecked(&mut end_key);
    readopts.set_iterate_upper_bound(end_key);

    let mut partition_store = PartitionStore::from(ctx.partition_db.clone());

    let rocks = ctx.partition_db.rocksdb().clone();
    let mut iterator = rocks
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(ctx.partition_db.cf_handle(), readopts);

    iterator.seek_to_first();

    let mut txn = partition_store.transaction();
    while iterator.valid() {
        // Allow tokio to cancel this task if the processor is being cancelled.
        tokio::task::consume_budget().await;
        let (key, value) = iterator.item().unwrap();
        let Some((invocation_id, status)) =
            read_invocation_status(key, value, skip_completed, stats)?
        else {
            iterator.next();
            continue;
        };

        match status {
            InvocationStatus::Scheduled(scheduled_invocation) => {
                migrate_scheduled_invocation(
                    &mut txn,
                    migration_record_created_at,
                    cache,
                    &invocation_id,
                    scheduled_invocation,
                )
                .await?;
                stats.inc_scheduled();
            }
            InvocationStatus::Invoked(in_flight_invocation_metadata) => {
                migrate_invoked_invocation(
                    &mut txn,
                    migration_record_created_at,
                    cache,
                    &invocation_id,
                    in_flight_invocation_metadata,
                )
                .await?;
                stats.inc_invoked();
            }
            InvocationStatus::Suspended {
                metadata,
                awaiting_on,
            } => {
                migrate_suspended_invocation(
                    &mut txn,
                    migration_record_created_at,
                    cache,
                    &invocation_id,
                    metadata,
                    awaiting_on,
                )
                .await?;
                stats.inc_suspended();
            }
            InvocationStatus::Paused(in_flight_invocation_metadata) => {
                migrate_paused_invocation(
                    &mut txn,
                    migration_record_created_at,
                    cache,
                    &invocation_id,
                    in_flight_invocation_metadata,
                )
                .await?;
                stats.inc_paused();
            }
            InvocationStatus::Completed(completed_invocation) => {
                // Completed invocations are only holding on to their result until their
                // `CleanInvocationStatus` timer fires. That timer is preserved by the migration
                // (unlike the scheduled-invocation timer), so when `skip_completed` is set they are
                // filtered out in `read_invocation_status` and left to be cleaned up on schedule.
                // The only trade-off is that skipped completed invocations won't show up in vqueue
                // introspection.
                migrate_completed_invocation(
                    &mut txn,
                    migration_record_created_at,
                    cache,
                    &invocation_id,
                    completed_invocation,
                )
                .await?;
                stats.inc_completed();
            }
            InvocationStatus::Inboxed(_) | InvocationStatus::Free => unreachable!(),
        }

        // determine whether to commit or continue base on transaction size
        if txn.estimated_size_in_bytes() >= MAX_TRANSACTION_SIZE {
            txn.commit().await?;
        }

        iterator.next();
    }

    // ensures we didn't stop because of an iterator error
    iterator
        .status()
        .context("iterating over inboxes")
        .map_err(StorageError::Generic)?;

    // in case we have an open transaction, commit it
    txn.commit().await?;

    Ok(())
}

// State mutations do not have invocation_status, so we still need to scan the inbox table anyway.
async fn migrate_scheduled_invocation(
    txn: &mut PartitionStoreTransaction<'_>,
    migration_record_created_at: UniqueTimestamp,
    cache: &mut VQueuesMetaCache,
    invocation_id: &InvocationId,
    mut scheduled: ScheduledInvocation,
) -> Result<(), StorageError> {
    // A scheduled invocation is somewhat similar to "inboxed", except that it can be on VO or a
    // normal service (or workflow).
    //
    // The scheduled invocation translates to inbox stage and get status scheduled.
    // It has an associated timer entry which we need to remove.
    assert!(scheduled.metadata.vqueue_id.is_none());
    assert!(
        scheduled.metadata.execution_time.is_some(),
        "Execution time must be set for scheduled invocation"
    );

    let qid = crate::util::infer_vqueue_id_from_invocation(
        invocation_id.partition_key(),
        &scheduled.metadata.invocation_target,
        // old invocations cannot have a limit-key.
        &LimitKey::None,
    );

    let mut vqueue = VQueue::<VQueueEvent, _>::get_or_insert_with(&qid, txn, cache, || {
        let link = if let Some(lock_name) = scheduled.metadata.invocation_target.lock_name() {
            VQueueLink::Lock(lock_name)
        } else {
            VQueueLink::Service(ServiceName::new(
                scheduled.metadata.invocation_target.service_name(),
            ))
        };

        VQueueMeta::new(migration_record_created_at, None, LimitKey::None, link)
    })
    .await?;

    trace!("Migrating invocation={invocation_id} into qid={qid}");

    let entry_created_at =
        UniqueTimestamp::from_unix_millis_unchecked(scheduled.metadata.timestamps.creation_time());

    // We use a special value (0) for all running invocations under the following assumptions:
    // - We don't allow two invocations with the same ID to co-exist
    // - Any new invocation with the same ID will be created with Lsn > 0 after migration.
    let seq = 0;
    vqueue.enqueue_new(
        entry_created_at,
        seq,
        scheduled.metadata.execution_time,
        EntryId::from(invocation_id),
        EntryMetadata::default(),
    );

    // Delete the timer entry, we don't need it anymore.
    txn.delete_timer(&TimerKey::neo_invoke(
        scheduled.metadata.execution_time.unwrap().as_u64(),
        invocation_id.invocation_uuid(),
    ))?;

    // Now, let's update the invocation status to make it vqueue-powered
    scheduled.metadata.vqueue_id = Some(qid);
    // Store the updated invoked invocation status
    txn.put_invocation_status(invocation_id, &InvocationStatus::Scheduled(scheduled))?;
    Ok(())
}

async fn migrate_invoked_invocation(
    txn: &mut PartitionStoreTransaction<'_>,
    migration_record_created_at: UniqueTimestamp,
    cache: &mut VQueuesMetaCache,
    invocation_id: &InvocationId,
    mut invoked: InFlightInvocationMetadata,
) -> Result<(), StorageError> {
    // invoked means that they are running (or _should_ be running). We want to place them back
    // onto the running stage and let the scheduler yield them on start.
    assert!(invoked.vqueue_id.is_none());

    let qid = crate::util::infer_vqueue_id_from_invocation(
        invocation_id.partition_key(),
        &invoked.invocation_target,
        // old invocations cannot have a limit-key.
        &LimitKey::None,
    );

    let mut vqueue = VQueue::<VQueueEvent, _>::get_or_insert_with(&qid, txn, cache, || {
        let link = if let Some(lock_name) = invoked.invocation_target.lock_name() {
            VQueueLink::Lock(lock_name)
        } else {
            VQueueLink::Service(ServiceName::new(invoked.invocation_target.service_name()))
        };

        VQueueMeta::new(migration_record_created_at, None, LimitKey::None, link)
    })
    .await?;

    trace!("Migrating invocation={invocation_id} into qid={qid}");
    vqueue.migrate_invoked_invocation(invocation_id, &invoked);

    // Now, let's update the invocation status to make it vqueue-powered
    invoked.vqueue_id = Some(qid);
    // Store the updated invoked invocation status
    txn.put_invocation_status(invocation_id, &InvocationStatus::Invoked(invoked))?;
    Ok(())
}

async fn migrate_completed_invocation(
    txn: &mut PartitionStoreTransaction<'_>,
    migration_record_created_at: UniqueTimestamp,
    cache: &mut VQueuesMetaCache,
    invocation_id: &InvocationId,
    mut completed: CompletedInvocation,
) -> Result<(), StorageError> {
    assert!(completed.vqueue_id.is_none());

    let qid = crate::util::infer_vqueue_id_from_invocation(
        invocation_id.partition_key(),
        &completed.invocation_target,
        // old invocations cannot have a limit-key.
        &LimitKey::None,
    );

    let mut vqueue = VQueue::<VQueueEvent, _>::get_or_insert_with(&qid, txn, cache, || {
        let link = if let Some(lock_name) = completed.invocation_target.lock_name() {
            VQueueLink::Lock(lock_name)
        } else {
            VQueueLink::Service(ServiceName::new(completed.invocation_target.service_name()))
        };

        VQueueMeta::new(migration_record_created_at, None, LimitKey::None, link)
    })
    .await?;

    trace!("Migrating invocation={invocation_id} into qid={qid}");
    vqueue.migrate_completed_invocation(invocation_id, &completed);

    // Now, let's update the invocation status to make it vqueue-powered
    completed.vqueue_id = Some(qid);
    // Store the updated invoked invocation status
    txn.put_invocation_status(invocation_id, &InvocationStatus::Completed(completed))?;
    Ok(())
}

async fn migrate_paused_invocation(
    txn: &mut PartitionStoreTransaction<'_>,
    migration_record_created_at: UniqueTimestamp,
    cache: &mut VQueuesMetaCache,
    invocation_id: &InvocationId,
    mut paused: InFlightInvocationMetadata,
) -> Result<(), StorageError> {
    // invoked means that they are running (or _should_ be running). We want to place them back
    // onto the running stage and let the scheduler yield them on start.
    assert!(paused.vqueue_id.is_none());

    let qid = crate::util::infer_vqueue_id_from_invocation(
        invocation_id.partition_key(),
        &paused.invocation_target,
        // old invocations cannot have a limit-key.
        &LimitKey::None,
    );

    let mut vqueue = VQueue::<VQueueEvent, _>::get_or_insert_with(&qid, txn, cache, || {
        let link = if let Some(lock_name) = paused.invocation_target.lock_name() {
            VQueueLink::Lock(lock_name)
        } else {
            VQueueLink::Service(ServiceName::new(paused.invocation_target.service_name()))
        };

        VQueueMeta::new(migration_record_created_at, None, LimitKey::None, link)
    })
    .await?;

    trace!("Migrating invocation={invocation_id} into qid={qid}");
    vqueue.migrate_parked_invocation(invocation_id, &paused, Stage::Paused);
    // Now, let's update the invocation status to make it vqueue-powered
    paused.vqueue_id = Some(qid);
    // Store the updated invoked invocation status
    txn.put_invocation_status(invocation_id, &InvocationStatus::Paused(paused))?;
    Ok(())
}

async fn migrate_suspended_invocation(
    txn: &mut PartitionStoreTransaction<'_>,
    migration_record_created_at: UniqueTimestamp,
    cache: &mut VQueuesMetaCache,
    invocation_id: &InvocationId,
    mut suspended: InFlightInvocationMetadata,
    awaiting_on: UnresolvedFuture,
) -> Result<(), StorageError> {
    assert!(suspended.vqueue_id.is_none());

    let qid = crate::util::infer_vqueue_id_from_invocation(
        invocation_id.partition_key(),
        &suspended.invocation_target,
        // old invocations cannot have a limit-key.
        &LimitKey::None,
    );

    let mut vqueue = VQueue::<VQueueEvent, _>::get_or_insert_with(&qid, txn, cache, || {
        let link = if let Some(lock_name) = suspended.invocation_target.lock_name() {
            VQueueLink::Lock(lock_name)
        } else {
            VQueueLink::Service(ServiceName::new(suspended.invocation_target.service_name()))
        };

        VQueueMeta::new(migration_record_created_at, None, LimitKey::None, link)
    })
    .await?;

    trace!("Migrating invocation={invocation_id} into qid={qid}");
    vqueue.migrate_parked_invocation(invocation_id, &suspended, Stage::Suspended);
    // Now, let's update the invocation status to make it vqueue-powered
    suspended.vqueue_id = Some(qid);
    // Store the updated invoked invocation status
    txn.put_invocation_status(
        invocation_id,
        &InvocationStatus::Suspended {
            metadata: suspended,
            awaiting_on,
        },
    )?;
    Ok(())
}

#[inline]
fn invocation_id_from_key_bytes<B: bytes::Buf>(
    bytes: &mut B,
) -> Result<InvocationId, StorageError> {
    let key = InvocationStatusKey::deserialize_from(bytes)?;
    Ok(InvocationId::from_parts(
        key.partition_key,
        key.invocation_uuid,
    ))
}

// NOTE: This will only consider non-inboxed invocations that have not been migrated to vqueues.
// When `skip_completed` is set, completed invocations are also filtered out (and counted in
// `stats`) using only the cheap `InvocationLite` discriminant, avoiding a full value decode.
fn read_invocation_status(
    mut k: &[u8],
    v: &[u8],
    skip_completed: bool,
    stats: &mut MigrationStats,
) -> Result<Option<(InvocationId, InvocationStatus)>, StorageError> {
    let invocation_id = invocation_id_from_key_bytes(&mut k)?;

    // a mutable view of the original slice
    let mut vv = v;
    let invocation_status = InvocationLite::decode(&mut vv)?;

    if invocation_status.vqueue_id.is_some()
        // because inboxed invocations are scanned through the inbox table instead
        || matches!(
            invocation_status.status,
            InvocationStatusDiscriminants::Inboxed
        )
    {
        Ok(None)
    } else if skip_completed
        && matches!(
            invocation_status.status,
            InvocationStatusDiscriminants::Completed
        )
    {
        // The `InvocationLite` discriminant is enough to know we can skip this entry, so we avoid
        // the cost of fully decoding the (potentially large) completed invocation value.
        stats.inc_skipped_completed();
        Ok(None)
    } else {
        Ok(Some((invocation_id, decode_value(v)?)))
    }
}

fn decode_value<V>(mut buf: &[u8]) -> Result<V, StorageError>
where
    V: PartitionStoreProtobufValue,
    <<V as PartitionStoreProtobufValue>::ProtobufType as TryInto<V>>::Error: Into<anyhow::Error>,
{
    StorageCodec::decode::<ProtobufStorageWrapper<V::ProtobufType>, _>(&mut buf)
        .map_err(|err| StorageError::Conversion(err.into()))
        .and_then(|v| {
            v.0.try_into()
                .map_err(|e| StorageError::Conversion(e.into()))
        })
}

struct MigrationStats {
    start_time: Instant,
    last_report: Instant,
    partition_id: PartitionId,
    total: usize,
    num_inboxed: usize,
    num_invoked: usize,
    num_scheduled: usize,
    num_suspended: usize,
    num_paused: usize,
    num_completed: usize,
    num_skipped_completed: usize,
}

impl MigrationStats {
    fn new(partition_id: PartitionId) -> Self {
        let start_time = Instant::now();
        let last_report = start_time;
        Self {
            start_time,
            last_report,
            total: 0,
            partition_id,
            num_inboxed: 0,
            num_invoked: 0,
            num_scheduled: 0,
            num_suspended: 0,
            num_paused: 0,
            num_completed: 0,
            num_skipped_completed: 0,
        }
    }

    fn inc_inboxed(&mut self) {
        self.num_inboxed += 1;
        self.total += 1;
        self.maybe_report();
    }

    fn maybe_report(&mut self) {
        if self.total.is_multiple_of(100) && self.last_report.elapsed() >= Duration::from_secs(5) {
            info!(
                partition_id = %self.partition_id,
                "[VQueues Migration Progress] total={} inbox={} invoked={} scheduled={} paused={} suspended={} completed={} skipped_completed={} elapsed={}",
                self.total,
                self.num_inboxed,
                self.num_invoked,
                self.num_scheduled,
                self.num_paused,
                self.num_suspended,
                self.num_completed,
                self.num_skipped_completed,
                self.start_time.elapsed().friendly()
            );
            self.last_report = Instant::now();
        }
    }

    fn report_finish(&self) {
        info!(
            partition_id = %self.partition_id,
            "[VQueues Migration Completed] total={} inbox={} invoked={} scheduled={} paused={} suspended={} completed={} skipped_completed={} elapsed={}",
            self.total,
            self.num_inboxed,
            self.num_invoked,
            self.num_scheduled,
            self.num_paused,
            self.num_suspended,
            self.num_completed,
            self.num_skipped_completed,
            self.start_time.elapsed().friendly()
        );
    }

    fn inc_scheduled(&mut self) {
        self.num_scheduled += 1;
        self.total += 1;
        self.maybe_report();
    }

    fn inc_invoked(&mut self) {
        self.num_invoked += 1;
        self.total += 1;
        self.maybe_report();
    }

    fn inc_suspended(&mut self) {
        self.num_suspended += 1;
        self.total += 1;
        self.maybe_report();
    }

    fn inc_paused(&mut self) {
        self.num_paused += 1;
        self.total += 1;
        self.maybe_report();
    }

    fn inc_completed(&mut self) {
        self.num_completed += 1;
        self.total += 1;
        self.maybe_report();
    }

    fn inc_skipped_completed(&mut self) {
        self.num_skipped_completed += 1;
        self.total += 1;
        self.maybe_report();
    }
}
