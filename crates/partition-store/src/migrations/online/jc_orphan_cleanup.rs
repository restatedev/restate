// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `jc_orphan_cleanup/1`: deletes the journal completion-index keys (`jc`)
//! whose journal is gone. The worked example of the guide in
//! [`crate::migrations::online`].
//!
//! A journal's `jc` keys map each completion ID to the command awaiting it.
//! Releases before v1.7.0 deleted journals without them, as `delete_journal`
//! scanned the wrong prefix, so stores can hold *orphans*: `jc` keys of an
//! invocation that has no journal. A journal exists while its first entry,
//! index 0, does.
//!
//! The guide's safety argument, for this migration:
//!
//! - **Foreground writes need no migration.** A command's `jc` keys are written
//!   with the command, after its journal's first entry, and `delete_journal`
//!   deletes them in the transaction that deletes the journal. So commands
//!   leave no orphan behind the cursor or past the walk's end.
//! - **Decide from what the transaction reads now.** Whether the journal
//!   exists is read through `tx`. A slice remembers it for the invocation's
//!   next keys, which follow in key order: no command runs within a slice, and
//!   the slice deletes only `jc` keys. The next slice looks up again.
//! - **Migrating twice changes nothing.** A deleted orphan is gone, a live key
//!   is kept.
//!
//! `ON_UNKNOWN` is `Keep`: deleting orphans changes nothing a live journal
//! uses, and every binary that can open a store with a registry (v1.8.2 and
//! later) deletes journals with their keys. One that doesn't know this
//! migration discards its walk, which restarts from the beginning.

use restate_storage_api::StorageError;
use restate_types::identifiers::{InvocationUuid, PartitionKey};

use crate::journal_table_v2::{JournalCompletionIdToCommandIndexKey, JournalKey};
use crate::keys::DecodeTableKey;
use crate::registry::MigrationOnUnknown;
use crate::walk::Source;
use crate::{PartitionStoreTransaction, StorageAccess};

use super::{Migrated, OnlineMigration};

/// Deletes the `jc` keys of invocations without a journal.
#[derive(Default)]
pub(crate) struct JcOrphanCleanup {
    /// The invocation this slice last looked up, and whether its journal
    /// exists. Lives for one slice, like the value.
    last: Option<(PartitionKey, InvocationUuid, bool)>,
}

impl OnlineMigration for JcOrphanCleanup {
    const NAME: &'static str = "jc_orphan_cleanup/1";
    const SOURCE: Source = Source::JournalCompletionId;
    const ON_UNKNOWN: MigrationOnUnknown = MigrationOnUnknown::Keep;
    type Row = JournalCompletionIdToCommandIndexKey;

    /// The walk checked that `key` has a `jc` key's length, so decoding can't
    /// panic.
    fn decode(key: &[u8], _value: &[u8]) -> Result<Self::Row, StorageError> {
        JournalCompletionIdToCommandIndexKey::deserialize_from(&mut &key[..])
    }

    fn migrate(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        key: &Self::Row,
    ) -> Result<Migrated, StorageError> {
        let (partition_key, invocation_uuid) = (key.partition_key, key.invocation_uuid);
        let (live, read_bytes) = match self.last {
            Some((pk, uuid, live)) if (pk, uuid) == (partition_key, invocation_uuid) => (live, 0),
            _ => {
                let (live, read_bytes) = tx.lookup_for_migration(JournalKey {
                    partition_key,
                    invocation_uuid,
                    journal_index: 0,
                })?;
                self.last = Some((partition_key, invocation_uuid, live));
                (live, read_bytes)
            }
        };
        if !live {
            tx.delete_key(key)?;
        }
        Ok(Migrated {
            changed: !live,
            read_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
    use restate_storage_api::Transaction;
    use restate_storage_api::journal_table_v2::WriteJournalTable;
    use restate_types::identifiers::{InvocationId, WithPartitionKey};
    use restate_types::journal_v2::{Entry, SleepCommand};
    use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
    use restate_types::time::MillisSinceEpoch;

    use crate::job::MigrationJobStatus::Completed;
    use crate::migrations::online::MigrationDefinition;
    use crate::migrations::online::testing::Harness;
    use crate::owned_iter::OwnedIterator;
    use crate::registry::MigrationState::Complete;
    use crate::walk::engine::Slice;
    use crate::{PartitionStore, StorageRead, TableScan};

    use super::*;

    const JC: &MigrationDefinition = &MigrationDefinition::of::<JcOrphanCleanup>();

    fn invocation(partition_key: PartitionKey) -> InvocationId {
        InvocationId::from_parts(
            partition_key,
            InvocationUuid::from_u128(partition_key.into()),
        )
    }

    /// Stages the journal of `invocation` as commands do: `len` commands, the
    /// `i`th awaiting completion `i`, so `len` `jc` keys.
    fn stage_journal(tx: &mut PartitionStoreTransaction<'_>, invocation: InvocationId, len: u32) {
        for i in 0..len {
            let sleep = Entry::from(SleepCommand {
                wake_up_time: 1.into(),
                completion_id: i,
                name: Default::default(),
            });
            let entry = StoredRawEntry::new(
                StoredRawEntryHeader::new(MillisSinceEpoch::UNIX_EPOCH),
                None,
                sleep.encode::<ServiceProtocolV4Codec>(),
            );
            tx.put_journal_entry(&invocation, i, &entry, &[i]).unwrap();
        }
    }

    /// Writes the journal of `invocation`, `len` commands, as commands do.
    async fn journal(store: &mut PartitionStore, invocation: InvocationId, len: u32) {
        let mut tx = store.transaction();
        stage_journal(&mut tx, invocation, len);
        tx.commit().await.unwrap();
    }

    /// Leaves the `len` `jc` keys of `invocation` without its journal, as the
    /// older journal deletion did.
    async fn orphan(store: &mut PartitionStore, invocation: InvocationId, len: u32) {
        let mut tx = store.transaction();
        stage_journal(&mut tx, invocation, len);
        for journal_index in 0..len {
            tx.delete_key(&JournalKey {
                partition_key: invocation.partition_key(),
                invocation_uuid: invocation.invocation_uuid(),
                journal_index,
            })
            .unwrap();
        }
        tx.commit().await.unwrap();
    }

    /// Deletes the journal of `invocation`, `len` commands, as commands do.
    async fn delete(store: &mut PartitionStore, invocation: InvocationId, len: u32) {
        let mut tx = store.transaction();
        tx.delete_journal(&invocation, len).unwrap();
        tx.commit().await.unwrap();
    }

    /// How many `jc` keys `invocation` has.
    fn keys(store: &mut PartitionStore, invocation: InvocationId) -> usize {
        let prefix = JournalCompletionIdToCommandIndexKey::builder()
            .partition_key(invocation.partition_key())
            .invocation_uuid(invocation.invocation_uuid());
        OwnedIterator::new(store.iterator_from(TableScan::Prefix(prefix)).unwrap()).count()
    }

    #[restate_core::test]
    async fn orphans_go_and_live_keys_stay_in_small_large_or_no_slices() {
        // Two stores with the same journals: one walks in slices of one row,
        // which split `b`'s keys over slices that each look `b` up again, the
        // other in one large slice, which looks each invocation up once.
        let [a, b, c, d] = [10, 20, 30, 40].map(invocation);
        for (rows, slices) in [(1, 9), (100, 1)] {
            let mut store = Harness::fenced(&[JC]).await;
            journal(store.store(), a, 3).await;
            orphan(store.store(), b, 3).await;
            journal(store.store(), c, 1).await;
            orphan(store.store(), d, 2).await;
            store.open().await.unwrap();
            assert_eq!(store.run::<JcOrphanCleanup>(rows).await.unwrap(), slices);
            let left = [a, b, c, d].map(|invocation| keys(store.store(), invocation));
            assert_eq!(left, [3, 0, 1, 0]);
            let job = store.job::<JcOrphanCleanup>().unwrap();
            assert_eq!(
                (job.status, job.rows_examined, job.rows_changed),
                (Completed, 9, 5)
            );
            let entry = store.entry::<JcOrphanCleanup>().unwrap();
            assert_eq!((entry.state(), entry.walk), (Some(Complete), None));
        }

        // A store without `jc` keys completes at registration: no walk, no job.
        let mut store = Harness::fenced(&[JC]).await;
        store.open().await.unwrap();
        let entry = store.entry::<JcOrphanCleanup>().unwrap();
        assert_eq!((entry.state(), entry.walk), (Some(Complete), None));
        assert!(store.job::<JcOrphanCleanup>().is_none());
    }

    #[restate_core::test]
    async fn walk_resumes_after_restart_and_leaves_foreground_writes_alone() {
        let mut store = Harness::fenced(&[JC]).await;
        let [a, b, c, d, e] = [10, 20, 30, 40, 50].map(invocation);
        journal(store.store(), a, 1).await;
        orphan(store.store(), b, 3).await;
        journal(store.store(), c, 2).await;
        orphan(store.store(), d, 1).await;
        store.open().await.unwrap();
        let walk = store.entry::<JcOrphanCleanup>().unwrap().walk;

        // Two slices: `a`'s key stays, `b`'s first goes; the cursor is on it.
        for _ in 0..2 {
            let slice = store.slice::<JcOrphanCleanup>(1).await.unwrap();
            assert_eq!(slice, Slice::Progressed);
        }
        assert_eq!(keys(store.store(), a), 1);
        assert_eq!(keys(store.store(), b), 2);

        // Commands delete `a` behind the cursor and `c` ahead of it, each with
        // its keys, and write `e` beyond the walk's end: no orphan for the walk.
        // `b`'s journal is written again, re-creating its first key behind the
        // cursor; commands never reuse an invocation, but this shows that a
        // slice decides from what it reads: `b`'s other keys are live now.
        delete(store.store(), a, 1).await;
        delete(store.store(), c, 2).await;
        journal(store.store(), e, 2).await;
        journal(store.store(), b, 1).await;

        // A restart resumes the walk at its cursor.
        store.open().await.unwrap();
        assert_eq!(store.entry::<JcOrphanCleanup>().unwrap().walk, walk);
        assert_eq!(store.run::<JcOrphanCleanup>(1).await.unwrap(), 3);
        let left = [a, b, c, d, e].map(|invocation| keys(store.store(), invocation));
        assert_eq!(left, [0, 3, 0, 0, 2]);
        let job = store.job::<JcOrphanCleanup>().unwrap();
        assert_eq!(
            (job.status, job.rows_examined, job.rows_changed),
            (Completed, 5, 2)
        );
    }

    #[restate_core::test]
    async fn failed_slice_commits_nothing_and_its_retry_does_the_work() {
        let mut store = Harness::fenced(&[JC]).await;
        let [a, b] = [10, 20].map(invocation);
        journal(store.store(), a, 1).await;
        orphan(store.store(), b, 2).await;
        store.open().await.unwrap();

        // A failed slice commits nothing; its job records the failure.
        store.fail_next_slice();
        assert!(store.slice::<JcOrphanCleanup>(100).await.is_err());
        assert_eq!(keys(store.store(), b), 2);
        let job = store.job::<JcOrphanCleanup>().unwrap();
        assert_eq!((job.failures, job.rows_changed), (1, 0));
        assert!(job.last_error.is_some());

        // Its retry does the work.
        let slice = store.slice::<JcOrphanCleanup>(100).await.unwrap();
        assert_eq!(slice, Slice::Exhausted);
        let left = [a, b].map(|invocation| keys(store.store(), invocation));
        assert_eq!(left, [1, 0]);
        let job = store.job::<JcOrphanCleanup>().unwrap();
        assert_eq!(
            (
                job.status,
                job.failures,
                job.consecutive_failures,
                job.rows_changed
            ),
            (Completed, 1, 0, 2)
        );
    }
}
