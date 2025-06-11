// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::TableKind::Journal;
use crate::keys::TableKey;
use crate::keys::{KeyKind, define_table_key};
use crate::owned_iter::OwnedIterator;
use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::scan::TableScan::FullScanPartitionKeyRange;
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess};
use crate::{TableScan, TableScanIterationDecision};
use anyhow::anyhow;
use futures::Stream;
use futures_util::stream;
use itertools::Itertools;
use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::journal_table_v2::{JournalEntryId, JournalTable, ReadOnlyJournalTable};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, PartitionKey, WithInvocationId, WithPartitionKey,
};
use restate_types::invocation::InvocationEpoch;
use restate_types::journal_v2::raw::{RawCommand, RawEntry, RawEntryInner};
use restate_types::journal_v2::{CompletionId, EntryMetadata, NotificationId};
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::RangeInclusive;

define_table_key!(
    Journal,
    KeyKind::JournalV2,
    JournalKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        journal_index: u32
    )
);
define_table_key!(
    Journal,
    KeyKind::ArchivedJournalV2,
    ArchivedJournalKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        invocation_epoch: InvocationEpoch,
        journal_index: u32
    )
);

define_table_key!(
    Journal,
    KeyKind::JournalV2CompletionIdToCommandIndex,
    JournalCompletionIdToCommandIndexKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        completion_id: CompletionId
    )
);

define_table_key!(
    Journal,
    KeyKind::JournalV2NotificationIdToNotificationIndex,
    JournalNotificationIdToNotificationIndexKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        notification_id: NotificationId
    )
);

fn write_journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> JournalKey {
    JournalKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .journal_index(journal_index)
}

fn write_archived_journal_entry_key(
    invocation_id: &InvocationId,
    invocation_epoch: InvocationEpoch,
    journal_index: u32,
) -> ArchivedJournalKey {
    ArchivedJournalKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .invocation_epoch(invocation_epoch)
        .journal_index(journal_index)
}

#[derive(Debug, Clone)]
pub struct StoredEntry {
    pub entry: RawEntry,
    pub epoch: InvocationEpoch,
}
impl PartitionStoreProtobufValue for StoredEntry {
    type ProtobufType = crate::protobuf_types::v1::Entry;
}

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub(crate) struct JournalEntryIndex(pub(crate) u32);
impl PartitionStoreProtobufValue for JournalEntryIndex {
    type ProtobufType = crate::protobuf_types::v1::JournalEntryIndex;
}

fn put_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    invocation_epoch: InvocationEpoch,
    journal_index: u32,
    journal_entry: &RawEntry,
    related_completion_ids: &[CompletionId],
) -> Result<()> {
    if let RawEntryInner::Notification(notification) = &journal_entry.inner {
        storage.put_kv(
            JournalNotificationIdToNotificationIndexKey::default()
                .partition_key(invocation_id.partition_key())
                .invocation_uuid(invocation_id.invocation_uuid())
                .notification_id(notification.id()),
            &JournalEntryIndex(journal_index),
        )?;
    } else if let RawEntryInner::Command(_) = &journal_entry.inner {
        for completion_id in related_completion_ids {
            storage.put_kv(
                JournalCompletionIdToCommandIndexKey::default()
                    .partition_key(invocation_id.partition_key())
                    .invocation_uuid(invocation_id.invocation_uuid())
                    .completion_id(*completion_id),
                &JournalEntryIndex(journal_index),
            )?;
        }
    }

    storage.put_kv(
        write_journal_entry_key(invocation_id, journal_index),
        &StoredEntry {
            entry: journal_entry.clone(),
            epoch: invocation_epoch,
        },
    )
}

fn get_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
) -> Result<Option<RawEntry>> {
    let _x = RocksDbPerfGuard::new("get-journal-entry");
    let key = write_journal_entry_key(invocation_id, journal_index);
    let opt: Option<StoredEntry> = storage.get_value(key)?;
    Ok(opt.map(|e| e.entry))
}

fn get_journal_entry_for_epoch<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    invocation_epoch: InvocationEpoch,
    journal_index: u32,
) -> Result<Option<RawEntry>> {
    let _x = RocksDbPerfGuard::new("get-journal-entry-for-epoch");
    // Try archived first
    if let Some(s) = storage.get_value::<_, StoredEntry>(write_archived_journal_entry_key(
        invocation_id,
        invocation_epoch,
        journal_index,
    ))? {
        return Ok(Some(s.entry));
    }

    // Nope, try to get the latest and check if the epoch is the same
    if let Some(s) = storage
        .get_value::<_, StoredEntry>(write_journal_entry_key(invocation_id, invocation_epoch))?
    {
        if s.epoch == invocation_epoch {
            return Ok(Some(s.entry));
        }
    }

    // Not found
    Ok(None)
}

fn get_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<Vec<Result<(EntryIndex, RawEntry)>>> {
    let _x = RocksDbPerfGuard::new("get-journal");
    let key = JournalKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let mut n = 0;
    storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(invocation_id.partition_key(), key),
        move |k, mut v| {
            let key = JournalKey::deserialize_from(&mut Cursor::new(k)).map(|journal_key| {
                journal_key
                    .journal_index
                    .expect("The journal index must be part of the journal key.")
            });
            let entry =
                StoredEntry::decode(&mut v).map_err(|error| StorageError::Generic(error.into()));

            let result = key.and_then(|key| entry.map(|entry| (key, entry.entry)));

            n += 1;
            if n < journal_length {
                TableScanIterationDecision::Emit(result)
            } else {
                TableScanIterationDecision::BreakWith(result)
            }
        },
    )
}

fn all_journals<S: StorageAccess>(
    storage: &S,
    range: RangeInclusive<PartitionKey>,
) -> Result<impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send + use<'_, S>> {
    let latest_iterator = OwnedIterator::new(
        storage.iterator_from(FullScanPartitionKeyRange::<JournalKey>(range.clone()))?,
    )
    .map(|(mut key, mut value)| {
        let journal_key = JournalKey::deserialize_from(&mut key)?;
        let journal_entry =
            StoredEntry::decode(&mut value).map_err(|err| StorageError::Conversion(err.into()))?;

        let (partition_key, invocation_uuid, entry_index) = journal_key.into_inner_ok_or()?;

        Ok((
            JournalEntryId::from_parts(
                InvocationId::from_parts(partition_key, invocation_uuid),
                journal_entry.epoch,
                entry_index,
            ),
            journal_entry.entry,
        ))
    });

    let archived_iterator = OwnedIterator::new(
        storage.iterator_from(FullScanPartitionKeyRange::<ArchivedJournalKey>(range))?,
    )
    .map(|(mut key, mut value)| {
        let journal_key = ArchivedJournalKey::deserialize_from(&mut key)?;
        let journal_entry =
            StoredEntry::decode(&mut value).map_err(|err| StorageError::Conversion(err.into()))?;

        let (partition_key, invocation_uuid, epoch, entry_index) =
            journal_key.into_inner_ok_or()?;

        Ok((
            JournalEntryId::from_parts(
                InvocationId::from_parts(partition_key, invocation_uuid),
                epoch,
                entry_index,
            ),
            journal_entry.entry,
        ))
    });

    Ok(stream::iter(archived_iterator.merge_by(
        latest_iterator,
        |is1, is2| {
            match (is1, is2) {
                (Ok((id1, _)), Ok((id2, _)))
                    if id1.invocation_id() == id2.invocation_id()
                        && id1.invocation_epoch() == id2.invocation_epoch() =>
                {
                    id1.journal_index() <= id2.journal_index()
                }
                (Ok((id1, _)), Ok((id2, _))) if id1.invocation_id() == id2.invocation_id() => {
                    id1.invocation_epoch() <= id2.invocation_epoch()
                }
                (Ok((id1, _)), Ok((id2, _))) => id1.invocation_id() <= id2.invocation_id(),
                // Doesn't matter if there's an error in between
                (_, _) => true,
            }
        },
    )))
}

fn delete_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    invocation_epoch: Option<InvocationEpoch>,
    journal_length: EntryIndex,
) -> Result<()> {
    let _x = RocksDbPerfGuard::new("delete-journal");

    let Some(invocation_epoch) = invocation_epoch else {
        delete_latest_journal(storage, invocation_id, journal_length)?;
        return Ok(());
    };

    // Check if latest journal has the same epoch of the provided one.
    let is_latest = storage
        .get_value::<_, StoredEntry>(write_journal_entry_key(invocation_id, invocation_epoch))?
        .is_some_and(|s| s.epoch == invocation_epoch);

    if is_latest {
        delete_latest_journal(storage, invocation_id, journal_length)?;
    } else {
        delete_archived_journal(storage, invocation_id, invocation_epoch, journal_length)?;
    }

    Ok(())
}

fn delete_latest_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<()> {
    let mut key = write_journal_entry_key(invocation_id, 0);
    let k = &mut key;
    for journal_index in 0..journal_length {
        k.journal_index = Some(journal_index);
        storage.delete_key(k)?;
    }

    // Delete the indexes
    let notification_id_to_notification_index =
        JournalNotificationIdToNotificationIndexKey::default()
            .partition_key(invocation_id.partition_key())
            .invocation_uuid(invocation_id.invocation_uuid());
    let notification_id_index = OwnedIterator::new(storage.iterator_from(
        TableScan::SinglePartitionKeyPrefix(
            invocation_id.partition_key(),
            notification_id_to_notification_index.clone(),
        ),
    )?)
    .map(|(mut key, _)| {
        let journal_key = JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut key)?;
        let (_, _, notification_id) = journal_key.into_inner_ok_or()?;
        Ok(notification_id)
    })
    .collect::<Result<Vec<_>>>()?;
    for notification_id in notification_id_index {
        storage.delete_key(
            &notification_id_to_notification_index
                .clone()
                .notification_id(notification_id),
        )?;
    }

    let completion_id_to_command_index = JournalCompletionIdToCommandIndexKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    let notification_id_index =
        OwnedIterator::new(storage.iterator_from(TableScan::SinglePartitionKeyPrefix(
            invocation_id.partition_key(),
            notification_id_to_notification_index.clone(),
        ))?)
        .map(|(mut key, _)| {
            let journal_key = JournalCompletionIdToCommandIndexKey::deserialize_from(&mut key)?;
            let (_, _, completion_id) = journal_key.into_inner_ok_or()?;
            Ok(completion_id)
        })
        .collect::<Result<Vec<_>>>()?;
    for notification_id in notification_id_index {
        storage.delete_key(
            &completion_id_to_command_index
                .clone()
                .completion_id(notification_id),
        )?;
    }

    Ok(())
}

fn delete_archived_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    invocation_epoch: InvocationEpoch,
    journal_length: EntryIndex,
) -> Result<()> {
    let mut key = write_archived_journal_entry_key(invocation_id, invocation_epoch, 0);
    let k = &mut key;
    for journal_index in 0..journal_length {
        k.journal_index = Some(journal_index);
        storage.delete_key(k)?;
    }

    Ok(())
}

fn delete_journal_range<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    from_included: EntryIndex,
    to_excluded: EntryIndex,
    notification_ids_to_cleanup: &[NotificationId],
) -> Result<()> {
    let _x = RocksDbPerfGuard::new("delete-journal-range");

    // Delete entries
    let mut key = write_journal_entry_key(invocation_id, 0);
    let k = &mut key;
    for journal_index in from_included..to_excluded {
        k.journal_index = Some(journal_index);
        storage.delete_key(k)?;
    }

    // Clean indexes
    if !notification_ids_to_cleanup.is_empty() {
        let mut notification_id_to_notification_index =
            JournalNotificationIdToNotificationIndexKey::default()
                .partition_key(invocation_id.partition_key())
                .invocation_uuid(invocation_id.invocation_uuid());
        let mut completion_id_to_command_index = JournalCompletionIdToCommandIndexKey::default()
            .partition_key(invocation_id.partition_key())
            .invocation_uuid(invocation_id.invocation_uuid());
        for notification_id_to_cleanup in notification_ids_to_cleanup {
            notification_id_to_notification_index.notification_id =
                Some(notification_id_to_cleanup.clone());
            storage.delete_key(&notification_id_to_notification_index)?;

            if let NotificationId::CompletionId(completion_id) = notification_id_to_cleanup {
                completion_id_to_command_index.completion_id = Some(*completion_id);
                storage.delete_key(&completion_id_to_command_index)?;
            }
        }
    }

    Ok(())
}

fn archive_journal_to_epoch<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    invocation_epoch: InvocationEpoch,
    journal_length: EntryIndex,
) -> Result<()> {
    let _x = RocksDbPerfGuard::new("archive-journal-to-epoch");

    let mut latest_key = write_journal_entry_key(invocation_id, 0);
    let mut archived_key = write_archived_journal_entry_key(invocation_id, invocation_epoch, 0);
    for journal_index in 0..journal_length {
        latest_key.journal_index = Some(journal_index);
        archived_key.journal_index = Some(journal_index);

        let Some(mut entry) = storage.get_value::<_, StoredEntry>(latest_key.clone())? else {
            return Err(StorageError::Generic(anyhow!(
                "Expected entry to be not empty"
            )));
        };

        entry.epoch = invocation_epoch;
        storage.put_kv(archived_key.clone(), &entry)?;
    }

    Ok(())
}

fn update_current_journal_epoch<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    new_epoch: InvocationEpoch,
    length: EntryIndex,
) -> Result<()> {
    let _x = RocksDbPerfGuard::new("update-current-journal-epoch");

    let mut key = write_journal_entry_key(invocation_id, 0);
    for journal_index in 0..length {
        key.journal_index = Some(journal_index);
        let Some(mut entry) = storage.get_value::<_, StoredEntry>(key.clone())? else {
            return Err(StorageError::Generic(anyhow!(
                "Expected entry to be not empty"
            )));
        };

        entry.epoch = new_epoch;
        storage.put_kv(key.clone(), &entry)?;
    }

    Ok(())
}

fn get_notifications_index<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
) -> Result<HashMap<NotificationId, EntryIndex>> {
    let key = JournalNotificationIdToNotificationIndexKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    let iter = storage.iterator_from(TableScan::SinglePartitionKeyPrefix(
        invocation_id.partition_key(),
        key,
    ))?;
    OwnedIterator::new(iter)
        .map(|(mut key, mut value)| {
            let journal_key =
                JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut key)?;
            let index = JournalEntryIndex::decode(&mut value)
                .map_err(|err| StorageError::Conversion(err.into()))?;

            let (_, _, notification_id) = journal_key.into_inner_ok_or()?;

            Ok((notification_id, index.0))
        })
        .collect()
}

fn get_command_by_completion_id<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
    completion_id: CompletionId,
) -> Result<Option<RawCommand>> {
    let _x = RocksDbPerfGuard::new("get-command-by-completion-id");

    // Access the index
    let completion_id_to_command_index = JournalCompletionIdToCommandIndexKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .completion_id(completion_id);
    let opt: Option<JournalEntryIndex> = storage.get_value(completion_id_to_command_index)?;
    if opt.is_none() {
        return Ok(None);
    }

    // Now access the entry
    let journal_index = opt.unwrap().0;
    let key = write_journal_entry_key(&invocation_id, journal_index);
    let opt: Option<StoredEntry> = storage.get_value(key)?;
    if opt.is_none() {
        return Ok(None);
    }

    let entry = opt.unwrap().entry;
    let entry_ty = entry.ty();
    Ok(Some(entry.inner.try_as_command().ok_or_else(|| {
        StorageError::Conversion(anyhow!(
            "Entry is expected to be a command, but is {entry_ty}"
        ))
    })?))
}

impl ReadOnlyJournalTable for PartitionStore {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) -> Result<Option<RawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        get_journal_entry(self, &invocation_id, entry_index)
    }

    async fn get_journal_entry_for_epoch(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        entry_index: EntryIndex,
    ) -> Result<Option<RawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        get_journal_entry_for_epoch(self, &invocation_id, invocation_epoch, entry_index)
    }

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send> {
        self.assert_partition_key(&invocation_id)?;
        Ok(stream::iter(get_journal(self, &invocation_id, length)?))
    }

    async fn has_journal(&mut self, invocation_id: &InvocationId) -> Result<bool> {
        self.assert_partition_key(invocation_id)?;
        Ok(get_journal_entry(self, invocation_id, 0)?.is_some())
    }

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send> {
        all_journals(self, range)
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, EntryIndex>> {
        get_notifications_index(self, invocation_id)
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<RawCommand>> {
        get_command_by_completion_id(self, invocation_id, notification_id)
    }
}

impl ReadOnlyJournalTable for PartitionStoreTransaction<'_> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) -> Result<Option<RawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, entry_index)
    }

    async fn get_journal_entry_for_epoch(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        entry_index: EntryIndex,
    ) -> Result<Option<RawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        get_journal_entry_for_epoch(self, &invocation_id, invocation_epoch, entry_index)
    }

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send> {
        self.assert_partition_key(&invocation_id)?;
        Ok(stream::iter(get_journal(self, &invocation_id, length)?))
    }

    async fn has_journal(&mut self, invocation_id: &InvocationId) -> Result<bool> {
        self.assert_partition_key(invocation_id)?;
        Ok(get_journal_entry(self, invocation_id, 0)?.is_some())
    }

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send> {
        all_journals(self, range)
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, EntryIndex>> {
        get_notifications_index(self, invocation_id)
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<RawCommand>> {
        get_command_by_completion_id(self, invocation_id, notification_id)
    }
}

impl JournalTable for PartitionStoreTransaction<'_> {
    async fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        index: u32,
        entry: &RawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        put_journal_entry(
            self,
            &invocation_id,
            invocation_epoch,
            index,
            entry,
            related_completion_ids,
        )
    }

    async fn archive_journal_to_epoch(
        &mut self,
        invocation_id: &InvocationId,
        invocation_epoch: InvocationEpoch,
        journal_length: EntryIndex,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        archive_journal_to_epoch(self, invocation_id, invocation_epoch, journal_length)
    }

    async fn update_current_journal_epoch(
        &mut self,
        invocation_id: &InvocationId,
        new_epoch: InvocationEpoch,
        length: EntryIndex,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        update_current_journal_epoch(self, invocation_id, new_epoch, length)
    }

    async fn delete_journal(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: Option<InvocationEpoch>,
        journal_length: EntryIndex,
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        delete_journal(self, &invocation_id, invocation_epoch, journal_length)
    }

    async fn delete_journal_range(
        &mut self,
        invocation_id: InvocationId,
        from_included: EntryIndex,
        to_excluded: EntryIndex,
        notification_ids_to_cleanup: &[NotificationId],
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        delete_journal_range(
            self,
            &invocation_id,
            from_included,
            to_excluded,
            notification_ids_to_cleanup,
        )
    }
}

#[cfg(test)]
mod tests {

    use super::write_journal_entry_key;

    use crate::keys::TableKey;
    use bytes::Bytes;
    use restate_types::identifiers::{InvocationId, InvocationUuid};

    fn journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> Bytes {
        write_journal_entry_key(invocation_id, journal_index)
            .serialize()
            .freeze()
    }

    #[test]
    fn journal_keys_sort_lex() {
        //
        // across invocations
        //
        assert!(
            journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
                0
            ) < journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(2)),
                0
            )
        );
        //
        // within the same service and key
        //
        let mut previous_key = journal_entry_key(
            &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
            0,
        );
        for i in 1..300 {
            let current_key = journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
                i,
            );
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
