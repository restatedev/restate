// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::ops::RangeInclusive;

use bytes::Bytes;
use futures::Stream;

use restate_memory::LocalMemoryLease;
use restate_types::blob::{BlobId, RelatedBlob};
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::journal_v2::raw::{
    RawCommand, RawCommandSpecificMetadata, RawEntry, RawNotification, RawNotificationResultVariant,
};
use restate_types::journal_v2::{
    CommandType, CompletionId, EntryMetadata, EntryType, NotificationId, NotificationType,
};
use restate_types::sharding::KeyRange;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_util_bytecount::ByteCount;

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

/// A hydrated journal entry and the lease covering it.
pub type BudgetedJournalItem = (EntryIndex, StoredRawEntry, LocalMemoryLease);

/// A journal entry as read from storage. Its content may be stored in a blob, so it must be
/// hydrated ([`ReadJournalTable::hydrate_journal_entry`]) into a [`StoredRawEntry`] before
/// decoding it. Everything else is available right away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredUnhydratedRawEntry {
    header: StoredRawEntryHeader,
    metadata: RawEntryMetadata,
    content: StoredContent,
}

/// Everything in a [`RawEntry`] except its content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RawEntryMetadata {
    Command {
        ty: CommandType,
        specific: RawCommandSpecificMetadata,
    },
    Notification {
        ty: NotificationType,
        id: NotificationId,
        result: RawNotificationResultVariant,
    },
}

impl RawEntryMetadata {
    /// Splits `entry` into its metadata and its content.
    pub(crate) fn split(entry: RawEntry) -> (Self, Bytes) {
        match entry {
            RawEntry::Command(cmd) => (
                Self::Command {
                    ty: cmd.command_type(),
                    specific: cmd.command_specific_metadata,
                },
                cmd.serialized_content,
            ),
            RawEntry::Notification(notification) => (
                Self::Notification {
                    ty: notification.ty(),
                    id: notification.id(),
                    result: notification.result_variant(),
                },
                notification.into_serialized_content(),
            ),
        }
    }

    /// The entry with this metadata and `content`.
    pub(crate) fn with_content(self, content: Bytes) -> RawEntry {
        match self {
            Self::Command { ty, specific } => RawCommand::new(ty, content)
                .with_command_specific_metadata(specific)
                .into(),
            Self::Notification { ty, id, result } => {
                RawNotification::new(ty, id, result, content).into()
            }
        }
    }
}

/// Where the content of a [`StoredUnhydratedRawEntry`] is.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum StoredContent {
    Inline(Bytes),
    Blob(RelatedBlob),
}

impl fmt::Debug for StoredContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inline(content) => write!(f, "Inline({})", ByteCount::from(content.len())),
            Self::Blob(blob) => f.debug_tuple("Blob").field(blob).finish(),
        }
    }
}

impl StoredUnhydratedRawEntry {
    pub(crate) fn from_parts(
        header: StoredRawEntryHeader,
        metadata: RawEntryMetadata,
        content: StoredContent,
    ) -> Self {
        Self {
            header,
            metadata,
            content,
        }
    }

    /// Moves the content of `entry` to a blob, written by `store`, which returns the blob.
    pub fn store_content<E>(
        entry: StoredRawEntry,
        store: impl FnOnce(Bytes) -> std::result::Result<RelatedBlob, E>,
    ) -> std::result::Result<Self, E> {
        let (metadata, content) = RawEntryMetadata::split(entry.inner);
        let content_blob = store(content)?;
        Ok(Self::from_parts(
            entry.header,
            metadata,
            StoredContent::Blob(content_blob),
        ))
    }

    pub fn header(&self) -> &StoredRawEntryHeader {
        &self.header
    }

    /// The blob storing the content, if any.
    pub fn content_blob(&self) -> Option<RelatedBlob> {
        match self.content {
            StoredContent::Inline(_) => None,
            StoredContent::Blob(blob) => Some(blob),
        }
    }

    /// Length of the content in bytes, wherever it's stored.
    pub fn content_len(&self) -> usize {
        match &self.content {
            StoredContent::Inline(content) => content.len(),
            StoredContent::Blob(blob) => usize::try_from(blob.size).unwrap_or(usize::MAX),
        }
    }

    pub fn command_type(&self) -> Option<CommandType> {
        match self.metadata {
            RawEntryMetadata::Command { ty, .. } => Some(ty),
            RawEntryMetadata::Notification { .. } => None,
        }
    }

    pub fn command_specific_metadata(&self) -> Option<&RawCommandSpecificMetadata> {
        match &self.metadata {
            RawEntryMetadata::Command { specific, .. } => Some(specific),
            RawEntryMetadata::Notification { .. } => None,
        }
    }

    pub fn notification_id(&self) -> Option<NotificationId> {
        match &self.metadata {
            RawEntryMetadata::Notification { id, .. } => Some(id.clone()),
            RawEntryMetadata::Command { .. } => None,
        }
    }

    pub fn notification_result_variant(&self) -> Option<RawNotificationResultVariant> {
        match self.metadata {
            RawEntryMetadata::Notification { result, .. } => Some(result),
            RawEntryMetadata::Command { .. } => None,
        }
    }

    /// The entry, if its content isn't stored in a blob.
    #[allow(
        clippy::result_large_err,
        reason = "Err hands the entry back, it's not propagated"
    )]
    pub fn try_into_inline(self) -> std::result::Result<StoredRawEntry, Self> {
        match self.content {
            StoredContent::Inline(content) => Ok(StoredRawEntry::new(
                self.header,
                self.metadata.with_content(content),
            )),
            StoredContent::Blob(_) => Err(self),
        }
    }

    /// The entry with the given content, read from [`Self::content_blob`].
    pub fn hydrate_with(self, content: Bytes) -> StoredRawEntry {
        debug_assert!(
            matches!(self.content, StoredContent::Blob(_)),
            "the content is inline"
        );
        StoredRawEntry::new(self.header, self.metadata.with_content(content))
    }

    /// The parts as they are stored.
    pub(crate) fn into_parts(self) -> (StoredRawEntryHeader, RawEntryMetadata, StoredContent) {
        (self.header, self.metadata, self.content)
    }
}

impl From<StoredRawEntry> for StoredUnhydratedRawEntry {
    fn from(entry: StoredRawEntry) -> Self {
        let (metadata, content) = RawEntryMetadata::split(entry.inner);
        Self::from_parts(entry.header, metadata, StoredContent::Inline(content))
    }
}

impl EntryMetadata for StoredUnhydratedRawEntry {
    fn ty(&self) -> EntryType {
        match self.metadata {
            RawEntryMetadata::Command { ty, .. } => EntryType::Command(ty),
            RawEntryMetadata::Notification { ty, .. } => EntryType::Notification(ty),
        }
    }
}

pub trait ReadJournalTable {
    fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
    ) -> impl Future<Output = Result<Option<StoredUnhydratedRawEntry>>> + Send;

    fn get_journal(
        &self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredUnhydratedRawEntry)>> + Send>;

    /// Reads the content of `entry` if it's stored in a blob. Sees the blobs written by the same
    /// transaction.
    fn hydrate_journal_entry(&self, entry: StoredUnhydratedRawEntry) -> Result<StoredRawEntry>;

    fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<HashMap<NotificationId, NotificationEntryIndex>>> + Send;

    fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> impl Future<Output = Result<Option<StoredUnhydratedRawEntry>>> + Send;

    fn has_completion(
        &mut self,
        invocation_id: InvocationId,
        completion_id: CompletionId,
    ) -> impl Future<Output = Result<bool>> + Send;
}

#[derive(Debug, Clone)]
pub enum ScanJournalTableRange {
    PartitionKey(KeyRange),
    InvocationId(RangeInclusive<InvocationId>),
}

pub trait ScanJournalTable {
    fn for_each_journal<
        F: FnMut(
                (
                    restate_types::identifiers::JournalEntryId,
                    StoredUnhydratedRawEntry,
                ),
            ) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: ScanJournalTableRange,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteJournalTable {
    /// Related completion ids to this RawEntry, used to build the internal index.
    ///
    /// Large contents may be stored in a blob, returned if so. The journal holds a reference to
    /// it, released when the journal is deleted.
    ///
    /// An entry must be written only once: the blob id is derived from the invocation, the index
    /// and the append time. Writing the same index again with a large content would overwrite the
    /// blob and reset its reference count, while journals copied with
    /// [`Self::copy_journal_entry`] still share it, and the previous blob would leak if the id
    /// differs.
    fn put_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        index: EntryIndex,
        entry: &StoredRawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<Option<BlobId>>;

    /// Writes an entry read from another journal as it is. A content blob is shared with that
    /// journal.
    fn copy_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        index: EntryIndex,
        entry: StoredUnhydratedRawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<()>;

    /// When length is available, it is suggested to provide it as it makes the delete more efficient.
    fn delete_journal(&mut self, invocation_id: &InvocationId, length: EntryIndex) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct StoredEntry(pub StoredUnhydratedRawEntry);

impl PartitionStoreProtobufValue for StoredEntry {
    type ProtobufType = crate::protobuf_types::v1::Entry;
}

/// Projection of [`StoredEntry`] that decodes only the content blob, skipping the entry
/// content. Decodes from the same bytes as [`StoredEntry`].
#[derive(Debug, Clone, Default)]
pub struct StoredEntryLite {
    pub content_blob: Option<RelatedBlob>,
}

impl PartitionStoreProtobufValue for StoredEntryLite {
    type ProtobufType = crate::protobuf_types::v1::EntryLite;
}

#[derive(Debug, Clone, Copy, derive_more::From, derive_more::Into)]
pub struct JournalEntryIndex(pub EntryIndex);
impl PartitionStoreProtobufValue for JournalEntryIndex {
    type ProtobufType = crate::protobuf_types::v1::JournalEntryIndex;
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct NotificationEntryIndex {
    pub entry_index: EntryIndex,
    pub result_variant: RawNotificationResultVariant,
}

impl NotificationEntryIndex {
    /// Creates a new NotificationEntryIndex with given entry_index
    /// and unknown result variant
    pub fn from_entry_index(
        entry_index: EntryIndex,
        result_variant: RawNotificationResultVariant,
    ) -> Self {
        Self {
            entry_index,
            result_variant,
        }
    }
}

impl PartitionStoreProtobufValue for NotificationEntryIndex {
    type ProtobufType = crate::protobuf_types::v1::NotificationEntryIndex;
}
