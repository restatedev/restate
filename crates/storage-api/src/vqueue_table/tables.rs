// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_sharding::KeyRange;
use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{CanonicalEntryId, Seq, VQueueId};
use restate_util_string::{EncodedMemCmpStr, encoded_mem_cmp_str};

use super::RawStatusHeaderRef;
use super::filters::{ScanEntryIdFilter, ScanMetaFilter};
use super::metadata::{VQueueMeta, VQueueMetaRef};
use super::{EntryContext, EntryId, EntryKey, EntryStateRef, EntryStatusHeader, EntryValue};
use crate::Result;

/// Stages in the inbox/vqueue
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    bilrost::Enumeration,
    enum_map::Enum,
    strum::EnumCount,
    strum::FromRepr,
    strum::Display,
    strum::VariantArray,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(u8)]
#[strum(serialize_all = "kebab-case")]
pub enum Stage {
    #[bilrost(0)]
    Unknown = 0,
    /// Holds entries that are waiting for the scheduler to pick and move to `Run`
    #[bilrost(1)]
    Inbox = b'i',
    /// Holds entries that are currently assumed to be running
    #[bilrost(2)]
    Running = b'r',
    /// Holds entries that are suspended and will be woken up upon receiving a signal.
    #[bilrost(3)]
    Suspended = b's',
    /// Holds entries that are paused, either manually or due to errors.
    /// Paused entries can be resumed by moving them back to `Inbox`
    #[bilrost(4)]
    Paused = b'p',
    /// Items that are completed/finished/terminated. This is a terminal stage and
    /// items in this stage are allowed to be deleted/purged/archived either immediately
    /// or delayed.
    #[bilrost(5)]
    Finished = b'f',
}

impl Stage {
    pub const fn serialized_length_fixed() -> usize {
        std::mem::size_of::<Self>()
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Stage::Unknown => "unknown",
            Stage::Inbox => "inbox",
            Stage::Running => "running",
            Stage::Suspended => "suspended",
            Stage::Paused => "paused",
            Stage::Finished => "finished",
        }
    }

    pub const fn as_mem_cmp_str(self) -> &'static EncodedMemCmpStr {
        match self {
            Stage::Unknown => encoded_mem_cmp_str!("unknown"),
            Stage::Inbox => encoded_mem_cmp_str!("inbox"),
            Stage::Running => encoded_mem_cmp_str!("running"),
            Stage::Suspended => encoded_mem_cmp_str!("suspended"),
            Stage::Paused => encoded_mem_cmp_str!("paused"),
            Stage::Finished => encoded_mem_cmp_str!("finished"),
        }
    }

    pub fn from_mem_cmp_str(value: &EncodedMemCmpStr) -> Option<Self> {
        <Self as strum::VariantArray>::VARIANTS
            .iter()
            .find(|stage| value == stage.as_mem_cmp_str())
            .copied()
    }
}

mod bilrost_encoding {
    use bilrost::encoding::{DistinguishedProxiable, Proxiable};
    use bilrost::{Canonicity, DecodeErrorKind, Enumeration};

    use super::Stage;

    impl Proxiable for Stage {
        type Proxy = u32;

        fn encode_proxy(&self) -> Self::Proxy {
            <Stage as Enumeration>::to_number(self)
        }

        fn decode_proxy(&mut self, proxy: Self::Proxy) -> Result<(), DecodeErrorKind> {
            *self = <Stage as Enumeration>::try_from_number(proxy).unwrap_or(Stage::Unknown);
            Ok(())
        }
    }

    impl DistinguishedProxiable for Stage {
        fn decode_proxy_distinguished(
            &mut self,
            proxy: Self::Proxy,
        ) -> Result<Canonicity, DecodeErrorKind> {
            self.decode_proxy(proxy)?;
            Ok(Canonicity::Canonical)
        }
    }

    bilrost::delegate_proxied_encoding!(
        use encoding (bilrost::encoding::Fixed)
        to encode proxied type (Stage)
        with encoding (bilrost::encoding::Fixed)
        including distinguished
    );
}

/// Whether updating a vqueue retained or purged its metadata.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VQueueDisposition {
    Retained,
    Purged,
}

pub trait WriteVQueueTable {
    /// Initializes a new vqueue
    fn create_vqueue(&mut self, qid: &VQueueId, meta: &VQueueMeta);

    /// Update VQueueMeta with a set of differential updates.
    /// The `meta` **must** match the vqueue metadata on disk prior to the update,
    /// then it gets updated in place. Obsolete metadata is deleted atomically
    /// with the update and reported through the returned disposition.
    fn update_vqueue(
        &mut self,
        qid: &VQueueId,
        meta: &mut VQueueMeta,
        update: &super::metadata::Update,
    ) -> VQueueDisposition;

    /// Deletes a vqueue's metadata record.
    ///
    /// Must only be used on obsolete vqueues (see
    /// [`super::metadata::VQueueMeta::is_obsolete`]); the metadata merge
    /// operator cannot apply updates to a deleted vqueue.
    /// `meta` must describe the stored metadata, including earlier changes in
    /// this transaction, so its secondary-index entry can be removed.
    fn delete_vqueue(&mut self, qid: &VQueueId, meta: &VQueueMeta);

    /// Places an entry onto an inbox stage
    fn put_vqueue_inbox(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
        value: &EntryValue,
    );

    /// Returns and entry from the vqueue
    fn get_vqueue_inbox(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
    ) -> Result<Option<EntryValue>>;

    /// Deletes entry from inbox unconditionally
    fn delete_vqueue_inbox(&mut self, qid: &VQueueId, stage: Stage, key: &EntryKey);

    // Left intentionally for future reference if/when we need to store extra state
    // fn put_vqueue_entry_state<E>(
    //     &mut self,
    //     qid: &VQueueId,
    //     stage: Stage,
    //     entry_key: &EntryKey,
    //     stats: EntryStatistics,
    //     meta: &EntryMetadata,
    //     status: Status,
    //     extra_state: &E,
    // ) where
    //     E: EntryState + bilrost::Message + bilrost::encoding::RawMessage,
    //     (): bilrost::encoding::EmptyState<(), E>;

    /// Creates a previously absent entry status. The caller must establish absence,
    /// including earlier writes in this transaction; this operation does not read storage.
    fn create_vqueue_entry_status(&mut self, context: &EntryContext<'_>, after: EntryStateRef<'_>);

    /// Updates an existing entry, keeping its queue, target, and base identity fixed.
    /// `before` must describe the actual previous state, including earlier writes in
    /// this transaction. This operation does not read or compare against storage.
    ///
    /// # Panics
    /// Panics if the before and after entry IDs differ.
    fn update_vqueue_entry_status(
        &mut self,
        context: &EntryContext<'_>,
        before: EntryStateRef<'_>,
        after: EntryStateRef<'_>,
    );

    /// Deletes an existing status by base identity, without checking its sequence.
    /// `before` must describe the actual previous state, including earlier writes in
    /// this transaction. This operation does not read or compare against storage.
    fn delete_vqueue_entry_status(&mut self, context: &EntryContext<'_>, before: EntryStateRef<'_>);

    /// Stores a vqueue entry input payload
    fn put_vqueue_input_payload<E>(
        &mut self,
        qid: &VQueueId,
        seq: impl Into<Seq>,
        id: &EntryId,
        item: E,
    ) where
        E: bilrost::Message;

    /// Deletes a vqueue item.
    fn delete_vqueue_input_payload(&mut self, qid: &VQueueId, id: &CanonicalEntryId);
}

pub trait ReadVQueueTable {
    /// Get vqueue's metadata
    fn get_vqueue(
        &self,
        qid: &VQueueId,
    ) -> impl Future<Output = Result<Option<super::metadata::VQueueMeta>>>;

    /// Get the current entry state (header information only) by base identity.
    /// This lookup does not check a sequence number.
    fn get_vqueue_entry_status(
        &self,
        id: &BaseEntryId,
    ) -> impl Future<Output = Result<Option<impl EntryStatusHeader + 'static + use<Self>>>>;

    // /// Get the entry state for a vqueue entry by id
    // fn get_vqueue_entry_status_lazy<'a>(
    //     &'a self,
    //     partition_key: PartitionKey,
    //     entry_id: &EntryId,
    // ) -> impl Future<Output = Result<Option<impl LazyEntryStatus + 'a>>>;

    // Left intentionally for future reference
    // fn get_entry_state<I>(
    //     &self,
    //     id: I,
    // ) -> impl Future<Output = Result<Option<(impl EntryStateHeader + 'static, I::State)>>>
    // where
    //     I: IdentifiesEntry,
    //     I::State: EntryState
    //         + bilrost::OwnedMessage
    //         + bilrost::encoding::RawMessageDecoder
    //         + Sized
    //         + Send
    //         + 'static,
    //     (): bilrost::encoding::EmptyState<(), I::State>;

    /// Gets a vqueue input payload identified by its qid, the entry id, its kind and the associated.
    /// sequence number.
    fn get_vqueue_input_payload<E>(
        &self,
        qid: &VQueueId,
        seq: impl Into<Seq>,
        id: &EntryId,
    ) -> impl Future<Output = Result<Option<E>>>
    where
        E: bilrost::OwnedMessage;
}

pub trait ScanVQueueTable {
    fn scan_active_vqueues(
        &self,
        on_item: impl FnMut(VQueueId, super::metadata::VQueueMeta),
    ) -> Result<()>;
}

pub trait ScanVQueueMetaTable {
    /// Used for data-fusion queries
    fn for_each_vqueue_meta<
        F: for<'a> FnMut((&'a VQueueId, &'a VQueueMetaRef<'a>)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        filter: ScanMetaFilter,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait ScanVQueueEntries {
    /// Iterate entries across one or more stages within a partition-key range.
    ///
    /// Stages are scanned sequentially in the order given. An empty `stages`
    /// iterator scans all stages except [`Stage::Unknown`]. The callback
    /// receives the originating stage along with each item.
    ///
    /// Used for data-fusion queries.
    fn for_each_vqueue_entry<F, S>(
        &self,
        range: KeyRange,
        stages: S,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>
    where
        F: for<'a> FnMut(
                (&'a VQueueId, Stage, &'a EntryKey, &'a EntryValue),
            ) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
        S: IntoIterator<Item = Stage>;
}

pub trait ScanVQueueEntryStatusTable {
    /// Iterate vqueue entry status headers within a partition-key range.
    ///
    /// Used for data-fusion queries.
    fn for_each_vqueue_entry_status<F>(
        &self,
        filter: ScanEntryIdFilter,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>
    where
        F: for<'a> FnMut(&'a BaseEntryId, &'a RawStatusHeaderRef<'a>) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static;
}
