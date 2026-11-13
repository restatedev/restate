// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;

use anyhow::anyhow;
use bytes::Bytes;
use bytes::BytesMut;
use enum_map::Enum;
use metrics::Histogram;
use rocksdb::{
    AsColumnFamilyRef, BoundColumnFamily, DBPinnableSlice, DBRawIteratorWithThreadMode, ReadOptions,
};
use static_assertions::const_assert_eq;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tracing::{Level, instrument, trace, warn};

use restate_core::io::{AdmissionClosed, Gate, IoExecutor};
use restate_futures_util::overdue::OverdueLoggingExt;
use restate_rocksdb::reads::ReadIterator;
use restate_rocksdb::{
    IoMode, IterAction, Priority, RocksDb, RocksDbManager, RocksError, StorageTaskKind,
};
use restate_storage_api::fsm_table::{FsmPosition, ReadFsmTable};
use restate_storage_api::protobuf_types::{PartitionStoreProtobufValue, ProtobufStorageWrapper};
use restate_storage_api::{Storage, StorageError, Transaction};
use restate_types::SemanticRestateVersion;
use restate_types::config::{Configuration, StorageOptions};
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId, WithPartitionKey};
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::Partition;
use restate_types::partitions::StorageVersion;
use restate_types::sharding::KeyRange;
use restate_types::storage::StorageCodec;
use restate_types::storage::StorageDecode;
use restate_types::storage::StorageEncode;
use restate_util_string::ReString;

use crate::PartitionStoreSnapshot;
use crate::features::{LoadedStorageFeatures, StorageFeatures};
use crate::fsm_table::delete_applied_lsn;
use crate::fsm_table::get_partition_seal;
use crate::fsm_table::put_fsm_position;
use crate::fsm_table::put_min_restate_version;
use crate::fsm_table::put_storage_features;
use crate::fsm_table::put_storage_version;
use crate::fsm_table::seal_partition;
use crate::fsm_table::{
    PartitionStateMachineKey, get_locally_durable_lsn, get_min_restate_version_from_partition_db,
    get_registry_from_partition_db, get_state_machine_features_from_partition_db,
    get_storage_features_from_partition_db, get_storage_version_from_partition_db,
    is_jc_orphan_cleanup_done, put_jc_orphan_cleanup_done,
};
use crate::keys::{EncodeTableKey, EncodeTableKeyPrefix, FixedLengthTableKey, KeyKind};
use crate::migrations::MigrationError;
use crate::partition_db::PartitionDb;
use crate::registry::MaintainedIndexes;
use crate::scan::PhysicalScan;
use crate::scan::TableScan;
use crate::snapshots::{CaptureGuard, LocalPartitionSnapshot, SnapshotCompatibility, export_dir};
use crate::stats::aggregated::AggregatedStatsMut;
use crate::{configure_prefix_iterator_opts, configure_range_iterator_opts};

pub type DB = rocksdb::DB;

// Key kind and partition key are 10 bytes (KeyKind(2) + PartitionKey/Id(8)).
// Statistics use a table-specific 8-byte suffix.
pub(crate) const DB_PREFIX_LENGTH: usize =
    KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>();

// If this changes, we need to know.
const_assert_eq!(DB_PREFIX_LENGTH, 10);

/// How long a commit can wait for a [`CommitFence`] before it's logged as slow.
const LOG_SLOW_COMMIT_FENCE_AFTER: Duration = Duration::from_secs(1);

/// Length of the capped prefix extractor of partition-store column families: the key kind, the
/// partition key and up to 10 more bytes, which tell apart the invocations and vqueues sharing a
/// partition key. Keys shorter than this are their own prefix, so prefix seeks need prefixes of at
/// least this length.
///
/// 20 bytes also cover the timestamp and 4 random bytes of ULID-based ids, and the sequence number
/// of canonical entry ids. On an 18.7M-invocation partition store, it halved the block reads of
/// cold journal scans on busy partition keys compared to 10 bytes (1.41 vs 2.72), for filters as
/// large as with 13 bytes and 7% smaller than with 26 bytes. Shorter scans lose their filter:
/// whole partition keys (2.39 vs 1.70 block reads) and the state of objects whose service name and
/// key add up to less than 7 bytes (510 vs 13 µs).
pub(crate) const PREFIX_EXTRACTOR_LENGTH: usize = 20;

/// An internal representation of PartitionId that pads the underlying u16 into u64 to align with
/// partition-key length. This should only be used as a replacement to PartitionId when
/// compatibility with old u64-sized PartitionId is needed. Additionally. This must be aligned with
/// the size of PartitionKey to match the prefix length requirements in rocksdb.
#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    derive_more::Add,
    derive_more::Display,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
)]
#[repr(transparent)]
#[serde(transparent)]
pub struct PaddedPartitionId(u64);

impl PaddedPartitionId {
    pub fn new(id: PartitionId) -> Self {
        Self(u16::from(id) as u64)
    }
}

impl From<PartitionId> for PaddedPartitionId {
    fn from(value: PartitionId) -> Self {
        Self(u16::from(value) as u64)
    }
}

impl From<PaddedPartitionId> for PartitionId {
    fn from(value: PaddedPartitionId) -> Self {
        Self::from(u16::try_from(value.0).expect("partition_id must fit in u16"))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_more::Display)]
#[serde(tag = "type")]
pub enum PartitionSeal {
    #[display(
        "last applied LSN is ahead of the log, \
        this indicates data-loss in the log. \
        partition_applied_lsn: {partition_applied_lsn}, log_tail_lsn: {log_tail_lsn}"
    )]
    AheadOfLog {
        partition_applied_lsn: Lsn,
        log_tail_lsn: Lsn,
    },
}

// Ensures that both types have the same length, this makes it possible to
// share prefix extractor in rocksdb.
const_assert_eq!(
    std::mem::size_of::<PartitionKey>(),
    std::mem::size_of::<PaddedPartitionId>(),
);

pub(crate) type Result<T, E = StorageError> = std::result::Result<T, E>;

pub enum TableScanIterationDecision<R> {
    Emit(Result<R>),
    Continue,
    Break,
    BreakWith(Result<R>),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Enum, strum::VariantArray)]
pub enum TableKind {
    // By Partition ID
    PartitionStateMachine,
    Deduplication,
    Outbox,
    Timers,
    // By Partition Key
    State,
    InvocationStatus,
    ServiceStatus,
    Inbox,
    Journal,
    JournalEvent,
    Promise,
    VQueue,
    Locks,
    Blob,
    SecondaryIndex,
    Stats,
}

impl TableKind {
    pub const fn key_kinds(self) -> &'static [KeyKind] {
        match self {
            Self::State => &[KeyKind::State, KeyKind::ScopedState],
            Self::InvocationStatus => &[KeyKind::InvocationStatus],
            Self::ServiceStatus => &[KeyKind::ServiceStatus],
            Self::Inbox => &[KeyKind::Inbox],
            Self::Outbox => &[KeyKind::Outbox],
            Self::Deduplication => &[KeyKind::Deduplication],
            Self::PartitionStateMachine => &[KeyKind::Fsm],
            Self::Timers => &[KeyKind::Timers],
            Self::Journal => &[
                KeyKind::Journal,
                KeyKind::InvocationStatus,
                KeyKind::JournalV2,
                KeyKind::JournalV2CompletionIdToCommandIndex,
                KeyKind::JournalV2NotificationIdToNotificationIndex,
            ],
            Self::JournalEvent => &[KeyKind::JournalEvent],
            Self::Promise => &[KeyKind::Promise, KeyKind::ScopedPromise],
            Self::VQueue => &[
                KeyKind::VQueueMeta,
                KeyKind::VQueueInboxStage,
                KeyKind::VQueueRunningStage,
                KeyKind::VQueueSuspendedStage,
                KeyKind::VQueuePausedStage,
                KeyKind::VQueueFinishedStage,
                KeyKind::VQueueActive,
                KeyKind::VQueueEntryStatus,
                KeyKind::VQueueInput,
            ],
            Self::Locks => &[KeyKind::Lock],
            Self::Blob => &[KeyKind::Blob],
            Self::SecondaryIndex => &[KeyKind::SecondaryIndex],
            Self::Stats => &[KeyKind::Stats],
        }
    }

    pub fn has_key_kind(self, prefix: &[u8]) -> bool {
        self.extract_key_kind(prefix).is_some()
    }

    pub fn extract_key_kind(self, prefix: &[u8]) -> Option<KeyKind> {
        if prefix.len() < KeyKind::SERIALIZED_LENGTH {
            return None;
        }
        let slice = prefix[..KeyKind::SERIALIZED_LENGTH].try_into().unwrap();
        let Some(kind) = KeyKind::from_bytes(slice) else {
            // warning
            return None;
        };
        self.key_kinds().iter().find(|k| **k == kind).copied()
    }
}

pub struct PartitionStore {
    db: PartitionDb,
    storage_features: OnceLock<StorageFeatures>,
    key_buffer: BytesMut,
    value_buffer: BytesMut,
}

impl std::fmt::Debug for PartitionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStore")
            .field("partition_id", &self.db.partition().partition_id)
            .field("cf", &self.db.partition().cf_name())
            .field("key_buffer", &self.key_buffer.len())
            .field("value_buffer", &self.value_buffer.len())
            .finish()
    }
}

impl Clone for PartitionStore {
    fn clone(&self) -> Self {
        PartitionStore {
            db: self.db.clone(),
            storage_features: self.storage_features.clone(),
            key_buffer: BytesMut::default(),
            value_buffer: BytesMut::default(),
        }
    }
}

impl From<PartitionDb> for PartitionStore {
    fn from(db: PartitionDb) -> Self {
        Self::new(db)
    }
}

impl PartitionStore {
    pub(crate) fn new(db: PartitionDb) -> Self {
        Self {
            db,
            storage_features: OnceLock::new(),
            key_buffer: BytesMut::new(),
            value_buffer: BytesMut::new(),
        }
    }

    /// Returns the on-disk storage features, reading it from RocksDB on the first
    /// call and memoizing it. Clones of PartitionStore will copy the memoized value.
    pub(crate) fn storage_features(&self) -> StorageFeatures {
        *self.storage_features.get_or_init(|| {
            // NOTE: Using `default` is definitely the wrong value, but it prevents the server
            // from crashing when data-fusion tries to access the partition-store prior
            // or during the migration phase.
            //
            // This is a shortcut until better isolation and ownership model is implemented.
            let storage_version =
                get_storage_version_from_partition_db(self.partition_db()).unwrap_or_default();
            let persisted =
                get_storage_features_from_partition_db(self.partition_db()).unwrap_or_default();
            let loaded = LoadedStorageFeatures::load(
                persisted,
                storage_version,
                SemanticRestateVersion::current(),
            )
            .inspect_err(|err| {
                warn!(
                    partition_id = %self.partition_id(),
                    "Failed to verify feature/version compatibility of partition-store: {}",
                    err
                )
            })
            .unwrap_or_default();
            *loaded.enabled()
        })
    }

    /// The indexes maintained on entry writes. Opening the partition reconciles
    /// the registry and installs this set before the processor writes. A store
    /// used without that (tools and tests) loads it from disk on first use. An
    /// unreadable registry or walks record is a bug there, and panics: writing
    /// with an empty set instead would skip the Ready indexes and clear the
    /// entries' tokens, corrupting the indexes silently, and an open would have
    /// failed on it.
    pub(crate) fn maintained_indexes(&self) -> Arc<MaintainedIndexes> {
        if let Some(maintained_indexes) = self.db.maintained_indexes() {
            return maintained_indexes;
        }
        let registry = get_registry_from_partition_db(self.partition_db())
            .unwrap_or_else(|err| {
                panic!(
                    "the index registry of partition {} is unreadable: {err}",
                    self.partition_id()
                )
            })
            .unwrap_or_default();
        let walks = crate::fsm_table::get_walks_from_partition_db(self.partition_db())
            .unwrap_or_else(|err| {
                panic!(
                    "the walks of partition {} are unreadable: {err}",
                    self.partition_id()
                )
            })
            .unwrap_or_default();
        let maintained_indexes = Arc::new(MaintainedIndexes::new(
            &registry,
            &walks,
            crate::registry::DEFINITIONS,
        ));
        self.db
            .install_maintained_indexes(Arc::clone(&maintained_indexes));
        maintained_indexes
    }

    /// Installs the maintained indexes for every handle of this partition, as opening does
    /// after reconciling the registry. Tests install indexes of test definitions.
    #[cfg(test)]
    #[allow(dead_code)] // Consumed when the registry is reconciled at open.
    pub(crate) fn set_maintained_indexes(&mut self, maintained_indexes: MaintainedIndexes) {
        self.db
            .install_maintained_indexes(Arc::new(maintained_indexes));
    }

    /// Overwrites the memoized storage features after their finalization batch commits.
    pub(crate) fn set_storage_features(&mut self, features: StorageFeatures) {
        self.storage_features.take();
        let _ = self.storage_features.set(features);
    }

    pub fn partition_db(&self) -> &PartitionDb {
        &self.db
    }

    pub fn into_inner(self) -> PartitionDb {
        self.db
    }

    #[inline]
    pub fn partition_id(&self) -> PartitionId {
        self.db.partition().partition_id
    }

    pub fn partition_key_range(&self) -> KeyRange {
        self.db.partition().key_range
    }

    /// Takes a read snapshot of the partition's tables. See [`PartitionStoreSnapshot`].
    ///
    /// Fails once the partition is closing or closed.
    pub fn snapshot(&mut self) -> std::result::Result<PartitionStoreSnapshot<'_>, AdmissionClosed> {
        let storage_features = self.storage_features();
        let key_range = self.partition_key_range();
        let snapshot = self.db.read_snapshot()?;
        Ok(PartitionStoreSnapshot::new(
            snapshot,
            &mut self.key_buffer,
            storage_features,
            key_range,
        ))
    }

    /// Admits a read: captures a [`crate::ReadSnapshot`] of the store as it is
    /// now, with its storage features and index registry, and returns the future
    /// that resolves once the snapshot's last holder (a clone, a cursor or a
    /// storage job) has released it. The processor calls this in its loop, after
    /// its readiness checks; `cancel` is the token that ends the read.
    pub fn query_read(
        &self,
        cancel: CancellationToken,
    ) -> Result<(crate::ReadSnapshot, WaitForCancellationFutureOwned)> {
        let completed = CancellationToken::new();
        Ok((
            crate::ReadSnapshot::new(
                self.db.clone(),
                self.storage_features(),
                cancel,
                completed.clone(),
            )?,
            completed.cancelled_owned(),
        ))
    }

    #[inline]
    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(self.db.partition().key_range, partition_key)
    }

    pub fn contains_partition_key(&self, key: PartitionKey) -> bool {
        self.db.partition().key_range.contains(&key)
    }

    // NOTE: This will be removed in lieu of cf_for_key_kind in later changes.
    pub(crate) fn table_handle(&self, table_kind: TableKind) -> &Arc<BoundColumnFamily<'_>> {
        self.db.table_cf_handle(table_kind)
    }

    pub(crate) fn cf_for_key_kind(&self, key_kind: KeyKind) -> &Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle_for_key_kind(key_kind)
    }

    /// Writes local state that cannot be reconstructed from Bifrost through RocksDB's WAL.
    pub(crate) fn put_kv_raw_with_wal<K: EncodeTableKey, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<()> {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        let table = self.cf_for_key_kind(K::KEY_KIND);
        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(false);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .put_cf_opt(table, key_buffer, value, &opts)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[allow(clippy::type_complexity)]
    fn iterator_step_filter_map<O: Send + 'static>(
        tx: mpsc::Sender<Result<O>>,
        mut f: impl FnMut((&[u8], &[u8])) -> Result<Option<O>> + Send + 'static,
    ) -> impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static {
        move |item| {
            let res = match item {
                // apply the caller's function
                Ok((key, value)) => match f((key, value)) {
                    Ok(Some(v)) => v,
                    Ok(None) => {
                        if tx.is_closed() {
                            return IterAction::Stop;
                        } else {
                            return IterAction::Next;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(e));
                        return IterAction::Stop;
                    }
                },
                Err(e) => {
                    let _ = tx.blocking_send(Err(StorageError::Generic(e.into())));
                    return IterAction::Stop;
                }
            };
            if tx.blocking_send(Ok(res)).is_err() {
                return IterAction::Stop;
            }
            // the channel is not closed yet, keep iterating
            IterAction::Next
        }
    }

    #[allow(clippy::type_complexity)]
    fn iterator_step_for_each(
        tx: oneshot::Sender<StorageError>,
        mut f: impl FnMut((&[u8], &[u8])) -> ControlFlow<Result<()>> + Send + 'static,
    ) -> impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send {
        let mut tx = Some(tx);
        move |item| {
            let mut must_send = |v| match tx.take() {
                Some(tx) => tx.send(v),
                None => panic!("Iterator continued after IterAction::Stop"),
            };

            match item {
                // apply the caller's function
                Ok((key, value)) => match f((key, value)) {
                    ControlFlow::Continue(()) => match &mut tx {
                        Some(tx_inner) if tx_inner.is_closed() => {
                            tx = None;
                            IterAction::Stop
                        }
                        // the channel is not closed yet, keep iterating
                        Some(_) => IterAction::Next,
                        None => panic!("Iterator continued after IterAction::Stop"),
                    },
                    ControlFlow::Break(Ok(())) => {
                        // function doesn't need any more items
                        tx = None;
                        IterAction::Stop
                    }
                    ControlFlow::Break(Err(e)) => {
                        let _ = must_send(e);
                        IterAction::Stop
                    }
                },
                Err(e) => {
                    let _ = must_send(StorageError::Generic(e.into()));
                    IterAction::Stop
                }
            }
        }
    }

    pub fn iterator_for_each<K: EncodeTableKeyPrefix>(
        &self,
        name: &'static str,
        priority: Priority,
        scan: TableScan<K>,
        f: impl FnMut((&[u8], &[u8])) -> std::ops::ControlFlow<Result<()>> + Send + 'static,
    ) -> Result<impl Future<Output = Result<()>>, AdmissionClosed> {
        let mut opts = ReadOptions::default();
        opts.set_async_io(true);
        self.iterator_for_each_physical(name, priority, opts, scan.into(), f)
    }

    /// Looks up the keys of `ids` with batched multi-gets of up to `batch_size` keys on the
    /// low-priority storage pool, and calls `f` in `ids` order for each id that has a value.
    ///
    /// `ids` must be ordered like their encoded keys, since the multi-get assumes sorted input.
    pub(crate) fn multi_get_for_each<I, K, const N: usize>(
        &self,
        name: &'static str,
        batch_size: NonZeroUsize,
        ids: BTreeSet<I>,
        to_key: impl Fn(&I) -> K + Send + 'static,
        mut f: impl FnMut(&I, &[u8]) -> ControlFlow<Result<()>> + Send + 'static,
    ) -> impl Future<Output = Result<()>> + Send
    where
        I: Send + 'static,
        K: FixedLengthTableKey<N>,
    {
        let db = self.db.clone();
        async move {
            let read = db.prepare_read(name, StorageTaskKind::MultiGet, move |raw_db, cfs| {
                let cf = cfs.get(K::KEY_KIND);
                let batch_size = batch_size.get();

                let batch_capacity = ids.len().min(batch_size);
                let mut batch_ids = Vec::with_capacity(batch_capacity);
                let mut keys = Vec::with_capacity(batch_capacity);

                let mut readopts = ReadOptions::default();
                // future proofing to make use of parallel L0 reads and async-io
                // if/when we build rocksdb with COROUTINES=1 and IO-URING support.
                // by default, this will not do anything.
                readopts.set_async_io(true);
                readopts.set_optimize_multiget_for_io(true);

                let mut ids = ids.into_iter().peekable();
                while ids.peek().is_some() {
                    batch_ids.clear();
                    keys.clear();
                    for id in ids.by_ref().take(batch_size) {
                        keys.push(to_key(&id).to_bytes());
                        batch_ids.push(id);
                    }

                    let results = raw_db.batched_multi_get_cf_opt(cf, &keys, true, &readopts);

                    for (id, result) in batch_ids.iter().zip(results) {
                        let Some(value) = result.map_err(|e| StorageError::Generic(e.into()))?
                        else {
                            continue;
                        };
                        if let ControlFlow::Break(result) = f(id, &value) {
                            return result;
                        }
                    }
                }
                Ok(())
            });
            db.rocksdb()
                .executor()
                .low_pri()
                .spawn(read)
                .map_err(|_| StorageError::OperationalError)?
                .await
                .map_err(|_| StorageError::OperationalError)?
        }
    }

    pub fn iterator_filter_map<K: EncodeTableKeyPrefix, O: Send + 'static>(
        &self,
        name: &'static str,
        priority: Priority,
        scan: TableScan<K>,
        f: impl FnMut((&[u8], &[u8])) -> Result<Option<O>> + Send + 'static,
    ) -> Result<ReceiverStream<Result<O>>, AdmissionClosed> {
        let (tx, rx) = mpsc::channel(8);
        let on_iter = Self::iterator_step_filter_map(tx, f);
        let mut opts = ReadOptions::default();
        opts.set_async_io(true);
        self.run_iterator_internal(name, priority, opts, scan.into(), on_iter)?;
        Ok(ReceiverStream::new(rx))
    }

    /// Starts an iterator from already-encoded bounds without re-encoding them.
    pub(crate) fn iterator_for_each_physical(
        &self,
        name: &'static str,
        priority: Priority,
        opts: ReadOptions,
        scan: PhysicalScan<Bytes>,
        f: impl FnMut((&[u8], &[u8])) -> ControlFlow<Result<()>> + Send + 'static,
    ) -> Result<impl Future<Output = Result<()>>, AdmissionClosed> {
        let (tx, rx) = oneshot::channel();
        let on_iter = Self::iterator_step_for_each(tx, f);
        self.run_iterator_internal(name, priority, opts, scan, on_iter)?;

        Ok(async {
            match rx.await {
                Ok(storage_err) => Err(storage_err),
                // The iterator completed without reporting a storage error.
                Err(_recv_err) => Ok(()),
            }
        })
    }

    fn run_iterator_internal(
        &self,
        name: &'static str,
        priority: Priority,
        mut opts: ReadOptions,
        scan: PhysicalScan<Bytes>,
        on_iter: impl FnMut(Result<(&[u8], &[u8]), RocksError>) -> IterAction + Send + 'static,
    ) -> Result<(), AdmissionClosed> {
        scan.configure(&mut opts);
        RocksDb::run_background_iterator(
            self.db.physical_scope(),
            self.db.gate(),
            scan.key_kind(),
            name,
            priority,
            IterAction::Seek(scan.into_start()),
            opts,
            on_iter,
        )?;
        Ok(())
    }

    /// Returns a watch of the durably persisted applied LSN, or [`Lsn::INVALID`]
    /// when no durable position is known or the old column family has been removed.
    pub async fn get_durable_lsn(&mut self) -> Result<watch::Receiver<Lsn>> {
        let needs_recovery = *self.db.durable_lsn_sender().borrow() == Lsn::INVALID;
        if needs_recovery && let Some(lsn) = get_locally_durable_lsn(self)? {
            self.db.note_durable_lsn(lsn);
        }

        let mut rx = self.db.durable_lsn_sender().subscribe();
        rx.mark_changed();
        Ok(rx)
    }

    pub fn transaction(&mut self) -> PartitionStoreTransaction<'_> {
        // An optimization to avoid looking up the cf handle everytime, if we split into more
        // column families, we will need to cache those cfs here as well.
        let data_cf_handle = self.db.cf_handle();

        // 99.9% of the time, this will return an already loaded value.
        // If PartitionStore.storage_features() was never called before,
        // this will fetch the value and cache it.
        let storage_features = self.storage_features();
        let maintained_indexes = self.maintained_indexes();
        let settings = TransactionSettings::from(&Configuration::pinned().worker.storage);

        PartitionStoreTransaction {
            write_batch_with_index: Some(rocksdb::WriteBatchWithIndex::new(0, true)),
            data_cf_handle,
            rocksdb: self.db.rocksdb(),
            admission: self.db.gate(),
            key_buffer: &mut self.key_buffer,
            value_buffer: &mut self.value_buffer,
            meta: self.db.partition(),
            storage_features,
            maintained_indexes,
            settings,
            commit_fence: None,
            write_batch_size_histogram: None,
            fsm_position: None,
            fsm_position_pending: false,
            #[cfg(any(test, feature = "faults"))]
            faults: self.db.faults(),
        }
    }

    /// Creates a snapshot of the partition in the given directory, which must not exist prior to
    /// the export. `capture` keeps the partition from changing until the export finishes, so the
    /// applied LSN and compatibility read from the store match the exported data.
    ///
    /// *NB:* Creating a snapshot causes an implicit flush of the column family!
    ///
    /// See [rocksdb::checkpoint::Checkpoint::export_column_family] for additional implementation details.
    pub(crate) async fn create_local_snapshot(
        &mut self,
        capture: CaptureGuard,
        snapshot_base_path: &Path,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
    ) -> Result<LocalPartitionSnapshot> {
        let applied_lsn = self
            .get_applied_lsn()
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?
            .ok_or(StorageError::DataIntegrityError)?; // if PartitionStore::get_applied_lsn returns None

        if let Some(min_target_lsn) = min_target_lsn
            && applied_lsn < min_target_lsn
        {
            return Err(StorageError::PreconditionFailed(anyhow!(
                "Applied LSN below the target LSN: {applied_lsn} < {min_target_lsn}",
            )));
        };

        // RocksDB will create the snapshot directory but the parent must exist first:
        tokio::fs::create_dir_all(snapshot_base_path)
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?;

        let compatibility = self.read_snapshot_compatibility();
        let position = self
            .get_fsm_position()
            .await
            .map_err(|e| StorageError::SnapshotExport(e.into()))?
            .filter(FsmPosition::has_byte_offsets);
        // The export holds the capture until RocksDB is done, even if we stop waiting.
        //
        // It runs on the lifecycle pool, next to creating and importing partition stores, rather
        // than on the regular storage I/O pools:
        // - The processor's commits are held back until it's done, so it shouldn't wait behind
        //   regular storage I/O.
        // - It keeps its thread while RocksDB flushes the partition. On the high-priority pool,
        //   concurrent exports could take the threads that every partition's commits use when
        //   they can't write inline.
        // Nothing on the lifecycle pool waits for an export, so they can't deadlock: closing a
        // database only reaches the pool once its last user is gone, and opening or deleting a
        // partition first needs the partition's lock, which the export holds.
        let export = self
            .db
            .prepare_export_cf(export_dir(snapshot_base_path, snapshot_id), move || capture);
        let (base_dir, export_files) = async {
            let export = RocksDbManager::get().lifecycle_executor().spawn(export)?;
            anyhow::Ok(export.await??)
        }
        .await
        .map_err(StorageError::SnapshotExport)?;

        trace!(
            cf_name = %self.db.partition().cf_name(),
            %applied_lsn,
            ?compatibility,
            "Exported column family snapshot to {:?}",
            base_dir.path()
        );

        Ok(LocalPartitionSnapshot {
            base_dir,
            files: export_files.get_files(),
            db_comparator_name: export_files.get_db_comparator_name(),
            log_id: self.db.partition().log_id(),
            min_applied_lsn: applied_lsn,
            key_range: self.db.partition().key_range,
            compatibility,
            byte_offsets_base_lsn: position.map(|p| p.byte_offsets_base_lsn),
            log_byte_offset: position.map(|p| p.log_byte_offset),
            partition_store_byte_offset: position.map(|p| p.partition_store_byte_offset),
        })
    }

    /// Reads what a server needs in order to use this partition's data from the persisted values
    /// that [`Self::verify_and_run_migrations`] checks. `None` if they can't be read.
    fn read_snapshot_compatibility(&self) -> Option<SnapshotCompatibility> {
        let db = self.partition_db();
        let read = || -> Result<_> {
            let features = get_storage_features_from_partition_db(db)?.unwrap_or_default();
            let barrier = get_min_restate_version_from_partition_db(db)?;
            let partition_features = get_state_machine_features_from_partition_db(db)?;
            Ok(SnapshotCompatibility {
                min_restate_version: match features.get_required_min_version() {
                    Some(required) => required.max(barrier),
                    None => barrier,
                },
                storage_features: features.names().cloned().collect(),
                partition_features: partition_features
                    .enabled_names()
                    .map(ReString::from_static)
                    .collect(),
            })
        };
        read()
            .inspect_err(
                |err| warn!(%err, "Failed to read the storage compatibility of a snapshot"),
            )
            .ok()
    }

    pub fn partition(&self) -> &Arc<Partition> {
        self.db.partition()
    }

    pub async fn get_seal_marker(&mut self) -> Result<Option<PartitionSeal>> {
        get_partition_seal(self, self.partition_id()).await
    }

    pub async fn seal(&mut self, seal: &PartitionSeal) -> Result<()> {
        seal_partition(self, seal).await
    }

    /// Returns `true` if the one-time cleanup of orphaned `jc` index entries has not yet been
    /// performed on this partition store.
    pub async fn needs_jc_orphan_cleanup(&mut self) -> Result<bool> {
        is_jc_orphan_cleanup_done(self, self.partition_id())
            .await
            .map(|done| !done)
    }

    /// Marks the one-time `jc` orphan cleanup as complete so it won't run again.
    pub async fn mark_jc_orphan_cleanup_done(&mut self) -> Result<()> {
        put_jc_orphan_cleanup_done(self, self.partition_id()).await
    }

    #[instrument(level = "info", skip_all, fields(partition_id = %self.partition().id()))]
    pub async fn verify_and_run_migrations(
        &mut self,
        cancel: CancellationToken,
        config: &Configuration,
    ) -> Result<(), MigrationError> {
        self.verify_and_run_migrations_at_version(SemanticRestateVersion::current(), cancel, config)
            .await
    }

    pub(crate) async fn verify_and_run_migrations_at_version(
        &mut self,
        current_restate_version: &SemanticRestateVersion,
        cancel: CancellationToken,
        config: &Configuration,
    ) -> Result<(), MigrationError> {
        // Migration decisions use authoritative on-disk metadata rather than either memoized
        // value. A transaction created before initialization may have populated a stale cache.
        let mut storage_version = get_storage_version_from_partition_db(self.partition_db())?;
        let persisted_features = get_storage_features_from_partition_db(self.partition_db())?;
        let mut current_min_restate_version =
            get_min_restate_version_from_partition_db(self.partition_db())?;

        let mut storage_features = match LoadedStorageFeatures::load(
            persisted_features,
            storage_version,
            current_restate_version,
        ) {
            Ok(features) => features,
            Err(barrier) => {
                return Err(MigrationError::StorageFeatureVersionBarrier {
                    required_min_version: barrier.required_min_version,
                    storage_features: barrier.features,
                });
            }
        };

        // There is another reason (other than storage features) why the min version is set high.
        // NOTE: We do the version check _after_ `LoadedStorageFeatures::load()` because the former
        // will give higher-quality details about the features blocking the startup.
        if !current_restate_version.is_equal_or_newer_than(&current_min_restate_version) {
            return Err(MigrationError::VersionBarrier {
                required_min_version: current_min_restate_version,
            });
        }

        // A store without an applied LSN is empty because normal state changes and the applied LSN
        // are committed in the same partition-store transaction.
        let is_store_empty = self.get_applied_lsn().await?.is_none();
        if !is_store_empty && matches!(storage_version, StorageVersion::None) {
            // Version 1.6+ does not support upgrading from pre-1.5 because the invocation-status V1
            // migration was removed in 1.6.
            return Err(MigrationError::MigrationBarrier(
                "Cannot upgrade from version <1.5 directly to 1.6 or later. \
             Please upgrade to version 1.5 first, which will migrate your data, \
             and then upgrade to 1.6+"
                    .to_owned(),
            ));
        }

        // An unsupported or invalid registry fails the open before any migration
        // below mutates the store.
        self.validate_registry(storage_features.enabled())?;

        // Registered indexes replace the legacy status gauges. Delete them once
        // every compatibility check above passed, before any family can be
        // activated below.
        self.remove_legacy_status_gauges(&mut storage_features)
            .await?;

        // Changes might be empty, but storage_features can still be dirty due to the automatic
        // convergence that happens based on storage-version or if the store is empty.
        let features_to_enable =
            storage_features.automatic_changes(config, current_restate_version, is_store_empty);

        // Do we need to perform migrations or data movement for these features?
        for feature in features_to_enable {
            // Enable each feature independently
            feature
                .enable(
                    self,
                    current_restate_version,
                    &mut current_min_restate_version,
                    &mut storage_version,
                    is_store_empty,
                    &cancel,
                    config,
                    &mut storage_features,
                )
                .await?;
            self.set_storage_features(*storage_features.enabled());
        }

        if storage_features.is_dirty() {
            put_storage_features(self, self.partition().id(), storage_features.ledger())?;
            storage_features.mark_persisted();
        }
        self.set_storage_features(*storage_features.enabled());

        // Before the processor starts writing: retire, reset and register
        // indexes, start their walks, fence the store, and install the indexes
        // maintained on writes.
        self.reconcile_registry(
            crate::registry::DEFINITIONS,
            &crate::registry::Requests::from_config(crate::registry::FAMILIES, config),
            crate::registry::FenceState {
                features: &mut storage_features,
                current_version: current_restate_version,
                min_restate_version: &mut current_min_restate_version,
            },
        )
        .await?;

        // Keep StorageVersion aligned for older binaries that used it as the sole signal for these
        // migrations. The combined variant is accurate only when both features are enabled; a
        // migration that completes the pair writes it atomically in its finalization batch above.
        // This fallback also converges stores initialized from older or incomplete metadata.
        let min_storage_version = if storage_features
            .enabled()
            .is_migrated_to_scoped_promise_table
            && storage_features.enabled().is_migrated_to_scoped_state_table
        {
            StorageVersion::ScopedStateAndPromise
        } else if is_store_empty && matches!(storage_version, StorageVersion::None) {
            StorageVersion::V1_5
        } else {
            storage_version
        };
        if min_storage_version > storage_version {
            put_storage_version(self, self.partition().id(), min_storage_version as u16).await?;
        }

        // Another safety net.
        if let Some(new_min) = storage_features.ledger().get_required_min_version()
            && new_min.is_newer_than(&current_min_restate_version)
        {
            put_min_restate_version(self, self.partition().id(), &new_min).await?;
        }

        debug_assert_eq!(self.storage_features(), *storage_features.enabled());
        Ok(())
    }

    pub fn get_storage_features_names(&self) -> Vec<ReString> {
        self.storage_features().into_names()
    }
}

impl Storage for PartitionStore {
    type TransactionType<'a> = PartitionStoreTransaction<'a>;

    fn transaction(&mut self) -> Self::TransactionType<'_> {
        PartitionStore::transaction(self)
    }
}

impl StorageRead for PartitionStore {
    type Iterator<'a>
        = DBRawIteratorWithThreadMode<'a, DB>
    where
        Self: 'a;

    fn iterator_from<K: EncodeTableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<Self::Iterator<'_>> {
        let mut opts = ReadOptions::default();
        opts.set_async_io(true);
        self.db.scan(scan.into(), opts)
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(
        &self,
        key_kind: KeyKind,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>> {
        let cf = self.cf_for_key_kind(key_kind);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .get_pinned_cf(cf, key)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.key_buffer.clear();
        self.key_buffer.reserve(min_size);
        &mut self.key_buffer
    }
}

impl StorageAccess for PartitionStore {
    #[inline]
    fn cleared_value_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.value_buffer.clear();
        self.value_buffer.reserve(min_size);
        &mut self.value_buffer
    }

    #[inline]
    fn put_cf(
        &mut self,
        table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let table = self.table_handle(table);
        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .put_cf_opt(table, key, value, &opts)
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) -> Result<()> {
        let table = self.table_handle(table);
        let mut opts = rocksdb::WriteOptions::default();
        opts.disable_wal(true);
        self.db
            .rocksdb()
            .inner()
            .as_raw_db()
            .delete_cf_opt(table, key, &opts)
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ScanMode {
    /// Scan is bound to a single fixed key prefix (partition id, or a single partition key).
    WithinPrefix,
    /// Scan/iterator requires total order seek, this means that the iterator is not bound to a
    /// fixed prefix that matches the column family prefix extractor length. For instance, if
    /// scanning data across multiple partition IDs or multiple partition keys.
    TotalOrder,
}

impl ScanMode {
    // Deduces the scan mode from whether the start and end keys share a prefix.
    pub(crate) fn from_range<S, E>(start: S, end: E) -> Self
    where
        S: AsRef<[u8]>,
        E: AsRef<[u8]>,
    {
        let start_prefix = start.as_ref().first_chunk::<PREFIX_EXTRACTOR_LENGTH>();
        let end_prefix = end.as_ref().first_chunk::<PREFIX_EXTRACTOR_LENGTH>();

        if start_prefix.is_some() && start_prefix == end_prefix {
            ScanMode::WithinPrefix
        } else {
            ScanMode::TotalOrder
        }
    }
}

/// Tunables snapshotted from the configuration when a transaction is created.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TransactionSettings {
    /// Probability that a vqueue metadata update is written as a full value instead of a merge.
    pub vqueue_meta_full_write_probability: f64,
}

impl From<&StorageOptions> for TransactionSettings {
    fn from(opts: &StorageOptions) -> Self {
        Self {
            vqueue_meta_full_write_probability: opts
                .vqueue_meta_full_write_probability
                .clamp(0.0, 1.0),
        }
    }
}

pub struct PartitionStoreTransaction<'a> {
    meta: &'a Arc<Partition>,
    write_batch_with_index: Option<rocksdb::WriteBatchWithIndex>,
    rocksdb: &'a Arc<RocksDb>,
    admission: &'a Arc<Gate>,
    data_cf_handle: &'a Arc<BoundColumnFamily<'a>>,
    key_buffer: &'a mut BytesMut,
    value_buffer: &'a mut BytesMut,
    storage_features: StorageFeatures,
    maintained_indexes: Arc<MaintainedIndexes>,
    settings: TransactionSettings,
    /// Resolves once the [`CommitFence`] holding back commits is dropped.
    commit_fence: Option<oneshot::Receiver<()>>,
    write_batch_size_histogram: Option<&'a Histogram>,
    /// The partition's position, if this transaction maintains it.
    fsm_position: Option<FsmPosition>,
    /// Whether the next commit persists the position even if it has nothing else to write,
    /// because the applied LSN advanced while only the position holds it.
    fsm_position_pending: bool,
    #[cfg(any(test, feature = "faults"))]
    faults: &'a crate::faults::Faults,
}

/// Holds back the commits of the [`PartitionStoreTransaction`] that created it until dropped.
/// A snapshot export holds one so that the partition doesn't change under it.
pub struct CommitFence {
    _release: oneshot::Sender<()>,
}

impl<'a> PartitionStoreTransaction<'a> {
    pub(crate) fn settings(&self) -> &TransactionSettings {
        &self.settings
    }

    /// Holds back commits until the returned fence is dropped. Writes still go into the
    /// transaction's buffer meanwhile; only [`Transaction::commit`] waits, even when there's
    /// nothing to commit. `None` if another fence still holds commits back.
    pub fn fence_commits(&mut self) -> Option<CommitFence> {
        if let Some(released) = &mut self.commit_fence
            && matches!(
                released.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            )
        {
            return None;
        }
        let (release, released) = oneshot::channel();
        self.commit_fence = Some(released);
        Some(CommitFence { _release: release })
    }

    /// Records the size in bytes of every write batch that later commits write to the database.
    /// Commits with nothing to write record nothing. Applies until set again, also across
    /// [`Self::clear`] and [`Transaction::commit`].
    pub fn set_write_batch_size_histogram(&mut self, histogram: &'a Histogram) {
        self.write_batch_size_histogram = Some(histogram);
    }

    /// Stops recording write batch sizes until a histogram is set again, so that
    /// commits that are not command batches stay out of the command batches'
    /// histogram.
    pub fn clear_write_batch_size_histogram(&mut self) {
        self.write_batch_size_histogram = None;
    }

    /// Lets this transaction maintain the partition's position, starting from `position`, the
    /// one at the partition's applied LSN. [`Self::advance_fsm_position`] advances it with every
    /// record applied, and every later commit that writes something persists it while the
    /// partition tracks byte offsets, so that each commit persists the position of the applied
    /// LSN in that commit. It's kept across [`Self::clear`] and [`Transaction::commit`].
    pub fn set_fsm_position(&mut self, position: FsmPosition) {
        self.fsm_position = Some(position);
    }

    /// The partition's position, as of the records applied with this transaction so far. `None`
    /// unless the transaction maintains it.
    pub fn fsm_position(&self) -> Option<FsmPosition> {
        self.fsm_position
    }

    /// Advances the partition's position, if this transaction maintains it, to the record or gap
    /// that ends at `lsn`, of `bytes` as the log stores it.
    pub fn advance_fsm_position(&mut self, lsn: Lsn, bytes: u64) {
        if let Some(position) = &mut self.fsm_position {
            position.advance(lsn, bytes);
        }
    }

    /// Turns byte offsets on at the record at `lsn`, which they count from. The position is
    /// written with this transaction's next commit, which also deletes the applied LSN persisted
    /// on its own: from then on, only the position holds it. Fails if this transaction doesn't
    /// maintain the partition's position (see [`Self::set_fsm_position`]).
    pub fn start_byte_offsets(&mut self, lsn: Lsn) -> Result<()> {
        let Some(position) = self.fsm_position.as_mut() else {
            return Err(StorageError::Generic(anyhow!(
                "can't turn byte offsets on: the transaction doesn't maintain the partition's \
                 position"
            )));
        };
        position.start_byte_offsets(lsn);
        let position = *position;
        delete_applied_lsn(self)?;
        put_fsm_position(self, &position)
    }

    /// Has the next commit persist the partition's position even if it has nothing else to
    /// write, for an applied LSN that only the position holds.
    pub(crate) fn mark_fsm_position_pending(&mut self) {
        self.fsm_position_pending = true;
    }

    /// Clears up all buffered operations in the transaction buffer. A commit fence stays in
    /// place, since it belongs to whoever holds the [`CommitFence`].
    pub fn clear(&mut self) {
        self.write_batch_with_index
            .get_or_insert_with(|| rocksdb::WriteBatchWithIndex::new(0, true))
            .clear();
        self.key_buffer.clear();
        self.value_buffer.clear();
    }

    #[inline]
    pub fn raw_put_cf(
        &mut self,
        key_kind: KeyKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        let cf = self.cf_for_key_kind(key_kind);
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .put_cf(cf, key, value);
    }

    #[inline]
    pub fn raw_merge_cf(
        &mut self,
        key_kind: KeyKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) {
        let cf = self.cf_for_key_kind(key_kind);
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .merge_cf(cf, key, value);
    }

    #[inline]
    pub fn raw_delete_cf(&mut self, _key_kind: KeyKind, key: impl AsRef<[u8]>) {
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .delete_cf(self.data_cf_handle, key);
    }

    #[inline]
    pub fn raw_single_delete_cf(&mut self, _key_kind: KeyKind, key: impl AsRef<[u8]>) {
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .single_delete_cf(self.data_cf_handle, key);
    }

    pub(crate) fn cf_for_key_kind(&self, _key_kind: KeyKind) -> &'a Arc<BoundColumnFamily<'a>> {
        // Right now, everything is in one cf, return a reference and save CPU.
        self.data_cf_handle
    }

    #[inline]
    pub(crate) fn partition_id(&self) -> PartitionId {
        self.meta.partition_id
    }

    /// Puts the partition's `position` into the batch, with its partition store byte offset
    /// advanced by the size of the batch, this put included. Returns the position it put.
    fn put_fsm_position_counting_batch(
        &mut self,
        mut position: FsmPosition,
    ) -> Result<FsmPosition> {
        let batch_size = self.estimated_size_in_bytes();
        // A position's encoded size only depends on which of its fixed-width fields are zero. Its
        // store offset isn't once it counts the batch, unless it wraps to exactly zero: it then
        // counts the 9 bytes that its omitted field doesn't take.
        let encoded_len = bilrost::Message::encoded_len(&FsmPosition {
            partition_store_byte_offset: u64::MAX,
            ..position
        });
        let put_size = write_batch_put_size(
            self.data_cf_handle.id(),
            PartitionStateMachineKey::serialized_length_fixed(),
            // StorageCodec's codec byte and the message
            1 + encoded_len,
        );
        position.add_partition_store_bytes((batch_size + put_size) as u64);
        put_fsm_position(self, &position)?;
        // The size predicted above follows RocksDB's write batch format. Were it ever off, e.g.
        // after a change of that format, the position just put would be off by the difference:
        // carry on from the size actually added, so that the next commit is exact again.
        let added = self.estimated_size_in_bytes() - batch_size;
        if added != put_size {
            debug_assert_eq!(
                added, put_size,
                "the position's put adds the size it predicts"
            );
            static LOGGED: Once = Once::new();
            LOGGED.call_once(|| {
                warn!(
                    predicted = put_size,
                    added,
                    "The partition store byte offset counts a different size for the FSM \
                     position's put than it adds to the write batch; offsets may be off by the \
                     difference"
                )
            });
            position.partition_store_byte_offset = position
                .partition_store_byte_offset
                .wrapping_add(added as u64)
                .wrapping_sub(put_size as u64);
        }
        Ok(position)
    }

    #[inline]
    pub(crate) fn storage_features(&self) -> StorageFeatures {
        self.storage_features
    }

    pub(crate) fn maintained_indexes(&self) -> Arc<MaintainedIndexes> {
        Arc::clone(&self.maintained_indexes)
    }

    /// Replaces the indexes maintained on writes, once a commit changed them.
    pub(crate) fn set_maintained_indexes(&mut self, maintained_indexes: Arc<MaintainedIndexes>) {
        self.maintained_indexes = maintained_indexes;
    }

    /// Whether a [`CommitFence`] currently holds back commits. Neither takes nor lifts a
    /// fence; it only forgets one already released.
    pub(crate) fn commits_held(&mut self) -> bool {
        let held = self.commit_fence.as_mut().is_some_and(|released| {
            matches!(
                released.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            )
        });
        if !held {
            self.commit_fence = None;
        }
        held
    }

    /// Marks the buffered writes, so that [`Self::rollback_to_save_point`] can drop
    /// the ones that follow. Save points nest, and a commit or [`Self::clear`]
    /// drops them.
    pub(crate) fn set_save_point(&mut self) {
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .set_save_point();
    }

    /// Drops the writes buffered since the latest save point, and that save point.
    pub(crate) fn rollback_to_save_point(&mut self) -> Result<()> {
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .rollback_to_save_point()
            .map_err(|err| StorageError::Generic(err.into()))
    }

    /// Whether no write is buffered.
    pub(crate) fn is_empty(&self) -> bool {
        self.write_batch_with_index
            .as_ref()
            .is_none_or(|batch| batch.is_empty())
    }

    #[inline]
    pub(crate) fn assert_partition_key(&self, partition_key: &impl WithPartitionKey) -> Result<()> {
        assert_partition_key_or_err(self.meta.key_range, partition_key)
    }
}

impl<'a> PartitionStoreTransaction<'a> {
    /// Merges into the statistic keys of index `id`.
    pub(crate) fn aggregated_stats<'b>(&'b mut self, id: u32) -> AggregatedStatsMut<'b, 'a> {
        AggregatedStatsMut::new(self, id)
    }
}

/// The bytes that a put adds to a RocksDB write batch, as RocksDB's write batch format lays it out:
/// the record type, the column family's id unless it's the default one, then the key and the
/// value, each after its length. The numbers are varint32s.
fn write_batch_put_size(cf_id: u32, key_len: usize, value_len: usize) -> usize {
    let varint32_len = |n: usize| (usize::BITS - n.leading_zeros()).div_ceil(7).max(1) as usize;
    let cf_id_len = if cf_id == 0 {
        0
    } else {
        varint32_len(cf_id as usize)
    };
    1 + cf_id_len + varint32_len(key_len) + key_len + varint32_len(value_len) + value_len
}

pub(crate) fn assert_partition_key_or_err(
    partition_key_range: KeyRange,
    partition_key: &impl WithPartitionKey,
) -> Result<()> {
    let partition_key = partition_key.partition_key();
    if partition_key_range.contains(&partition_key) {
        return Ok(());
    }
    Err(StorageError::Generic(anyhow!(
        "Partition key '{partition_key}' is not part of PartitionStore's partition '{partition_key_range:?}'. This indicates a bug."
    )))
}

impl Transaction for PartitionStoreTransaction<'_> {
    async fn commit(&mut self) -> Result<()> {
        // A snapshot export must not see writes made after it started, so wait for it to finish
        // first. This happens even when there's nothing to commit: the version barrier commits
        // right before its migrations write to the partition outside this transaction, and it
        // relies on this wait to keep those writes out of the export too. It also happens before
        // taking the admission lease, so that closing the partition doesn't wait for the export.
        if let Some(released) = &mut self.commit_fence {
            let _ = released
                .log_slow_after(
                    LOG_SLOW_COMMIT_FENCE_AFTER,
                    Level::WARN,
                    format!(
                        "Partition {} holding back commits for a snapshot export",
                        self.meta.partition_id
                    ),
                )
                .await;
            self.commit_fence = None;
        }

        if !self.fsm_position_pending
            && self
                .write_batch_with_index
                .as_ref()
                .is_none_or(|batch| batch.is_empty())
        {
            return Ok(());
        }

        // Rejection must preserve buffered writes. Once admitted, the write owns
        // this lease through inline execution or background completion.
        let lease = self
            .admission
            .try_acquire()
            .ok_or(StorageError::OperationalError)?;
        // The position this commit persists, kept once the write succeeded.
        let position = self
            .fsm_position
            .filter(FsmPosition::has_byte_offsets)
            .map(|position| self.put_fsm_position_counting_batch(position))
            .transpose()?;
        let write_batch = self.write_batch_with_index.take().expect("nonempty batch");
        if let Some(histogram) = self.write_batch_size_histogram {
            histogram.record(write_batch.size_in_bytes() as f64);
        }

        let io_mode = if Configuration::pinned()
            .worker
            .storage
            .always_commit_in_background
        {
            IoMode::AlwaysBackground
        } else {
            IoMode::Default
        };
        let mut opts = rocksdb::WriteOptions::default();
        // We disable WAL since bifrost is our durable distributed log.
        opts.disable_wal(true);
        let write = self.rocksdb.write_batch_with_index(
            "partition-store-txn-commit",
            Priority::High,
            io_mode,
            opts,
            write_batch,
            lease,
        );
        #[cfg(any(test, feature = "faults"))]
        let write = crate::faults::queued(self.faults, write);
        self.write_batch_with_index = Some(
            write
                .await
                .map_err(|error| StorageError::Generic(error.into()))?,
        );
        self.write_batch_with_index.as_mut().unwrap().clear();
        if position.is_some() {
            self.fsm_position = position;
        }
        self.fsm_position_pending = false;
        Ok(())
    }

    fn estimated_size_in_bytes(&self) -> usize {
        self.write_batch_with_index
            .as_ref()
            .map(|wbwi| wbwi.size_in_bytes())
            .unwrap_or_default()
    }
}

impl StorageRead for PartitionStoreTransaction<'_> {
    type Iterator<'b>
        = DBRawIteratorWithThreadMode<'b, DB>
    where
        Self: 'b;

    fn iterator_from<K: EncodeTableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<Self::Iterator<'_>> {
        let scan: PhysicalScan<Bytes> = scan.into();
        let (cf, start, opts) = match scan {
            PhysicalScan::Prefix(key_kind, prefix) => {
                let mut opts = ReadOptions::default();
                configure_prefix_iterator_opts(&mut opts, prefix.as_ref());
                (self.cf_for_key_kind(key_kind), prefix, opts)
            }
            PhysicalScan::RangeExclusive(key_kind, scan_mode, start, end) => {
                let mut opts = ReadOptions::default();
                configure_range_iterator_opts(&mut opts, scan_mode, start.as_ref(), end);
                (self.cf_for_key_kind(key_kind), start, opts)
            }
        };

        let base = self
            .rocksdb
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(cf, opts);
        let mut iterator = self
            .write_batch_with_index
            .as_ref()
            .expect("transaction valid")
            .iterator_with_base_cf(base, cf);
        iterator.seek(start);
        Ok(iterator)
    }

    #[inline]
    fn get<K: AsRef<[u8]>>(
        &self,
        key_kind: KeyKind,
        key: K,
    ) -> Result<Option<DBPinnableSlice<'_>>> {
        let cf = self.cf_for_key_kind(key_kind);
        let read_options = ReadOptions::default();
        self.write_batch_with_index
            .as_ref()
            .expect("transaction valid")
            .get_pinned_from_batch_and_db_cf(
                self.rocksdb.inner().as_raw_db(),
                cf,
                key,
                &read_options,
            )
            .map_err(|error| StorageError::Generic(error.into()))
    }

    #[inline]
    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.key_buffer.clear();
        self.key_buffer.reserve(min_size);
        self.key_buffer
    }
}

impl StorageAccess for PartitionStoreTransaction<'_> {
    #[inline]
    fn cleared_value_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut {
        self.value_buffer.clear();
        self.value_buffer.reserve(min_size);
        self.value_buffer
    }

    #[inline]
    fn put_cf(
        &mut self,
        _table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .put_cf(self.data_cf_handle, key, value);
        Ok(())
    }

    #[inline]
    fn delete_cf(&mut self, _table: TableKind, key: impl AsRef<[u8]>) -> Result<()> {
        self.write_batch_with_index
            .as_mut()
            .expect("transaction valid")
            .delete_cf(self.data_cf_handle, key);
        Ok(())
    }
}

pub(crate) trait StorageAccess: StorageRead {
    fn cleared_value_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut;

    fn put_cf(
        &mut self,
        table: TableKind,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()>;

    fn delete_cf(&mut self, table: TableKind, key: impl AsRef<[u8]>) -> Result<()>;

    #[inline]
    fn put_kv_raw<K: EncodeTableKey, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<()> {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        self.put_cf(K::TABLE, key_buffer, value)
    }

    #[inline]
    fn put_kv_proto<K: EncodeTableKey, V: PartitionStoreProtobufValue + Clone + 'static>(
        &mut self,
        key: K,
        value: &V,
    ) -> Result<()> {
        self.put_kv_storage_codec(
            key,
            &ProtobufStorageWrapper::<V::ProtobufType>(value.clone().into()),
        )
    }

    #[inline]
    fn put_kv_proto_owned<K: EncodeTableKey, V: PartitionStoreProtobufValue + 'static>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<()> {
        self.put_kv_storage_codec(
            key,
            &ProtobufStorageWrapper::<V::ProtobufType>(value.into()),
        )
    }

    #[inline]
    fn put_kv_storage_codec<K: EncodeTableKey, V: StorageEncode + 'static>(
        &mut self,
        key: K,
        value: &V,
    ) -> Result<()> {
        let key_buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buffer);
        let key_buffer = key_buffer.split();

        let value_buffer = self.cleared_value_buffer_mut(0);
        StorageCodec::encode(value, value_buffer).map_err(|e| StorageError::Generic(e.into()))?;
        let value_buffer = value_buffer.split();

        self.put_cf(K::TABLE, key_buffer, value_buffer)
    }

    #[inline]
    fn delete_key<K: EncodeTableKey>(&mut self, key: &K) -> Result<()> {
        let buffer = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(buffer);
        let buffer = buffer.split();

        self.delete_cf(K::TABLE, buffer)
    }
}

/// A cursor over the entries of a scan, implemented for both RocksDB's raw iterators and
/// snapshot scans.
pub(crate) trait StorageIterator {
    /// The key and value at the current position. Returns `Ok(None)` once the iterator has moved
    /// past the last entry, or the error that stopped it.
    #[allow(clippy::type_complexity)]
    fn item(&self) -> Result<Option<(&[u8], &[u8])>>;

    /// Moves to the next entry.
    fn next(&mut self);
}

impl<D: rocksdb::DBAccess> StorageIterator for DBRawIteratorWithThreadMode<'_, D> {
    #[inline]
    fn item(&self) -> Result<Option<(&[u8], &[u8])>> {
        match DBRawIteratorWithThreadMode::item(self) {
            Some(entry) => Ok(Some(entry)),
            None => self
                .status()
                .map(|()| None)
                .map_err(|err| StorageError::Generic(err.into())),
        }
    }

    #[inline]
    fn next(&mut self) {
        DBRawIteratorWithThreadMode::next(self)
    }
}

impl<H> StorageIterator for ReadIterator<'_, H> {
    #[inline]
    fn item(&self) -> Result<Option<(&[u8], &[u8])>> {
        ReadIterator::item(self).map_err(|err| StorageError::Generic(err.into()))
    }

    #[inline]
    fn next(&mut self) {
        ReadIterator::next(self)
    }
}

/// Reads of a partition's tables: point lookups and scans.
pub(crate) trait StorageRead {
    type Iterator<'a>: StorageIterator + Send
    where
        Self: 'a;

    fn iterator_from<K: EncodeTableKeyPrefix>(
        &self,
        scan: TableScan<K>,
    ) -> Result<Self::Iterator<'_>>;

    #[inline]
    fn get_first_blocking<K, F, R>(&mut self, scan: TableScan<K>, f: F) -> Result<R>
    where
        K: EncodeTableKeyPrefix,
        F: FnOnce(Option<(&[u8], &[u8])>) -> Result<R>,
    {
        let iterator = self.iterator_from(scan)?;
        f(iterator.item()?)
    }

    #[inline]
    fn for_each_key_value_in_place<K, F, R>(
        &self,
        scan: TableScan<K>,
        mut op: F,
    ) -> Result<Vec<Result<R>>>
    where
        K: EncodeTableKeyPrefix,
        F: FnMut(&[u8], &[u8]) -> TableScanIterationDecision<R>,
    {
        let mut res = Vec::new(); // TODO: this should be passed in.

        let mut iterator = self.iterator_from(scan)?;

        loop {
            let (k, v) = match iterator.item() {
                Ok(Some(item)) => item,
                Ok(None) => return Ok(res),
                Err(err) => {
                    // We stopped the iteration because of an iterator error
                    res.push(Err(err));
                    return Ok(res);
                }
            };
            match op(k, v) {
                TableScanIterationDecision::Emit(result) => {
                    res.push(result);
                    iterator.next();
                }
                TableScanIterationDecision::BreakWith(result) => {
                    res.push(result);
                    return Ok(res);
                }
                TableScanIterationDecision::Continue => {
                    iterator.next();
                }
                TableScanIterationDecision::Break => {
                    return Ok(res);
                }
            };
        }
    }

    /// Reads the value of `key`, whose kind is `key_kind`, without copying it.
    fn get<K: AsRef<[u8]>>(&self, key_kind: KeyKind, key: K)
    -> Result<Option<DBPinnableSlice<'_>>>;

    fn cleared_key_buffer_mut(&mut self, min_size: usize) -> &mut BytesMut;

    #[inline]
    fn get_value_proto<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: EncodeTableKey,
        V: PartitionStoreProtobufValue,
        <<V as PartitionStoreProtobufValue>::ProtobufType as TryInto<V>>::Error:
            Into<anyhow::Error>,
    {
        let value: Option<ProtobufStorageWrapper<V::ProtobufType>> =
            self.get_value_storage_codec(key)?;

        value
            .map(|v| v.0.try_into())
            .transpose()
            .map_err(|err| StorageError::Conversion(err.into()))
    }

    #[inline]
    fn get_value_storage_codec<K, V>(&mut self, key: K) -> Result<Option<V>>
    where
        K: EncodeTableKey,
        V: StorageDecode,
    {
        let mut buf = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(&mut buf);
        let buf = buf.split();

        self.get(K::KEY_KIND, &buf)?
            .map(|value| {
                let mut slice = value.as_ref();
                StorageCodec::decode(&mut slice)
            })
            .transpose()
            .map_err(|err| StorageError::Generic(err.into()))
    }

    #[inline]
    fn get_kv_raw<K, F, R>(&mut self, key: K, f: F) -> Result<R>
    where
        K: EncodeTableKey,
        F: FnOnce(&[u8], Option<&[u8]>) -> Result<R>,
    {
        let mut buf = self.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(&mut buf);
        let buf = buf.split();

        match self.get(K::KEY_KIND, &buf) {
            Ok(value) => {
                let slice = value.as_ref().map(|v| v.as_ref());
                f(&buf, slice)
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut};

    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::{StorageError, Transaction};
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::logs::Lsn;
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;

    use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind};
    use crate::partition_store::{StorageAccess, StorageRead};
    use crate::{PartitionSeal, PartitionStoreManager, TableKind};

    impl EncodeTableKey for String {
        const TABLE: TableKind = TableKind::State;
        const KEY_KIND: KeyKind = KeyKind::State;

        fn serialize_to<B: BufMut>(&self, bytes: &mut B) {
            Self::KEY_KIND.serialize(bytes);
            bytes.put_u32(self.len() as u32);
            bytes.put_slice(self.as_bytes());
        }

        fn serialized_length(&self) -> usize {
            KeyKind::SERIALIZED_LENGTH + self.len()
        }
    }

    impl DecodeTableKey for String {
        fn deserialize_from(bytes: &mut &[u8]) -> crate::partition_store::Result<Self> {
            let key_kind = KeyKind::deserialize(bytes)?;
            assert_eq!(key_kind, Self::KEY_KIND);

            let len = bytes.get_u32() as usize;
            let mut string_bytes = Vec::with_capacity(len);
            bytes.copy_to_slice(&mut string_bytes);
            Ok(String::from_utf8(string_bytes).expect("valid key"))
        }
    }

    /// Every active key kind must be claimed by at least one table, otherwise
    /// `TableKind::has_key_kind` returns false and prefix scans trip the
    /// `assert!(table.has_key_kind(..))` guard in the iterator paths.
    #[test]
    fn every_key_kind_belongs_to_a_table() {
        use strum::VariantArray;

        // Retired/reserved kinds intentionally map to no table: their byte
        // encodings are kept reserved but nothing reads or writes them.
        #[allow(deprecated)]
        let reserved = [
            KeyKind::Idempotency,
            KeyKind::InvocationStatusV1,
            KeyKind::LegacyStats,
        ];

        for kind in KeyKind::VARIANTS {
            if reserved.contains(kind) {
                continue;
            }
            assert!(
                TableKind::VARIANTS
                    .iter()
                    .any(|table| table.key_kinds().contains(kind)),
                "KeyKind::{kind:?} is not mapped to any TableKind::key_kinds()"
            );
        }
    }

    #[restate_core::test]
    async fn concurrent_writes_and_reads() -> googletest::Result<()> {
        RocksDbManager::init();
        let partition_store_manager = PartitionStoreManager::create(true).await?;
        let mut partition_store = partition_store_manager
            .open(
                &Partition::new(
                    PartitionId::MIN,
                    KeyRange::new(PartitionKey::MIN, PartitionKey::MAX),
                ),
                None,
            )
            .await?;
        let key_a = "a".to_owned();
        let key_b = "b".to_owned();

        // put the initial values
        partition_store.put_kv_raw(key_a.clone(), 0_u32.to_be_bytes())?;
        partition_store.put_kv_raw(key_b.clone(), 0_u32.to_be_bytes())?;

        let mut partition_store_clone = partition_store.clone();
        // a snapshot shouldn't see writes of concurrently finishing transactions
        let mut read_snapshot = partition_store.snapshot()?;

        let value_a = read_snapshot.get_kv_raw(key_a.clone(), decode_u32)?;

        // concurrent write which should not be visible by read_snapshot
        let mut write_txn = partition_store_clone.transaction();
        write_txn.put_kv_raw(key_a.clone(), 42_u32.to_be_bytes())?;
        write_txn.put_kv_raw(key_b.clone(), 42_u32.to_be_bytes())?;
        write_txn.commit().await?;

        let value_b = read_snapshot.get_kv_raw(key_b.clone(), decode_u32)?;

        assert_eq!(value_a, value_b);

        Ok(())
    }

    #[restate_core::test]
    async fn seal_is_written_to_wal() -> googletest::Result<()> {
        RocksDbManager::init();
        let partition_store_manager = PartitionStoreManager::create(true).await?;
        let mut partition_store = partition_store_manager
            .open(
                &Partition::new(
                    PartitionId::MIN,
                    KeyRange::new(PartitionKey::MIN, PartitionKey::MAX),
                ),
                None,
            )
            .await?;
        let db = partition_store.partition_db().rocksdb().clone();
        let sequence_number = db.inner().as_raw_db().latest_sequence_number();

        partition_store
            .seal(&PartitionSeal::AheadOfLog {
                partition_applied_lsn: Lsn::from(2),
                log_tail_lsn: Lsn::from(1),
            })
            .await?;

        let mut updates = db.inner().as_raw_db().get_updates_since(sequence_number)?;
        let (_, batch) = updates.next().transpose()?.expect("seal WAL entry");
        assert_eq!(batch.len(), 1);
        assert!(updates.next().is_none());

        Ok(())
    }

    fn decode_u32(_key: &[u8], bytes: Option<&[u8]>) -> Result<Option<u32>, StorageError> {
        if let Some(bytes) = bytes {
            bytes
                .try_into()
                .map(u32::from_be_bytes)
                .map(Some)
                .map_err(|err| StorageError::Conversion(err.into()))
        } else {
            Ok(None)
        }
    }
}
