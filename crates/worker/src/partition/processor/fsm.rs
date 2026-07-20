// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tokio::sync::watch;

use restate_limiter::RuleBook;
use restate_partition_store::PartitionStore;
use restate_storage_api::fsm_table::{
    CachedEpochMetadata, PartitionDurability, ReadFsmTable, WriteFsmTable,
};
use restate_storage_api::{StorageError, Transaction};
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::message::MessageIndex;
use restate_types::partitions::{PartitionFeatureChange, PersistedFeatures};
use restate_types::schema::Schema;
use restate_types::{SemanticRestateVersion, Version, Versioned};
use restate_worker_api::processor::{FsmAccess, FsmMut, PartitionFeatures};

pub(super) struct Fsm {
    last_applied_lsn: Lsn,
    /// Temporary watcher for the last applied log LSN, this is only used for ingestion client
    /// migration and should be removed in v1.8.0
    ///
    /// Note: This is only updated when we refresh the processor status and may lag
    /// behind the actual last applied LSN until the processor finishes processing
    /// a batch of bifrost records.
    last_applied_lsn_watch: watch::Sender<Lsn>,
    /// initialized from persistent storage.
    /// [Legacy] Only used for non-vqueues invocations.
    inbox_seq_number: MessageIndex,
    /// The minimum version of restate server that we currently support
    min_restate_version: SemanticRestateVersion,
    /// Set of state-machine features currently enabled on this partition.
    /// Mutated via `VersionBarrierCommand` entries carrying feature changes.
    /// *Since v1.7.0*
    enabled_features: PersistedFeatures,
    /// Consistent schema
    schema: Option<Arc<Schema>>,
    /// Persisted partition configuration state (since v1.6)
    epoch_metadata: Option<CachedEpochMetadata>,
    /// The known durable point for this partition
    durable_point: Option<PartitionDurability>,
    /// Cluster-global rule book, kept consistent across replicas via
    /// `Command::UpsertRuleBook` log entries. `Arc` because the apply
    /// path also pushes the same value into the node-level
    /// `RuleBookCache` (one allocation, cheap clones).
    rule_book: Arc<RuleBook>,
}

impl Fsm {
    #[cfg(test)]
    pub fn new(enabled_features: PersistedFeatures) -> Self {
        Self {
            last_applied_lsn: Lsn::INVALID,
            last_applied_lsn_watch: watch::Sender::new(Lsn::INVALID),
            inbox_seq_number: 0,
            min_restate_version: SemanticRestateVersion::unknown(),
            epoch_metadata: None,
            schema: None,
            durable_point: None,
            enabled_features,
            rule_book: Arc::new(RuleBook::default()),
        }
    }

    /// Overwrites the in-memory feature set without persisting it, mirroring the
    /// old `StateMachine`-embedded cache mutation used by tests.
    #[cfg(test)]
    pub fn set_enabled_features_in_memory(&mut self, enabled_features: PersistedFeatures) {
        self.enabled_features = enabled_features;
    }

    pub async fn create<S>(storage: &mut S) -> Result<Self, StorageError>
    where
        S: ReadFsmTable,
    {
        let min_restate_version = storage.get_min_restate_version().await?;
        let inbox_seq_number = storage.get_inbox_seq_number().await?;
        let enabled_features = storage.get_state_machine_features().await?;
        let durable_point = storage.get_partition_durability().await?;
        let last_applied_lsn = storage.get_applied_lsn().await?.unwrap_or(Lsn::INVALID);
        // Load persisted partition configuration state (since v1.7.2)
        let schema = storage.get_schema().await?.map(Arc::new);
        // Load persisted partition configuration state (since v1.7.0)
        let rule_book = Arc::new(storage.get_rule_book().await?.unwrap_or_default());
        // Load persisted partition configuration state (since v1.6)
        let epoch_metadata = storage.get_partition_config_state().await?;

        let last_applied_lsn_watch = watch::Sender::new(last_applied_lsn);
        Ok(Self {
            last_applied_lsn,
            last_applied_lsn_watch,
            inbox_seq_number,
            min_restate_version,
            enabled_features,
            schema,
            epoch_metadata,
            durable_point,
            rule_book,
        })
    }

    /// Returns true if verification failed due to min version violation. Otherwise,
    /// it will run initialization or any migration steps on the FSM state.
    ///
    /// If `read_only` is true, the migration steps will be skipped but any initialization
    /// that promotes default features will be applied to the in-memory cache but will not
    /// be persisted.
    pub async fn verify_and_run_migrations(
        &mut self,
        partition_store: &mut PartitionStore,
        current_restate_version: &SemanticRestateVersion,
        read_only: bool,
    ) -> Result<bool, StorageError> {
        // Fence off starting if current restate server version is older than the minimum
        // acceptable by this partition store.
        if !current_restate_version.is_equal_or_newer_than(&self.min_restate_version) {
            return Ok(false);
        }

        // for backward compatibility because PartitionFeatureChanges were only introduced with v1.7,
        // we need to enable journal v2 if the min restate version is >= v1.6.0
        if !self.enabled_features.use_journal_v2_as_default()
            && self.min_restate_version.is_equal_or_newer_than(
                PartitionFeatureChange::EnableJournalV2.min_required_version(),
            )
        {
            PartitionFeatureChange::EnableJournalV2.apply_to(&mut self.enabled_features);
            if !read_only {
                // update the internal storage
                let mut txn = partition_store.transaction();
                txn.put_state_machine_features(&self.enabled_features)?;
                txn.commit().await?;
            }
        }
        Ok(true)
    }

    pub fn enabled_features(&self) -> &PersistedFeatures {
        &self.enabled_features
    }

    // Set to pub(super) to reduce the chance of accidentally using it directly
    // without updating the replay status of the processor.
    pub(super) fn set_last_applied_lsn<S: WriteFsmTable>(&mut self, txn: &mut S, lsn: Lsn) {
        self.last_applied_lsn = lsn;
        txn.put_applied_lsn(lsn).expect("infallible serde");
    }

    pub fn release_applied_lsn(&mut self) {
        // Notify all lsn watchers that the lsn has been committed
        self.last_applied_lsn_watch
            .send_replace(self.last_applied_lsn);
    }

    pub fn subscribe_to_last_applied_lsn(&self) -> watch::Receiver<Lsn> {
        self.last_applied_lsn_watch.subscribe()
    }
}

impl FsmAccess for Fsm {
    #[inline]
    fn last_applied_lsn(&self) -> Lsn {
        self.last_applied_lsn
    }

    #[inline]
    fn inbox_seq_number(&self) -> MessageIndex {
        self.inbox_seq_number
    }

    #[inline]
    fn schema(&self) -> Option<&Arc<Schema>> {
        self.schema.as_ref()
    }

    #[inline]
    fn schema_version(&self) -> Version {
        self.schema
            .as_ref()
            .map(|s| s.version())
            .unwrap_or(Version::INVALID)
    }

    #[inline]
    fn rule_book(&self) -> &Arc<RuleBook> {
        &self.rule_book
    }

    #[inline]
    fn rule_book_version(&self) -> Version {
        self.rule_book.version()
    }

    #[inline]
    fn epoch_metadata(&self) -> Option<&CachedEpochMetadata> {
        self.epoch_metadata.as_ref()
    }

    fn epoch_metadata_version(&self) -> Version {
        self.epoch_metadata
            .as_ref()
            .map(|m| m.version)
            .unwrap_or(Version::INVALID)
    }

    #[inline]
    fn durable_point(&self) -> Option<&PartitionDurability> {
        self.durable_point.as_ref()
    }

    #[inline]
    fn min_restate_version(&self) -> &SemanticRestateVersion {
        &self.min_restate_version
    }

    #[inline]
    fn features(&self) -> impl PartitionFeatures {
        &self.enabled_features
    }
}

impl FsmMut for Fsm {
    fn set_min_restate_version<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        new_version: SemanticRestateVersion,
    ) -> bool {
        if matches!(
            new_version.cmp_precedence(&self.min_restate_version),
            std::cmp::Ordering::Greater,
        ) {
            txn.put_min_restate_version(&new_version)
                .expect("infallible serde");
            self.min_restate_version = new_version;
            true
        } else {
            false
        }
    }

    fn set_epoch_metadata<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        metadata: CachedEpochMetadata,
    ) -> bool {
        if self.epoch_metadata_version() < metadata.version {
            txn.put_partition_config_state(&metadata)
                .expect("infallible serde");
            self.epoch_metadata = Some(metadata);
            true
        } else {
            false
        }
    }

    fn set_durable_point<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        durable_point: PartitionDurability,
    ) {
        txn.put_partition_durability(&durable_point)
            .expect("infallible serde");
        self.durable_point = Some(durable_point);
    }

    fn set_schema<S: WriteFsmTable>(&mut self, txn: &mut S, schema: Arc<Schema>) {
        txn.put_schema(&schema).expect("infallible serde");
        self.schema = Some(schema);
    }

    fn set_rule_book<S: WriteFsmTable>(&mut self, txn: &mut S, rule_book: Arc<RuleBook>) {
        txn.put_rule_book(&rule_book).expect("infallible serde");
        self.rule_book = rule_book;
    }

    fn set_enabled_features<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        updated: PersistedFeatures,
    ) -> bool {
        if updated != self.enabled_features {
            txn.put_state_machine_features(&updated)
                .expect("infallible serde");
            self.enabled_features = updated;
            true
        } else {
            false
        }
    }

    fn set_inbox_seq_number<S: WriteFsmTable>(&mut self, txn: &mut S, seq: MessageIndex) {
        self.inbox_seq_number = seq;
        txn.put_inbox_seq_number(seq).expect("infallible serde");
    }
}
