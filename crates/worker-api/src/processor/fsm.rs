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

use restate_limiter::RuleBook;
use restate_storage_api::fsm_table::{CachedEpochMetadata, PartitionDurability, WriteFsmTable};
use restate_types::logs::Lsn;
use restate_types::message::MessageIndex;
use restate_types::partitions::PersistedFeatures;
use restate_types::schema::Schema;
use restate_types::{SemanticRestateVersion, Version};

use super::PartitionFeatures;

/// Read access to the partition FSM cache — the durable per-partition state-machine metadata
/// (applied LSN, schema, enabled features, epoch, minimum Restate version, ...).
pub trait HasFsm {
    /// Returns a read-only view of the FSM cache.
    fn fsm(&self) -> impl FsmAccess;
}

/// Mutating access to the FSM cache. Setters write through the supplied storage transaction and
/// update the in-memory cache; the changes are durable only once that transaction commits.
pub trait HasFsmMut: HasFsm {
    /// Returns a mutable view of the FSM cache.
    fn fsm_mut(&mut self) -> impl FsmMut;
}

/// Read-only view of the FSM cache.
pub trait FsmAccess {
    fn last_applied_lsn(&self) -> Lsn;
    fn schema(&self) -> Option<&Arc<Schema>>;
    fn schema_version(&self) -> Version;
    fn rule_book(&self) -> &Arc<RuleBook>;
    fn rule_book_version(&self) -> Version;
    fn durable_point(&self) -> Option<&PartitionDurability>;
    fn features(&self) -> impl PartitionFeatures;
    fn epoch_metadata(&self) -> Option<&CachedEpochMetadata>;
    fn epoch_metadata_version(&self) -> Version;
    fn min_restate_version(&self) -> &SemanticRestateVersion;
    // Legacy/Deprecated. Will be removed after vqueue's migration.
    fn inbox_seq_number(&self) -> MessageIndex;
}

/// Mutable view of the FSM cache. Each setter writes through the given [`WriteFsmTable`]
/// transaction as well as the in-memory cache; setters returning `bool` report whether the value
/// actually changed.
pub trait FsmMut: FsmAccess {
    fn set_min_restate_version<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        new_version: SemanticRestateVersion,
    ) -> bool;

    fn set_epoch_metadata<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        metadata: CachedEpochMetadata,
    ) -> bool;

    fn set_durable_point<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        durable_point: PartitionDurability,
    );

    fn set_schema<S: WriteFsmTable>(&mut self, txn: &mut S, schema: Arc<Schema>);
    fn set_rule_book<S: WriteFsmTable>(&mut self, txn: &mut S, rule_book: Arc<RuleBook>);

    fn set_enabled_features<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        updated: PersistedFeatures,
    ) -> bool;

    // Legacy/Deprecated. Will be removed after vqueue's migration.
    fn set_inbox_seq_number<S: WriteFsmTable>(&mut self, txn: &mut S, seq: MessageIndex);
}

// -- Boilerplate --
impl<P: HasFsm> HasFsm for &P {
    #[inline]
    fn fsm(&self) -> impl FsmAccess {
        (**self).fsm()
    }
}

impl<P: HasFsm> HasFsm for &mut P {
    #[inline]
    fn fsm(&self) -> impl FsmAccess {
        (**self).fsm()
    }
}

impl<P: HasFsmMut> HasFsmMut for &mut P {
    #[inline]
    fn fsm_mut(&mut self) -> impl FsmMut {
        (**self).fsm_mut()
    }
}

impl<T: FsmMut> FsmMut for &mut T {
    fn set_min_restate_version<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        new_version: SemanticRestateVersion,
    ) -> bool {
        (**self).set_min_restate_version(txn, new_version)
    }

    fn set_epoch_metadata<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        metadata: CachedEpochMetadata,
    ) -> bool {
        (**self).set_epoch_metadata(txn, metadata)
    }

    fn set_durable_point<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        durable_point: PartitionDurability,
    ) {
        (**self).set_durable_point(txn, durable_point)
    }

    fn set_schema<S: WriteFsmTable>(&mut self, txn: &mut S, schema: Arc<Schema>) {
        (**self).set_schema(txn, schema)
    }

    fn set_rule_book<S: WriteFsmTable>(&mut self, txn: &mut S, rule_book: Arc<RuleBook>) {
        (**self).set_rule_book(txn, rule_book)
    }

    fn set_enabled_features<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        updated: PersistedFeatures,
    ) -> bool {
        (**self).set_enabled_features(txn, updated)
    }

    fn set_inbox_seq_number<S: WriteFsmTable>(&mut self, txn: &mut S, seq: MessageIndex) {
        (**self).set_inbox_seq_number(txn, seq)
    }
}

impl<T: FsmAccess> FsmAccess for &T {
    #[inline]
    fn last_applied_lsn(&self) -> Lsn {
        (**self).last_applied_lsn()
    }
    #[inline]
    fn schema(&self) -> Option<&Arc<Schema>> {
        (**self).schema()
    }
    #[inline]
    fn schema_version(&self) -> Version {
        (**self).schema_version()
    }
    #[inline]
    fn rule_book(&self) -> &Arc<RuleBook> {
        (**self).rule_book()
    }
    #[inline]
    fn rule_book_version(&self) -> Version {
        (**self).rule_book_version()
    }
    #[inline]
    fn durable_point(&self) -> Option<&PartitionDurability> {
        (**self).durable_point()
    }
    #[inline]
    fn features(&self) -> impl PartitionFeatures {
        (**self).features()
    }
    #[inline]
    fn epoch_metadata(&self) -> Option<&CachedEpochMetadata> {
        (**self).epoch_metadata()
    }
    #[inline]
    fn epoch_metadata_version(&self) -> Version {
        (**self).epoch_metadata_version()
    }
    #[inline]
    fn min_restate_version(&self) -> &SemanticRestateVersion {
        (**self).min_restate_version()
    }
    #[inline]
    fn inbox_seq_number(&self) -> MessageIndex {
        (**self).inbox_seq_number()
    }
}

impl<T: FsmAccess> FsmAccess for &mut T {
    #[inline]
    fn last_applied_lsn(&self) -> Lsn {
        (**self).last_applied_lsn()
    }
    #[inline]
    fn schema(&self) -> Option<&Arc<Schema>> {
        (**self).schema()
    }
    #[inline]
    fn schema_version(&self) -> Version {
        (**self).schema_version()
    }
    #[inline]
    fn rule_book(&self) -> &Arc<RuleBook> {
        (**self).rule_book()
    }
    #[inline]
    fn rule_book_version(&self) -> Version {
        (**self).rule_book_version()
    }
    #[inline]
    fn durable_point(&self) -> Option<&PartitionDurability> {
        (**self).durable_point()
    }
    #[inline]
    fn features(&self) -> impl PartitionFeatures {
        (**self).features()
    }
    #[inline]
    fn epoch_metadata(&self) -> Option<&CachedEpochMetadata> {
        (**self).epoch_metadata()
    }
    #[inline]
    fn epoch_metadata_version(&self) -> Version {
        (**self).epoch_metadata_version()
    }
    #[inline]
    fn min_restate_version(&self) -> &SemanticRestateVersion {
        (**self).min_restate_version()
    }
    #[inline]
    fn inbox_seq_number(&self) -> MessageIndex {
        (**self).inbox_seq_number()
    }
}
