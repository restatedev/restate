// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::partitions::PersistedFeatures;

/// Read-only view of the state-machine features currently enabled for a partition.
///
/// Each feature is gated either on the partition's persisted minimum Restate-server version
/// (`min_restate_version`) or on its persisted opt-in feature set
/// ([`PersistedFeatures`]), depending on what the feature requires. See each method's
/// doc-comment for the specific gate.
pub trait PartitionFeatures {
    /// Write to journal v2 instead of journal v1 by default. This is a preparational step for
    /// removing the journal v1 after enabling this feature and migrating all unpinned invocations
    /// from journal v1 to journal v2.
    fn use_journal_v2_as_default(&self) -> bool;

    /// Whether vqueue-related code paths are active on this partition.
    ///
    /// *Since v1.7.0*
    fn is_vqueues_enabled(&self) -> bool;

    /// Whether new invocations should persist a unique random seed
    /// (invocation_id + record_created_at entropy). When off, SDKs derive the
    /// seed from `InvocationId::to_random_seed()` at invoke time.
    ///
    /// *Since v1.7.0*
    fn is_unique_random_seeds_enabled(&self) -> bool;

    /// Whether child invocations created via ctx.call inherit their caller's
    /// scope when the child target carries none.
    ///
    /// *Since v1.7.0*
    fn is_scope_inheritance_enabled(&self) -> bool;
}

impl PartitionFeatures for PersistedFeatures {
    #[inline]
    fn use_journal_v2_as_default(&self) -> bool {
        self.journal_v2
    }

    #[inline]
    fn is_vqueues_enabled(&self) -> bool {
        self.vqueues
    }

    #[inline]
    fn is_unique_random_seeds_enabled(&self) -> bool {
        self.unique_random_seeds
    }

    #[inline]
    fn is_scope_inheritance_enabled(&self) -> bool {
        self.scope_inheritance
    }
}

// -- Boilerplate --

impl<T: PartitionFeatures> PartitionFeatures for &T {
    fn use_journal_v2_as_default(&self) -> bool {
        (**self).use_journal_v2_as_default()
    }

    fn is_vqueues_enabled(&self) -> bool {
        (**self).is_vqueues_enabled()
    }

    fn is_unique_random_seeds_enabled(&self) -> bool {
        (**self).is_unique_random_seeds_enabled()
    }

    fn is_scope_inheritance_enabled(&self) -> bool {
        (**self).is_scope_inheritance_enabled()
    }
}

impl<T: PartitionFeatures> PartitionFeatures for &mut T {
    fn use_journal_v2_as_default(&self) -> bool {
        (**self).use_journal_v2_as_default()
    }

    fn is_vqueues_enabled(&self) -> bool {
        (**self).is_vqueues_enabled()
    }

    fn is_unique_random_seeds_enabled(&self) -> bool {
        (**self).is_unique_random_seeds_enabled()
    }

    fn is_scope_inheritance_enabled(&self) -> bool {
        (**self).is_scope_inheritance_enabled()
    }
}
