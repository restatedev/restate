// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod scoped_promise_migration;
mod scoped_state_migration;
mod state_promise_migration_combined;

use std::collections::BTreeMap;

use anyhow::Context;
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use strum::VariantArray as _;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use restate_rocksdb::{IoMode, Priority};
use restate_storage_api::StorageError;
use restate_types::SemanticRestateVersion;
use restate_types::config::Configuration;
use restate_types::partitions::StorageVersion;
use restate_util_string::ReString;
use restate_util_time::DurationExt;

use crate::fsm_table::{
    append_min_restate_version_to_wb, append_storage_features_to_wb, append_storage_version_to_wb,
};
use crate::{MigrationError, PartitionStore};

/// An incompatible persisted storage-feature set.
#[derive(Debug, thiserror::Error)]
#[error(
    "partition-store requires Restate version {required_min_version} or newer due to enabled storage features {features:?}"
)]
pub(crate) struct StorageFeatureVersionBarrier {
    pub(crate) required_min_version: SemanticRestateVersion,
    pub(crate) features: Vec<ReString>,
}

// Local storage features enabled for a partition store.
//
// These features describe local physical storage and must not leak outside this crate.
//
// Local partition store features are not coordinated between leader and followers, these features
// may enable data layout changes, format upgrades, or may indicate the enablement of certain
// lookup indexes. The presence of a feature _should not_ change the public API of the storage
// layer but it may impact the efficiency of performing certain operations.
//
// Look at FSM's `PersistedFeatures` for cluster-coordinated features that may impact
// the system behaviour in a way that can cause divergence between state machines' logical
// states.
//
//
// NOTE: When scanning for which features we should enable, we check each feature in the same
// order as defined in this macro.
//
// *Since v1.7.9*
storage_features! {
    /// A meta feature to support compatibility with v1.7.x. This feature
    /// implies that the two other features are also enabled.
    MigratedToScopedPromiseAndStateTables,
    /// If enabled, state `KeyKind::State` data tables are guaranteed to be empty.
    /// In its presence, `KeyKind::ScopedState` will hold both scoped
    /// and unscoped values and there is no need to ever read the old key kinds.
    ///
    /// In short:
    /// * unscoped `state_table` -> scoped `state_table` (with `scope = None`)
    pub MigratedToScopedStateTable,
    /// If enabled, promise `KeyKind::Promise` data tables are guaranteed to be empty.
    /// In its presence, `KeyKind::ScopedPromise` will hold both scoped
    /// and unscoped values and there is no need to ever read the old key kinds.
    ///
    /// In short:
    /// * unscoped `promise_table` -> scoped `promise_table` (with `scope = None`)
    pub MigratedToScopedPromiseTable,
}

trait StorageFeature: Sized + 'static {
    fn persisted_name() -> &'static ReString;
    fn min_required_version() -> &'static SemanticRestateVersion;
    fn should_enable(
        config: &Configuration,
        _current_version: &SemanticRestateVersion,
        is_store_empty: bool,
    ) -> bool;
    fn is_enabled(features: &StorageFeatures) -> bool;
    fn set_enabled(features: &mut StorageFeatures);

    /// Prepares the new representation and appends feature-specific cutover operations to
    /// `finalization`. The caller commits them atomically with the updated feature ledger.
    ///
    /// Implementors of this function must ensure that it's crash-proof by either batching
    /// all changes into a single write batch or by performing idempotent operations.
    ///
    /// Note that `enable` will not be called if the partition-store is empty
    /// (no LSNs have been applied).
    fn enable(
        storage: &mut PartitionStore,
        cancel: &CancellationToken,
        config: &Configuration,
        finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError>;
}

/// Loaded feature state used while planning and applying storage migrations.
#[derive(Debug, Default)]
pub(crate) struct LoadedStorageFeatures {
    enabled: StorageFeatures,
    persisted: PersistedEnabledFeatures,
    dirty: bool,
}

impl LoadedStorageFeatures {
    pub(crate) fn load(
        persisted: Option<PersistedEnabledFeatures>,
        storage_version: StorageVersion,
        binary_version: &SemanticRestateVersion,
    ) -> Result<Self, StorageFeatureVersionBarrier> {
        let dirty = persisted.is_none();
        let persisted = persisted.unwrap_or_default();

        // Compatibility must be checked before resolving feature names. This lets older binaries
        // report future features without having to understand their behavior.
        persisted.verify_compatible(binary_version)?;

        let mut enabled = StorageFeatures::default();

        for feature in KnownStorageFeature::VARIANTS {
            if persisted.features.contains_key(feature.persisted_name()) {
                feature.mark_as_enabled(&mut enabled);
            }
        }

        let mut loaded = Self {
            enabled,
            persisted,
            dirty,
        };

        // If the feature for this migration wasn't already recorded, we record it here.
        //
        // Key 5 (STORAGE_VERSION) remains the compatibility source for stores created before storage features were
        // introduced. Keep the JSON ledger of features synchronized.
        if storage_version.is_scope_migrated() {
            // This marks the loaded features as dirty if it wasn't recorded before.
            loaded.set_enabled::<MigratedToScopedPromiseAndStateTablesFeature>(None);
        }

        Ok(loaded)
    }

    fn is_enabled<F: StorageFeature>(&self) -> bool {
        F::is_enabled(&self.enabled)
    }

    /// Marks the feature as enabled without performing any actual migration or data movement.
    fn set_enabled<F: StorageFeature>(
        &mut self,
        enabled_by: Option<&SemanticRestateVersion>,
    ) -> bool {
        if self.is_enabled::<F>() {
            return false;
        }

        F::set_enabled(&mut self.enabled);
        // Update persisted ledger using F's metadata.
        if !self.persisted.features.contains_key(F::persisted_name()) {
            self.persisted.features.insert(
                F::persisted_name().to_owned(),
                FeatureBarrier {
                    min_required_version: F::min_required_version().clone(),
                    enabled_by_version: enabled_by.cloned(),
                    unknown: BTreeMap::new(),
                },
            );
            self.dirty = true;
        }
        true
    }

    pub(crate) fn enabled(&self) -> &StorageFeatures {
        &self.enabled
    }

    pub(crate) fn ledger(&self) -> &PersistedEnabledFeatures {
        &self.persisted
    }

    pub(crate) fn mark_persisted(&mut self) {
        self.dirty = false;
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn automatic_changes(
        &self,
        config: &Configuration,
        current_version: &SemanticRestateVersion,
        is_store_empty: bool,
    ) -> Vec<KnownStorageFeature> {
        KnownStorageFeature::VARIANTS
            .iter()
            .filter(|feature| !feature.is_enabled(self.enabled()))
            .filter(|feature| {
                current_version.is_equal_or_newer_than(feature.min_required_version())
                    && feature.should_enable(config, current_version, is_store_empty)
            })
            .copied()
            .collect()
    }
}

/// Data model stored as JSON under `fsm_variable::STORAGE_FEATURES`.
///
/// Allows loading features that we can't recognize but will still let us gate opening
/// the partition-store if the minimum required version isn't met.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PersistedEnabledFeatures {
    #[serde(default)]
    features: BTreeMap<ReString, FeatureBarrier>,
    // The unknown other attributes that we need to re-serialize for lossless read-modify-write
    // operations.
    #[serde(flatten)]
    unknown: BTreeMap<ReString, serde_json::Value>,
}

impl PersistedEnabledFeatures {
    fn verify_compatible(
        &self,
        current_version: &SemanticRestateVersion,
    ) -> Result<(), StorageFeatureVersionBarrier> {
        let mut required_min_version: Option<&SemanticRestateVersion> = None;
        let mut features = Vec::with_capacity(self.features.len());

        for (name, barrier) in &self.features {
            match required_min_version {
                None => {
                    required_min_version = Some(&barrier.min_required_version);
                    features.push(name.clone());
                }
                Some(required) => match barrier.min_required_version.cmp_precedence(required) {
                    std::cmp::Ordering::Greater => {
                        // raises the minimum version floor up
                        required_min_version = Some(&barrier.min_required_version);
                        features.clear();
                        features.push(name.clone());
                    }
                    std::cmp::Ordering::Equal => features.push(name.clone()),
                    std::cmp::Ordering::Less => {}
                },
            }
        }

        let Some(required_min_version) = required_min_version else {
            return Ok(());
        };

        if current_version.is_equal_or_newer_than(required_min_version) {
            return Ok(());
        }

        Err(StorageFeatureVersionBarrier {
            required_min_version: required_min_version.clone(),
            features,
        })
    }

    /// Calculates the restate server version floor that is required for the currently enabled
    /// features.
    pub fn get_required_min_version(&self) -> Option<SemanticRestateVersion> {
        self.features
            .values()
            .map(|v| &v.min_required_version)
            .max()
            .cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FeatureBarrier {
    min_required_version: SemanticRestateVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    enabled_by_version: Option<SemanticRestateVersion>,
    // The unknown other attributes that we need to re-serialize for lossless read-modify-write
    // operations.
    #[serde(flatten)]
    unknown: BTreeMap<ReString, serde_json::Value>,
}

async fn enable_helper<F: StorageFeature>(
    storage: &mut PartitionStore,
    current_version: &SemanticRestateVersion,
    current_min_restate_version: &mut SemanticRestateVersion,
    current_storage_version: &mut StorageVersion,
    is_store_empty: bool,
    cancel: &CancellationToken,
    config: &Configuration,
    features: &mut LoadedStorageFeatures,
) -> Result<(), MigrationError> {
    if features.is_enabled::<F>() {
        return Ok(());
    }

    let start = Instant::now();
    debug!(
        "Enabling partition-store storage feature '{}'",
        F::persisted_name()
    );

    let partition_db = storage.partition_db().clone();
    let cf_handle = partition_db.cf_handle().clone();
    let partition_id = storage.partition_id();
    let mut finalization = WriteBatch::default();

    if !is_store_empty {
        F::enable(storage, cancel, config, &mut finalization)?;
    }

    if cancel.is_cancelled() {
        return Err(MigrationError::MigrationCancelled);
    }

    features.set_enabled::<F>(Some(current_version));
    append_storage_features_to_wb(
        &cf_handle,
        &mut finalization,
        partition_id,
        features.ledger(),
    )?;

    // Bump up the min restate version
    if F::min_required_version().is_newer_than(current_min_restate_version) {
        append_min_restate_version_to_wb(
            &cf_handle,
            &mut finalization,
            partition_id,
            F::min_required_version().clone(),
        )?;
        *current_min_restate_version = F::min_required_version().clone();
    }

    // StorageVersion is the compatibility signal understood by v1.7.x binaries. Its
    // ScopedStateAndPromise variant is equivalent to the feature ledger only when both table
    // migrations are enabled, so write it atomically when this feature completes the pair.
    //
    // This can be removed when min_restate_version is v1.8 since v1.8 depends on StorageFeatures only.
    // Estimated to be in v1.9
    if MigratedToScopedPromiseAndStateTablesFeature::is_enabled(features.enabled())
        && *current_storage_version < StorageVersion::ScopedStateAndPromise
    {
        append_storage_version_to_wb(
            &cf_handle,
            &mut finalization,
            partition_id,
            StorageVersion::ScopedStateAndPromise,
        )?;
        *current_storage_version = StorageVersion::ScopedStateAndPromise;
    }

    // The cutover marker cannot be persisted without its feature-specific finalization. FIFO
    // memtable flush order also keeps this batch behind the preceding copy batches.
    let mut opts = rocksdb::WriteOptions::default();
    opts.disable_wal(true);
    partition_db
        .rocksdb()
        .write_batch(
            "storage-feature-migration-finalization",
            Priority::High,
            IoMode::Default,
            opts,
            finalization,
        )
        .await
        .context("failed to commit storage feature migration finalization")
        .map_err(StorageError::Generic)?;

    features.mark_persisted();

    if !is_store_empty {
        info!(
            "partition-store storage feature '{}' has been enabled in {}",
            F::persisted_name(),
            start.elapsed().friendly()
        );
    }

    Ok(())
}

/// Defines a local storage (PartitionStore) feature from a list of feature names.
macro_rules! storage_features {
    (
        $(
            $(#[$attrs:meta])*
            $visibility:vis $feature:ident
        ),* $(,)?
    ) => {
        ::paste::paste! {
            /// Local storage features enabled for a partition store.
            ///
            /// These features describe local physical storage and must not leak outside this crate.
            ///
            /// Local partition store features are not coordinated between leader and followers, these features
            /// may enable data layout changes, format upgrades, or may indicate the enablement of certain
            /// lookup indexes. The presence of a feature _should not_ change the public API of the storage
            /// layer but it may impact the efficiency of performing certain operations.
            ///
            /// Look at FSM's `PersistedFeatures` for cluster-coordinated features that may impact
            /// the system behaviour in a way that can cause divergence between state machines' logical
            /// states.
            ///
            /// *Since v1.7.9*
            #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
            pub(crate) struct StorageFeatures {
                $(
                    $(#[$attrs])*
                    $visibility [<is_ $feature:snake>]: bool,
                )*
            }

            // Per-feature no-value type.
            $(
                $(#[$attrs])*
                $visibility enum [<$feature:camel Feature>] {}
            )*


            impl StorageFeatures {
                /// Returns the list of enabled features by name
                pub fn into_names(self) -> Vec<ReString> {
                    [
                        $(
                            self.[<is_ $feature:snake>]
                            .then_some(
                                <[< $feature:camel Feature >] as
                                StorageFeature>::persisted_name().clone()
                            ),
                        )*
                    ]
                    .into_iter()
                    .flatten()
                    .collect()
                }

            }
            /// Local storage features enabled for a partition store.
            #[derive(strum::VariantArray, Clone, Copy, PartialEq, Eq)]
            pub(crate) enum KnownStorageFeature {
                $(
                    $(#[$attrs])*
                    [<$feature:camel>],
                )*
            }

            impl KnownStorageFeature {
                pub fn persisted_name(self) -> &'static ReString {
                    match self {
                        $(
                            Self::[<$feature:camel>] =>
                                <[<$feature:camel Feature>] as StorageFeature>::persisted_name(),
                        )*
                    }
                }

                /// The minimum Restate version that can work with this feature when enabled.
                fn min_required_version(self) -> &'static SemanticRestateVersion {
                    match self {
                        $(
                            Self::[<$feature:camel>] =>
                                <[<$feature:camel Feature>] as StorageFeature>::min_required_version(),
                        )*
                    }
                }

                /// Returns true if this feature is eligible to be enabled.
                fn should_enable(
                    self,
                    config: &Configuration,
                    current_version: &SemanticRestateVersion,
                    is_store_empty: bool,
                ) -> bool {
                    match self {
                        $(
                            Self::[<$feature:camel>] =>
                                <[<$feature:camel Feature>] as StorageFeature>::should_enable(
                                    config,
                                    current_version,
                                    is_store_empty,
                                ),
                        )*
                    }
                }

                /// Mutates [`StorageFeatures`] to reflect the enablement of this feature
                fn mark_as_enabled(self, features: &mut StorageFeatures) {
                    match self {
                        $(
                            Self::[<$feature:camel>] =>
                                <[<$feature:camel Feature>] as StorageFeature>::set_enabled(features),
                        )*
                    }
                }

                /// Is this feature considered to be enabled in the set of `features`?
                fn is_enabled(self, features: &StorageFeatures) -> bool {
                    match self {
                        $(
                            Self::[<$feature:camel>] =>
                                <[<$feature:camel Feature>] as StorageFeature>::is_enabled(features),
                        )*
                    }
                }

                /// Performs the necessary data migration if needed, then marks the feature as
                /// enabled in the input set of `features`
                pub async fn enable(
                    self,
                    storage: &mut PartitionStore,
                    current_version: &SemanticRestateVersion,
                    min_restate_version: &mut SemanticRestateVersion,
                    current_storage_version: &mut StorageVersion,
                    is_store_empty: bool,
                    cancel: &CancellationToken,
                    config: &Configuration,
                    features: &mut LoadedStorageFeatures,
                ) -> Result<(), MigrationError> {
                    match self {
                        $(
                            Self::[<$feature:camel>] => enable_helper::<[<$feature:camel Feature>]>(
                                storage,
                                current_version,
                                min_restate_version,
                                current_storage_version,
                                is_store_empty,
                                cancel,
                                config,
                                features,
                            )
                            .await,
                        )*
                    }
                }
            }

            impl std::fmt::Display for KnownStorageFeature {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str(self.persisted_name())
                }
            }

            impl std::fmt::Debug for KnownStorageFeature {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str(self.persisted_name())
                }
            }
        }
    };
}

use storage_features;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_features_raise_the_barrier_and_survive_updates() {
        let persisted: PersistedEnabledFeatures = serde_json::from_str(
            r#"{
                "features": {
                    "future-a": {
                        "min_required_version": "2.0.0",
                        "enabled_by_version": "2.1.0",
                        "future-metadata": { "value": 1 }
                    },
                    "future-b": {
                        "min_required_version": "2.0.0"
                    },
                    "older-feature": {
                        "min_required_version": "1.9.0"
                    }
                },
                "future-document-field": { "value": 2 }
            }"#,
        )
        .unwrap();

        let error = LoadedStorageFeatures::load(
            Some(persisted.clone()),
            StorageVersion::None,
            &SemanticRestateVersion::new(1, 9, 0),
        )
        .unwrap_err();
        assert_eq!(
            error.required_min_version,
            SemanticRestateVersion::new(2, 0, 0)
        );
        assert_eq!(error.features, ["future-a", "future-b"]);

        let mut loaded = LoadedStorageFeatures::load(
            Some(persisted),
            StorageVersion::None,
            &SemanticRestateVersion::new(2, 0, 0),
        )
        .unwrap();
        assert!(!loaded.enabled().is_migrated_to_scoped_state_table);
        assert!(!loaded.enabled().is_migrated_to_scoped_promise_table);
        loaded.set_enabled::<MigratedToScopedStateTableFeature>(Some(
            &SemanticRestateVersion::new(2, 0, 0),
        ));
        loaded.set_enabled::<MigratedToScopedPromiseTableFeature>(Some(
            &SemanticRestateVersion::new(2, 0, 0),
        ));

        let encoded = serde_json::to_value(loaded.ledger()).unwrap();
        assert_eq!(
            encoded["features"]["future-a"]["enabled_by_version"],
            "2.1.0"
        );
        assert_eq!(
            encoded["features"]["future-a"]["future-metadata"]["value"],
            1
        );
        assert_eq!(encoded["future-document-field"]["value"], 2);
        assert!(encoded["features"]["future-b"].is_object());
        assert!(encoded["features"]["older-feature"].is_object());
        assert!(encoded["features"]["scoped-state-table"].is_object());
        assert!(encoded["features"]["scoped-promises-table"].is_object());
    }

    #[test]
    fn scoped_tables_are_only_automatically_enabled_for_empty_stores() {
        let current_version = SemanticRestateVersion::new(2, 0, 0);
        let features =
            LoadedStorageFeatures::load(None, StorageVersion::V1_5, &current_version).unwrap();
        let config = Configuration::default();

        assert!(
            features
                .automatic_changes(&config, &current_version, false)
                .is_empty()
        );
        assert_eq!(
            features.automatic_changes(&config, &current_version, true),
            [
                KnownStorageFeature::MigratedToScopedStateTable,
                KnownStorageFeature::MigratedToScopedPromiseTable,
            ]
        );
    }
}
