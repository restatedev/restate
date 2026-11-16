// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Checks that a partition has the features this server requires
//! ([`REQUIRED_PARTITION_FEATURES`](restate_types::partitions::REQUIRED_PARTITION_FEATURES)).

use restate_types::partitions::{PartitionFeatureChange, PersistedFeatures};
use restate_worker_api::processor::PartitionFeatures;

use crate::partition::ProcessorError;

/// The features in `required` that `features` lacks, in the order of `required`.
pub fn missing_features(
    features: &PersistedFeatures,
    required: &[PartitionFeatureChange],
) -> Vec<PartitionFeatureChange> {
    required
        .iter()
        .copied()
        .filter(|feature| !features.has_feature(*feature))
        .collect()
}

/// Checks a partition's features before the processor reads its log.
///
/// A partition stores features only once it applies a version barrier that enables one, so a
/// partition without any has applied no barrier yet. It may still enable the required features
/// with its first barrier; until then, the processor refuses every command other than
/// `AnnounceLeader` and `VersionBarrier`. A partition with features but without a required one
/// has applied a barrier without it, and may hold state this server can no longer process.
pub fn check_on_startup(
    features: &PersistedFeatures,
    required: &[PartitionFeatureChange],
) -> Result<(), ProcessorError> {
    if *features == PersistedFeatures::default() {
        return Ok(());
    }
    check(features, required)
}

fn check(
    features: &PersistedFeatures,
    required: &[PartitionFeatureChange],
) -> Result<(), ProcessorError> {
    let missing = missing_features(features, required);
    if missing.is_empty() {
        Ok(())
    } else {
        Err(ProcessorError::MissingRequiredFeatures { missing })
    }
}

#[cfg(test)]
mod tests {
    use restate_types::partitions::{PartitionFeatureChange, PersistedFeatures};

    use super::check_on_startup;
    use crate::partition::ProcessorError;

    #[test]
    fn startup_requires_features_once_a_barrier_was_applied() {
        let required = [
            PartitionFeatureChange::EnableJournalV2,
            PartitionFeatureChange::EnableByteOffsets,
        ];

        // No barrier applied yet: the first barrier may still enable the required features.
        assert!(check_on_startup(&PersistedFeatures::default(), &required).is_ok());

        // A barrier was applied without one of the required features.
        let mut features = PersistedFeatures::default();
        PartitionFeatureChange::EnableJournalV2.apply_to(&mut features);
        assert!(matches!(
            check_on_startup(&features, &required),
            Err(ProcessorError::MissingRequiredFeatures { missing })
                if missing == [PartitionFeatureChange::EnableByteOffsets]
        ));

        PartitionFeatureChange::EnableByteOffsets.apply_to(&mut features);
        assert!(check_on_startup(&features, &required).is_ok());

        // Nothing is required.
        assert!(check_on_startup(&features, &[]).is_ok());
    }
}
