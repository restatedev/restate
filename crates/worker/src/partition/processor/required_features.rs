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

use metrics::gauge;

use restate_types::partitions::{PartitionFeatureChange, PersistedFeatures};
use restate_types::sharding::PartitionId;
use restate_wal_protocol::v2::CommandKind;
use restate_worker_api::processor::PartitionFeatures;

use crate::metric_definitions::{
    FLARE_REASON_MISSING_REQUIRED_FEATURES, PARTITION_BLOCKED_FLARE, PARTITION_LABEL, REASON_LABEL,
};
use crate::partition::ProcessorError;

/// The features in `required` that `features` lacks, in the order of `required`.
fn missing_features(
    features: &PersistedFeatures,
    required: &[PartitionFeatureChange],
) -> impl Iterator<Item = PartitionFeatureChange> {
    required
        .iter()
        .copied()
        .filter(|feature| !features.has_feature(*feature))
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

/// Checks a partition's features before the processor applies a command.
///
/// While a required feature is missing, only `AnnounceLeader` and `VersionBarrier` apply: the
/// barrier that enables the feature has to come before any other command, since those may need
/// the logic that the feature retires. A required feature stays enabled once it is, so the check
/// is done as soon as one command has found all of them.
///
/// A partition that passes the check is no longer blocked on a required feature, which clears the
/// flare that an earlier attempt to run it may have raised.
#[derive(Debug)]
pub struct CommandCheck {
    passed: bool,
    partition_id: PartitionId,
}

impl CommandCheck {
    pub fn new(
        partition_id: PartitionId,
        features: &PersistedFeatures,
        required: &[PartitionFeatureChange],
    ) -> Self {
        let passed = missing_features(features, required).next().is_none();
        // Nothing raises the flare if nothing is required.
        if passed && !required.is_empty() {
            clear_blocked_flare(partition_id);
        }
        Self {
            passed,
            partition_id,
        }
    }

    /// Checks the partition's features before the processor applies a command of `kind`.
    #[inline]
    pub fn check(
        &mut self,
        kind: CommandKind,
        features: &PersistedFeatures,
        required: &[PartitionFeatureChange],
    ) -> Result<(), ProcessorError> {
        if self.passed
            || matches!(
                kind,
                CommandKind::AnnounceLeader | CommandKind::VersionBarrier
            )
        {
            return Ok(());
        }
        check(features, required)?;
        self.passed = true;
        clear_blocked_flare(self.partition_id);
        Ok(())
    }
}

fn clear_blocked_flare(partition_id: PartitionId) {
    gauge!(
        PARTITION_BLOCKED_FLARE,
        PARTITION_LABEL => partition_id.to_string(),
        REASON_LABEL => FLARE_REASON_MISSING_REQUIRED_FEATURES
    )
    .set(0);
}

fn check(
    features: &PersistedFeatures,
    required: &[PartitionFeatureChange],
) -> Result<(), ProcessorError> {
    let mut missing = missing_features(features, required).peekable();
    if missing.peek().is_none() {
        return Ok(());
    }
    Err(ProcessorError::MissingRequiredFeatures {
        missing: missing.collect(),
    })
}

#[cfg(test)]
mod tests {
    use restate_types::partitions::{PartitionFeatureChange, PersistedFeatures};
    use restate_types::sharding::PartitionId;
    use restate_wal_protocol::v2::CommandKind;

    use super::{CommandCheck, check_on_startup};
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

    #[test]
    fn only_control_commands_apply_while_a_required_feature_is_missing() {
        let required = [PartitionFeatureChange::EnableByteOffsets];
        let mut features = PersistedFeatures::default();
        let mut check = CommandCheck::new(PartitionId::MIN, &features, &required);

        for kind in [CommandKind::AnnounceLeader, CommandKind::VersionBarrier] {
            assert!(check.check(kind, &features, &required).is_ok());
        }
        assert!(matches!(
            check.check(CommandKind::Invoke, &features, &required),
            Err(ProcessorError::MissingRequiredFeatures { missing })
                if missing == required
        ));

        // Once a barrier has enabled the feature, commands apply and are no longer checked.
        PartitionFeatureChange::EnableByteOffsets.apply_to(&mut features);
        assert!(
            check
                .check(CommandKind::Invoke, &features, &required)
                .is_ok()
        );
        assert!(check.passed);

        // Nothing to check if nothing is required, or if the partition starts with the features.
        assert!(CommandCheck::new(PartitionId::MIN, &PersistedFeatures::default(), &[]).passed);
        assert!(CommandCheck::new(PartitionId::MIN, &features, &required).passed);
    }
}
