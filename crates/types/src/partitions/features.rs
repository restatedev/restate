// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BytesMut;

use restate_encoding::NetSerde;

use crate::storage::{
    StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError, decode,
    encode,
};
use crate::{RESTATE_VERSION_1_6_0, RESTATE_VERSION_1_7_0, SemanticRestateVersion};

/// A change to the set of state-machine features enabled on a partition.
///
/// Each variant represents a (feature, direction) pair: enabling and disabling are
/// distinct IDs. One-way features (those that can only be enabled) simply do not
/// have a corresponding `Disable*` variant.
///
/// Variants are encoded on the wire as raw `u16` IDs; unknown IDs cause the
/// [`PartitionFeatureChange::from_repr`] lookup to return `None`, allowing
/// the apply path to surface a precise error rather than silently dropping
/// the change.
///
/// *Since v1.7.0*
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::FromRepr, strum::EnumString, strum::Display,
)]
#[strum(ascii_case_insensitive)]
#[repr(u16)]
pub enum PartitionFeatureChange {
    /// Enable journal v2 by default.
    ///
    /// *Since v1.7.0*
    EnableJournalV2 = 1,
    /// Enable vqueues for the partition.
    ///
    /// *Since v1.7.0*
    EnableVqueues = 2,
    /// Persist a unique random seed (invocation_id + record_created_at entropy) on new
    /// invocations so SDK RNG output is deterministic per invocation.
    ///
    /// *Since v1.7.0*
    EnableUniqueRandomSeeds = 3,
}

impl PartitionFeatureChange {
    /// The raw ID used on the wire.
    pub fn id(self) -> u16 {
        self as u16
    }

    /// The minimum Restate-server version required to interpret this change.
    /// Proposers must set `VersionBarrierCommand::version` to the max of these
    /// across all changes carried by the barrier.
    pub fn min_required_version(self) -> &'static SemanticRestateVersion {
        match self {
            Self::EnableJournalV2 => &RESTATE_VERSION_1_6_0,
            Self::EnableVqueues => &RESTATE_VERSION_1_7_0,
            Self::EnableUniqueRandomSeeds => &RESTATE_VERSION_1_7_0,
        }
    }

    /// Apply this change to the persisted feature set. Returns true if the feature
    /// change had an effect on the state machine features.
    pub fn apply_to(self, features: &mut PersistedStateMachineFeatures) -> bool {
        match self {
            Self::EnableJournalV2 => !std::mem::replace(&mut features.journal_v2, true),
            Self::EnableVqueues => !std::mem::replace(&mut features.vqueues, true),
            Self::EnableUniqueRandomSeeds => {
                !std::mem::replace(&mut features.unique_random_seeds, true)
            }
        }
    }
}

/// Persisted set of state-machine feature flags for a partition.
///
/// Each field is a boolean indicating whether the corresponding feature is
/// enabled. New features add new fields; bilrost's default-on-missing-field
/// behavior makes existing FSM data forward-compatible (new fields default to
/// `false`).
///
/// # Important
/// When adding new fields to this struct, update [`PersistedStateMachineFeatures::enabled_names`]
/// accordingly.
///
/// *Since v1.7.0*
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    bilrost::Message,
    NetSerde,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct PersistedStateMachineFeatures {
    /// Journal v2 should be used by this partition.
    ///
    /// *Since v1.7.0*
    #[bilrost(tag(1))]
    pub journal_v2: bool,
    /// Virtual queues are enabled on this partition.
    ///
    /// *Since v1.7.0*
    #[bilrost(tag(2))]
    pub vqueues: bool,
    /// Persist a unique random seed on new invocations.
    ///
    /// *Since v1.7.0*
    #[bilrost(tag(3))]
    pub unique_random_seeds: bool,
}

impl PersistedStateMachineFeatures {
    /// Names of features currently enabled, in declaration order.
    ///
    /// Adding a new feature requires adding one entry here.
    pub fn enabled_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        [
            self.journal_v2.then_some("journal_v2"),
            self.vqueues.then_some("vqueues"),
            self.unique_random_seeds.then_some("unique_random_seeds"),
        ]
        .into_iter()
        .flatten()
    }
}

impl StorageEncode for PersistedStateMachineFeatures {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        encode::encode_bilrost(self, buf)
    }

    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::Bilrost
    }
}

impl StorageDecode for PersistedStateMachineFeatures {
    fn decode<B: bytes::Buf>(
        buf: &mut B,
        kind: StorageCodecKind,
    ) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        assert_eq!(kind, StorageCodecKind::Bilrost);
        decode::decode_bilrost(buf)
    }
}

impl FromIterator<PartitionFeatureChange> for PersistedStateMachineFeatures {
    fn from_iter<I: IntoIterator<Item = PartitionFeatureChange>>(iter: I) -> Self {
        let mut features = Self::default();
        for change in iter {
            change.apply_to(&mut features);
        }
        features
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_repr_round_trip() {
        let change = PartitionFeatureChange::EnableVqueues;
        assert_eq!(PartitionFeatureChange::from_repr(change.id()), Some(change));
    }

    #[test]
    fn unknown_id_returns_none() {
        assert!(PartitionFeatureChange::from_repr(9999).is_none());
    }

    #[test]
    fn apply_to_sets_field() {
        let mut features = PersistedStateMachineFeatures::default();
        assert!(!features.vqueues);
        assert!(!features.unique_random_seeds);
        PartitionFeatureChange::EnableVqueues.apply_to(&mut features);
        PartitionFeatureChange::EnableUniqueRandomSeeds.apply_to(&mut features);
        assert!(features.vqueues);
        assert!(features.unique_random_seeds);
    }

    #[test]
    fn enabled_names_reflects_set_flags() {
        let mut features = PersistedStateMachineFeatures::default();
        assert_eq!(
            features.enabled_names().collect::<Vec<_>>(),
            Vec::<&str>::new()
        );

        features.vqueues = true;
        assert_eq!(
            features.enabled_names().collect::<Vec<_>>(),
            vec!["vqueues"]
        );

        features.journal_v2 = true;
        assert_eq!(
            features.enabled_names().collect::<Vec<_>>(),
            vec!["journal_v2", "vqueues"],
        );
    }

    #[test]
    fn from_str_round_trip_and_case_insensitive() {
        use std::str::FromStr;

        assert_eq!(
            PartitionFeatureChange::from_str("EnableVqueues").unwrap(),
            PartitionFeatureChange::EnableVqueues,
        );
        assert_eq!(
            PartitionFeatureChange::from_str("enablejournalv2").unwrap(),
            PartitionFeatureChange::EnableJournalV2,
        );
        assert!(PartitionFeatureChange::from_str("not-a-feature").is_err());
    }
}
