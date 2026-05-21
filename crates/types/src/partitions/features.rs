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

use crate::storage::{
    StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError, decode,
    encode,
};
use crate::{RESTATE_VERSION_1_7_0, SemanticRestateVersion};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::FromRepr)]
#[repr(u16)]
pub enum PartitionFeatureChange {
    /// Enable vqueues for the partition.
    ///
    /// *Since v1.7.0*
    EnableVqueues = 1,
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
            Self::EnableVqueues => &RESTATE_VERSION_1_7_0,
        }
    }

    /// Apply this change to the persisted feature set. Returns true if the feature
    /// change had an effect on the state machine features.
    pub fn apply_to(self, features: &mut PersistedStateMachineFeatures) -> bool {
        match self {
            Self::EnableVqueues => {
                let before = features.vqueues;
                features.vqueues = true;

                // we enable the feature if it was disabled before
                !before
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
/// *Since v1.7.0*
#[derive(
    Debug, Clone, Default, PartialEq, Eq, bilrost::Message, serde::Serialize, serde::Deserialize,
)]
pub struct PersistedStateMachineFeatures {
    /// Virtual queues are enabled on this partition.
    ///
    /// *Since v1.7.0*
    #[bilrost(tag(1))]
    pub vqueues: bool,
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
        PartitionFeatureChange::EnableVqueues.apply_to(&mut features);
        assert!(features.vqueues);
    }
}
