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
use strum::IntoEnumIterator;

use restate_encoding::NetSerde;

use crate::storage::{
    StorageCodecKind, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError, decode,
    encode,
};
use crate::{
    RESTATE_VERSION_1_6_0, RESTATE_VERSION_1_7_0, RESTATE_VERSION_1_7_5, RESTATE_VERSION_1_7_8,
    RESTATE_VERSION_1_8_0, RESTATE_VERSION_1_8_1, RESTATE_VERSION_1_8_2, SemanticRestateVersion,
};

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
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    strum::FromRepr,
    strum::EnumIter,
    strum::EnumString,
    strum::IntoStaticStr,
    strum::Display,
    strum::VariantArray,
)]
#[strum(ascii_case_insensitive)]
#[repr(u16)]
pub enum PartitionFeatureChange {
    /// Enable journal v2 by default.
    ///
    /// *Since v1.7.0*
    EnableJournalV2 = 1,
    /// Enable vqueues for the partition and migrate all invocations.
    ///
    /// *Since v1.7.0*
    EnableVqueues = 2,
    /// Persist a unique random seed (invocation_id + record_created_at entropy) on new
    /// invocations so SDK RNG output is deterministic per invocation.
    ///
    /// *Since v1.7.0*
    EnableUniqueRandomSeeds = 3,
    /// Enable vqueues for the partition and skip migrating completed invocations.
    ///
    /// *Since v1.7.5*
    EnableVqueuesSkipCompleted = 4,
    /// Apply completion and journal retention when terminating a preflight invocation.
    ///
    /// *Since v1.7.8*
    EnablePreflightInvocationTerminationRetention = 5,
    /// Remove all pending state mutations. State mutations enqueued before v1.8.0 could be
    /// stored under different ids on different replicas (#5416). Afterwards, all replicas store
    /// state mutations under the same ids.
    ///
    /// *Since v1.8.0*
    EnableInconsistentStateMutationCleanup = 6,
    /// Record signals sent to scheduled or inboxed invocations in their journal instead of
    /// discarding them.
    ///
    /// *Since v1.8.1*
    EnablePreflightSignalRecording = 7,
    /// Track how far the partition has applied its log and written its partition store in
    /// bytes, counting from the record that applies this change.
    ///
    /// *Since v1.8.2*
    EnableByteOffsets = 8,
    /// Record vqueue entries' lifecycle more precisely, from the record that applies this change
    /// on:
    /// - every run of an entry records `started`, so that pausing or suspending it carries its
    ///   latest attempt's status, which resuming or waking it up restores; before, a later run
    ///   kept the status the entry had in the inbox, such as `backing-off`;
    /// - an invocation's entry enters Finished at the invocation's completion time, never before
    ///   its last modification, instead of at the completing record's time.
    ///
    /// *Since v1.8.2*
    EnableVqLifecycleRefined = 9,
    /// The invocation status of a completed invocation refers to the blob of its Output journal
    /// entry instead of holding a copy of the result. Independent of the partition store's
    /// journal-entry-blobs storage feature, which decides where entry contents are stored.
    /// Proposed by the leader when `experimental-enable-write-blob-table` is set.
    ///
    /// *Since v1.8.2*
    EnableInvocationStatusOutputBlob = 10,
}

/// The partition features this server requires. Each one retires logic this server no longer
/// has, such as commands, command variants or state formats. A partition processor that lacks one
/// of them refuses to start once it has applied a version barrier, and refuses to apply any
/// command other than `AnnounceLeader` or `VersionBarrier` before then.
///
/// A release may only add a feature here once the previous release enables it on every
/// partition, so that a rolling upgrade or a rollback never runs a partition without it. Only
/// features that nothing disables again can be required.
///
/// *Since v1.8.2*
pub const REQUIRED_PARTITION_FEATURES: &[PartitionFeatureChange] = &[];

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
            Self::EnableVqueuesSkipCompleted => &RESTATE_VERSION_1_7_5,
            Self::EnablePreflightInvocationTerminationRetention => &RESTATE_VERSION_1_7_8,
            Self::EnableInconsistentStateMutationCleanup => &RESTATE_VERSION_1_8_0,
            Self::EnablePreflightSignalRecording => &RESTATE_VERSION_1_8_1,
            Self::EnableByteOffsets => &RESTATE_VERSION_1_8_2,
            Self::EnableVqLifecycleRefined => &RESTATE_VERSION_1_8_2,
            Self::EnableInvocationStatusOutputBlob => &RESTATE_VERSION_1_8_2,
        }
    }

    /// Whether a barrier may carry this change as soon as every replica is held to its
    /// [`Self::min_required_version`]: by the changes applied before, or by the barrier that
    /// carries it. Such a change only alters what replicas record from then on: it costs no
    /// rollback and needs no opt-in. It must ship in the release its minimum version names, so
    /// that every server held to that version knows it.
    pub fn rides_along(self) -> bool {
        match self {
            Self::EnableVqLifecycleRefined => true,
            // Each needs its own decision: a migration, or an effect beyond what is recorded.
            Self::EnableJournalV2
            | Self::EnableVqueues
            | Self::EnableUniqueRandomSeeds
            | Self::EnableVqueuesSkipCompleted
            | Self::EnablePreflightInvocationTerminationRetention
            | Self::EnableInconsistentStateMutationCleanup
            | Self::EnablePreflightSignalRecording
            | Self::EnableByteOffsets
            | Self::EnableInvocationStatusOutputBlob => false,
        }
    }

    /// The name of the feature that this change enables, as [`PersistedFeatures::enabled_names`]
    /// lists it.
    ///
    /// Snapshots record these names to tell which features their data has, so a name must never
    /// change.
    pub fn enabled_name(self) -> &'static str {
        match self {
            Self::EnableJournalV2 => "journal_v2",
            Self::EnableVqueues => "vqueues",
            Self::EnableUniqueRandomSeeds => "unique_random_seeds",
            Self::EnableVqueuesSkipCompleted => "vqueues_skip_completed",
            Self::EnablePreflightInvocationTerminationRetention => {
                "preflight_invocation_termination_retention"
            }
            Self::EnableInconsistentStateMutationCleanup => "inconsistent_state_mutation_cleanup",
            Self::EnablePreflightSignalRecording => "preflight_signal_recording",
            Self::EnableByteOffsets => "byte_offsets",
            Self::EnableVqLifecycleRefined => "vq_lifecycle_refined",
            Self::EnableInvocationStatusOutputBlob => "invocation_status_output_blob",
        }
    }

    /// Apply this change to the persisted feature set. Returns true if the feature
    /// change had an effect on the state machine features.
    pub fn apply_to(self, features: &mut PersistedFeatures) -> bool {
        match self {
            Self::EnableJournalV2 => !std::mem::replace(&mut features.journal_v2, true),
            Self::EnableVqueues => {
                // Setting vqueues to true is a superset of vqueues_skip_completed, so unsetting it
                // to make it less confusing.
                features.vqueues_skip_completed = false;
                !std::mem::replace(&mut features.vqueues, true)
            }
            Self::EnableVqueuesSkipCompleted => {
                !std::mem::replace(&mut features.vqueues_skip_completed, true)
            }
            Self::EnableUniqueRandomSeeds => {
                !std::mem::replace(&mut features.unique_random_seeds, true)
            }
            Self::EnablePreflightInvocationTerminationRetention => !std::mem::replace(
                &mut features.preflight_invocation_termination_retention,
                true,
            ),
            Self::EnableInconsistentStateMutationCleanup => {
                !std::mem::replace(&mut features.inconsistent_state_mutation_cleanup, true)
            }
            Self::EnablePreflightSignalRecording => {
                !std::mem::replace(&mut features.preflight_signal_recording, true)
            }
            Self::EnableByteOffsets => !std::mem::replace(&mut features.byte_offsets, true),
            Self::EnableVqLifecycleRefined => {
                !std::mem::replace(&mut features.vq_lifecycle_refined, true)
            }
            Self::EnableInvocationStatusOutputBlob => {
                !std::mem::replace(&mut features.invocation_status_output_blob, true)
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
/// When adding new fields to this struct, add the [`PartitionFeatureChange`] that enables them.
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
pub struct PersistedFeatures {
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

    /// Virtual queues are enabled on this partition with only inflight-invocations migrated.
    ///
    /// *Since v1.7.5*
    #[bilrost(tag(4))]
    pub vqueues_skip_completed: bool,

    /// Apply completion and journal retention when terminating a preflight invocation.
    ///
    /// *Since v1.7.8*
    #[bilrost(tag(5))]
    pub preflight_invocation_termination_retention: bool,

    /// Pending state mutations from before v1.8.0 have been removed, so all replicas store
    /// state mutations under the same ids.
    ///
    /// *Since v1.8.0*
    #[bilrost(tag(6))]
    pub inconsistent_state_mutation_cleanup: bool,

    /// Signals sent to scheduled or inboxed invocations are recorded in their journal.
    ///
    /// *Since v1.8.1*
    #[bilrost(tag(7))]
    pub preflight_signal_recording: bool,

    /// The partition tracks how far it has applied its log and written its partition store in
    /// bytes.
    ///
    /// *Since v1.8.2*
    #[bilrost(tag(8))]
    pub byte_offsets: bool,

    /// Vqueue entries record their lifecycle more precisely
    /// ([`PartitionFeatureChange::EnableVqLifecycleRefined`]).
    ///
    /// *Since v1.8.2*
    #[bilrost(tag(9))]
    pub vq_lifecycle_refined: bool,

    /// The invocation status of a completed invocation refers to the blob of its Output journal
    /// entry instead of holding a copy of the result. Independent of the partition store's
    /// journal-entry-blobs storage feature, which decides where entry contents are stored.
    /// Proposed by the leader when `experimental-enable-write-blob-table` is set.
    ///
    /// *Since v1.8.2*
    #[bilrost(tag(10))]
    pub invocation_status_output_blob: bool,
}

impl PersistedFeatures {
    /// Whether the feature that `change` enables is enabled.
    #[inline]
    pub fn is_enabled(&self, change: PartitionFeatureChange) -> bool {
        match change {
            PartitionFeatureChange::EnableJournalV2 => self.journal_v2,
            PartitionFeatureChange::EnableVqueues => self.vqueues,
            PartitionFeatureChange::EnableUniqueRandomSeeds => self.unique_random_seeds,
            PartitionFeatureChange::EnableVqueuesSkipCompleted => self.vqueues_skip_completed,
            PartitionFeatureChange::EnablePreflightInvocationTerminationRetention => {
                self.preflight_invocation_termination_retention
            }
            PartitionFeatureChange::EnableInconsistentStateMutationCleanup => {
                self.inconsistent_state_mutation_cleanup
            }
            PartitionFeatureChange::EnablePreflightSignalRecording => {
                self.preflight_signal_recording
            }
            PartitionFeatureChange::EnableByteOffsets => self.byte_offsets,
            PartitionFeatureChange::EnableVqLifecycleRefined => self.vq_lifecycle_refined,
            PartitionFeatureChange::EnableInvocationStatusOutputBlob => {
                self.invocation_status_output_blob
            }
        }
    }

    /// Names of features currently enabled, in declaration order.
    pub fn enabled_names(&self) -> impl Iterator<Item = &'static str> + '_ {
        PartitionFeatureChange::iter()
            .filter(move |change| self.is_enabled(*change))
            .map(PartitionFeatureChange::enabled_name)
    }
}

impl StorageEncode for PersistedFeatures {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), StorageEncodeError> {
        encode::encode_bilrost(self, buf)
    }

    fn default_codec(&self) -> StorageCodecKind {
        StorageCodecKind::Bilrost
    }
}

impl StorageDecode for PersistedFeatures {
    fn decode<B: bytes::Buf>(buf: B, kind: StorageCodecKind) -> Result<Self, StorageDecodeError>
    where
        Self: Sized,
    {
        assert_eq!(kind, StorageCodecKind::Bilrost);
        decode::decode_bilrost(buf)
    }
}

impl FromIterator<PartitionFeatureChange> for PersistedFeatures {
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
        let mut features = PersistedFeatures::default();
        assert!(!features.vqueues);
        assert!(!features.unique_random_seeds);
        PartitionFeatureChange::EnableVqueues.apply_to(&mut features);
        PartitionFeatureChange::EnableUniqueRandomSeeds.apply_to(&mut features);
        assert!(features.vqueues);
        assert!(features.unique_random_seeds);

        // Byte offsets can only be turned on once, by servers from v1.8.2 on.
        let byte_offsets = PartitionFeatureChange::EnableByteOffsets;
        assert_eq!(PartitionFeatureChange::from_repr(8), Some(byte_offsets));
        assert_eq!(
            byte_offsets.min_required_version(),
            &*crate::RESTATE_VERSION_1_8_2
        );
        assert!(byte_offsets.apply_to(&mut features));
        assert!(!byte_offsets.apply_to(&mut features));
        assert!(features.byte_offsets);
        // So can the refined vqueue lifecycle.
        let lifecycle_refined = PartitionFeatureChange::EnableVqLifecycleRefined;
        assert_eq!(
            PartitionFeatureChange::from_repr(9),
            Some(lifecycle_refined)
        );
        assert_eq!(
            lifecycle_refined.min_required_version(),
            &*crate::RESTATE_VERSION_1_8_2
        );
        assert!(lifecycle_refined.apply_to(&mut features));
        assert!(!lifecycle_refined.apply_to(&mut features));
        assert_eq!(
            features.enabled_names().collect::<Vec<_>>(),
            [
                "vqueues",
                "unique_random_seeds",
                "byte_offsets",
                "vq_lifecycle_refined"
            ]
        );
    }

    #[test]
    fn set_vqueue_features() {
        let mut features = PersistedFeatures::default();
        assert!(!features.vqueues);
        assert!(!features.vqueues_skip_completed);

        PartitionFeatureChange::EnableVqueuesSkipCompleted.apply_to(&mut features);
        assert!(!features.vqueues);
        assert!(features.vqueues_skip_completed);

        // Setting vqueues to true, unsets vqueues_skip_completed.
        PartitionFeatureChange::EnableVqueues.apply_to(&mut features);
        assert!(features.vqueues);
        assert!(!features.vqueues_skip_completed);
    }

    #[test]
    fn enabled_names_reflects_set_flags() {
        let mut features = PersistedFeatures::default();
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
