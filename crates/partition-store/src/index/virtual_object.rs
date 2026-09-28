// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_storage_api::vqueue_table::Stage;
use restate_types::ServiceName;
use restate_types::vqueues::{CanonicalEntryId, Seq};
use restate_util_string::ReString;

use super::macros::define_secondary_index;

define_secondary_index!(
    /// Find the most recently transitioned entries for one virtual object and stage.
    ///
    /// Ordering within a partition:
    /// `service_name ASC -> scope ASC -> key ASC -> stage ASC`, then
    /// `transitioned_at DESC -> canonical_id ASC`.
    /// Fix service, scope, object key, and stage to scan newest transitions first.
    /// Includes virtual-object invocations and external state mutations.
    EntryByVirtualObjectStage,
    key: EntryByVirtualObjectStageKey {
        service_name: ServiceName => str,
        scope: Option<ReString> => str,
        key: ReString => str,
        stage: Stage,
        transitioned_at: Reverse<UniqueTimestamp>,
        canonical_id: CanonicalEntryId (primary_key),
    }
);

define_secondary_index!(
    /// Find the earliest next transitions for one virtual object and stage.
    ///
    /// Ordering within a partition:
    /// `service_name ASC -> scope ASC -> key ASC -> stage ASC`, then
    /// `next_at ASC -> seq ASC -> canonical_id ASC`.
    /// Fix service, scope, object key, and stage to scan earliest transitions first.
    /// Sequence breaks same-second ties; includes invocations and external state mutations.
    EntryNextAtByVirtualObjectStage,
    key: EntryNextAtByVirtualObjectStageKey {
        service_name: ServiceName => str,
        scope: Option<ReString> => str,
        key: ReString => str,
        stage: Stage,
        next_at: RoughTimestamp,
        // Derived from canonical_id; repeated here to order across partition keys.
        seq: Seq,
        canonical_id: CanonicalEntryId (primary_key),
    }
);

#[cfg(test)]
mod tests {
    use restate_types::identifiers::BaseEntryId;
    use restate_types::sharding::PartitionId;
    use restate_types::vqueues::{EntryId, EntryKind};

    use crate::index::{IndexId, SecondaryIndexKey};
    use crate::keys::IndexKeyPrefix;

    use super::*;

    #[test]
    fn virtual_object_indexes_preserve_identity_and_opposite_time_ordering() {
        let partition = PartitionId::MIN;
        let base = BaseEntryId::new(3337, EntryId::new(EntryKind::StateMutation, [42; 16]));
        let mut previous = None;
        for (service, scope, object) in [
            ("svc", None, "a"),
            ("svc", Some(""), "a"),
            ("svc", Some("tenant"), "a"),
            ("svc", Some("tenant"), "b"),
            ("zzz", None, "a"),
        ] {
            let mut transitions = Vec::new();
            let mut next_times = Vec::new();
            for n in [255u32, 256] {
                let id = base.canonicalize(Seq::new(u64::from(n)));
                let at = UniqueTimestamp::try_from_parts(u64::from(n), 1).unwrap();
                let next_at = RoughTimestamp::new(n);
                let mut bytes = Vec::new();
                EntryByVirtualObjectStageKey::borrowed(
                    service,
                    scope,
                    object,
                    Stage::Inbox,
                    Reverse(at),
                    id,
                )
                .encode_key(partition, &mut bytes);
                let (prefix, payload) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
                assert_eq!(prefix.index_id(), Some(IndexId::EntryByVirtualObjectStage));
                let decoded = payload
                    .into_decoder::<EntryByVirtualObjectStageKey>()
                    .decode_all()
                    .unwrap();
                assert_eq!(
                    (
                        decoded.service_name.as_str(),
                        decoded.scope.as_deref(),
                        decoded.key.as_str()
                    ),
                    (service, scope, object)
                );
                assert_eq!(
                    (decoded.stage, decoded.transitioned_at, decoded.canonical_id),
                    (Stage::Inbox, Reverse(at), id)
                );
                transitions.push(bytes);

                let mut bytes = Vec::new();
                EntryNextAtByVirtualObjectStageKey::borrowed(
                    service,
                    scope,
                    object,
                    Stage::Inbox,
                    next_at,
                    id.seq(),
                    id,
                )
                .encode_key(partition, &mut bytes);
                let (prefix, payload) = IndexKeyPrefix::decode_prefix(&bytes).unwrap();
                assert_eq!(
                    prefix.index_id(),
                    Some(IndexId::EntryNextAtByVirtualObjectStage)
                );
                let decoded = payload
                    .into_decoder::<EntryNextAtByVirtualObjectStageKey>()
                    .decode_all()
                    .unwrap();
                assert_eq!(
                    (
                        decoded.service_name.as_str(),
                        decoded.scope.as_deref(),
                        decoded.key.as_str()
                    ),
                    (service, scope, object)
                );
                assert_eq!(
                    (
                        decoded.stage,
                        decoded.next_at,
                        decoded.seq,
                        decoded.canonical_id
                    ),
                    (Stage::Inbox, next_at, id.seq(), id)
                );
                next_times.push(bytes);
            }
            assert!(transitions[1] < transitions[0]);
            assert!(next_times[0] < next_times[1]);
            if let Some((last_transition, last_next)) = previous {
                assert!(last_transition < transitions[1]);
                assert!(last_next < next_times[0]);
            }
            previous = Some((transitions.remove(0), next_times.remove(1)));
        }
    }
}
