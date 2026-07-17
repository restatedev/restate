// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::RawStatusHeader;
use restate_storage_api::vqueue_table::{EntryKey, OwnedEntryStatusHeader};
use restate_types::identifiers::{InvocationId, PartitionKey, WithPartitionKey};
use restate_types::vqueues::EntryId;

use crate::TableKind;
use crate::keys::{KeyKind, define_table_key};

// `qs` | PKEY | ENTRY_ID
define_table_key!(
    TableKind::VQueue,
    KeyKind::VQueueEntryStatus,
    EntryStatusKey(
        partition_key: PartitionKey,
        id: EntryId,
    )
);

impl EntryStatusKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + EntryId::serialized_length_fixed()
    }
}

impl From<&InvocationId> for EntryStatusKey {
    #[inline]
    fn from(id: &InvocationId) -> Self {
        EntryStatusKey {
            partition_key: WithPartitionKey::partition_key(id),
            id: EntryId::from(id),
        }
    }
}

pub(super) fn entry_status_header_from_raw(
    entry_id: EntryId,
    header: RawStatusHeader,
) -> OwnedEntryStatusHeader {
    OwnedEntryStatusHeader::new(
        header.qid,
        header.stage,
        EntryKey::new(header.has_lock, header.next_run_at, header.seq, entry_id),
        header.metadata,
        header.stats,
        header.status,
    )
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use restate_types::vqueues::VQueueEntryId;

    use crate::keys::EncodeTableKeyPrefix;

    use super::*;

    /// Encodes a `VQueueEntryId` exactly the way its `EntryStatusKey` lands on disk:
    /// `qs | partition_key (u64 BE) | kind (u8) | remainder (16B)`.
    fn encode(id: VQueueEntryId) -> BytesMut {
        EntryStatusKey {
            partition_key: id.partition_key(),
            id: EntryId::from(id),
        }
        .serialize()
    }

    /// A spread of ids exercising every tier of the comparison:
    /// - partition keys whose relative order flips under little- vs big-endian,
    /// - both entry kinds (Invocation = 0x69 < StateMutation = 0x73),
    /// - remainders differing only in the first vs last byte (bytewise order).
    fn sample_ids() -> Vec<VQueueEntryId> {
        let r0 = [0u8; 16];
        let mut r_first = [0u8; 16];
        r_first[0] = 1;
        let mut r_last = [0u8; 16];
        r_last[15] = 1;
        let r_max = [0xffu8; 16];

        vec![
            VQueueEntryId::Invocation(0, r0),
            VQueueEntryId::StateMutation(0, r0),
            VQueueEntryId::Invocation(0, r_first),
            VQueueEntryId::Invocation(0, r_last),
            VQueueEntryId::Invocation(0, r_max),
            // partition keys that catch a little-endian mistake:
            // 0x0000_0000_0000_00ff must sort before 0x0000_0000_0000_ff00.
            VQueueEntryId::Invocation(0x0000_0000_0000_00ff, r0),
            VQueueEntryId::Invocation(0x0000_0000_0000_ff00, r0),
            VQueueEntryId::StateMutation(0x0000_0000_0000_00ff, r_max),
            VQueueEntryId::Invocation(0xff00_0000_0000_0000, r0),
            VQueueEntryId::StateMutation(u64::MAX, r0),
            VQueueEntryId::Invocation(u64::MAX, r_max),
        ]
    }

    /// The hand-written `Ord`/`PartialOrd` on `VQueueEntryId` must agree with the
    /// lexicographic byte ordering of its encoded `EntryStatusKey` for every pair.
    /// The `qs` kind prefix is identical for all entries, so it doesn't affect the
    /// relative ordering.
    #[test]
    fn ord_matches_encoded_entry_status_key_bytes() {
        let ids = sample_ids();

        for a in &ids {
            for b in &ids {
                let logical = a.cmp(b);
                let bytewise = encode(*a).as_ref().cmp(encode(*b).as_ref());
                assert_eq!(
                    logical, bytewise,
                    "ordering mismatch between {a:?} and {b:?}: \
                     VQueueEntryId::cmp = {logical:?} but encoded bytes compare = {bytewise:?}"
                );

                // PartialOrd must delegate to Ord.
                assert_eq!(a.partial_cmp(b), Some(logical));
            }
        }
    }

    /// Sorting a collection by `VQueueEntryId::Ord` yields the same sequence as
    /// sorting by the encoded key bytes (RocksDB's on-disk order).
    #[test]
    fn sort_order_agrees_with_encoded_bytes() {
        let mut by_logical = sample_ids();
        by_logical.sort();

        let mut by_bytes = sample_ids();
        by_bytes.sort_by(|a, b| encode(*a).as_ref().cmp(encode(*b).as_ref()));

        assert_eq!(
            by_logical, by_bytes,
            "sorting by VQueueEntryId::Ord disagrees with sorting by encoded key bytes"
        );
    }
}
