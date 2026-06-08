// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::VQueueId;

use crate::TableKind::VQueue;
use crate::keys::{EncodeTableKey, KeyKind, define_table_key};

// 'qm' | QID
define_table_key!(
    VQueue,
    KeyKind::VQueueMeta,
    MetaKey(
        qid: VQueueId,
    )
);

// 'qa' | QID (QID is prefixed by PartitionKey internally)
define_table_key!(
    VQueue,
    KeyKind::VQueueActive,
    ActiveKey(
        qid: VQueueId,
    )
);

static_assertions::const_assert_eq!(ActiveKey::serialized_length_fixed(), 27);

impl ActiveKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH + VQueueId::serialized_length_fixed()
    }

    pub const fn by_partition_prefix_len() -> usize {
        KeyKind::SERIALIZED_LENGTH + std::mem::size_of::<PartitionKey>()
    }
}

impl MetaKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH + VQueueId::serialized_length_fixed()
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        self.serialize_to(&mut buf.as_mut());
        buf
    }
}

impl From<&VQueueId> for MetaKey {
    #[inline]
    fn from(qid: &VQueueId) -> Self {
        MetaKey { qid: qid.clone() }
    }
}

impl From<MetaKey> for VQueueId {
    #[inline]
    fn from(key: MetaKey) -> Self {
        key.qid
    }
}

impl From<ActiveKey> for MetaKey {
    #[inline]
    fn from(key: ActiveKey) -> Self {
        MetaKey { qid: key.qid }
    }
}

// Rocksdb merge operator for the vqueue keys
pub mod vqueue_meta_merge {
    use bilrost::{Message, OwnedMessage};
    use rocksdb::MergeOperands;
    use tracing::error;

    use restate_storage_api::vqueue_table;
    use restate_storage_api::vqueue_table::metadata::VQueueMeta;

    use crate::keys::DecodeTableKey;

    use super::MetaKey;

    pub fn full_merge(
        key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        full_merge_slices(key, existing_val, operands)
    }

    pub fn full_merge_slices<'a>(
        mut key: &[u8],
        existing_val: Option<&[u8]>,
        operands: impl IntoIterator<Item = &'a [u8]>,
    ) -> Option<Vec<u8>> {
        let Some(mut existing_val) = existing_val else {
            let key = MetaKey::deserialize_from(&mut key);
            error!(
                key = ?key,
                "[full merge] Failed to merge vqueue metadata updates with a non-existent vqueue",
            );
            return None;
        };

        let mut vqueue_meta = match VQueueMeta::decode(&mut existing_val) {
            Ok(m) => m,
            Err(e) => {
                error!(
                    key = ?key,
                    "[full merge] Failed to decode existing VQueueMeta ({} bytes): {e}",
                    existing_val.len(),
                );
                return None;
            }
        };

        let mut update = <vqueue_table::metadata::Update as bilrost::encoding::RawMessage>::empty();
        for op in operands {
            if let Err(err) = update.replace_from_slice(op) {
                let key = MetaKey::deserialize_from(&mut key);
                error!(
                    ?err,
                    ?key,
                    "[full merge] Failed to decode vqueue meta update ({} bytes)",
                    op.len(),
                );
                return None;
            }
            vqueue_meta.apply_update(&update);
        }

        Some(vqueue_meta.encode_contiguous().into_vec())
    }
}
