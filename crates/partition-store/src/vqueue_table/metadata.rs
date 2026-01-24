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
use restate_types::vqueue::{VQueueId, VQueueInstance, VQueueParent};

use crate::TableKind::VQueue;
use crate::keys::{KeyKind, TableKey, define_table_key};

use super::inbox::ActiveKey;

// 'qm' | QID
define_table_key!(
    VQueue,
    KeyKind::VQueueMeta,
    MetaKey(
        partition_key: PartitionKey,
        parent: VQueueParent,
        instance: VQueueInstance,
    )
);

impl MetaKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + std::mem::size_of::<VQueueParent>()
            + std::mem::size_of::<VQueueInstance>()
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::serialized_length_fixed()] {
        let mut buf = [0u8; Self::serialized_length_fixed()];
        self.serialize_to(&mut buf.as_mut());
        buf
    }
}

impl From<VQueueId> for MetaKey {
    #[inline]
    fn from(qid: VQueueId) -> Self {
        MetaKey {
            partition_key: qid.partition_key,
            parent: qid.parent,
            instance: qid.instance,
        }
    }
}

impl From<MetaKey> for VQueueId {
    #[inline]
    fn from(key: MetaKey) -> Self {
        VQueueId {
            partition_key: key.partition_key,
            parent: key.parent,
            instance: key.instance,
        }
    }
}

impl From<ActiveKey> for MetaKey {
    #[inline]
    fn from(qid: ActiveKey) -> Self {
        MetaKey {
            partition_key: qid.partition_key,
            parent: qid.parent,
            instance: qid.instance,
        }
    }
}

// Rocksdb merge operator for the vqueue keys
pub(crate) mod vqueue_meta_merge {
    use bilrost::{Message, OwnedMessage};
    use rocksdb::MergeOperands;
    use tracing::error;

    use restate_storage_api::vqueue_table::metadata::{VQueueMeta, VQueueMetaUpdates};

    use crate::keys::TableKey;

    use super::MetaKey;

    pub fn full_merge(
        mut key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut vqueue_meta = VQueueMeta::default();
        if let Some(existing_val) = existing_val
            && let Err(e) = vqueue_meta.replace_from_slice(existing_val)
        {
            let key = MetaKey::deserialize_from(&mut key);
            error!(
                key = ?key,
                "[full merge] Failed to decode ({} bytes) VQueueMeta: {}",
                existing_val.len(),
                e
            );
            return None;
        }

        for op in operands {
            let updates = match VQueueMetaUpdates::decode(op) {
                Err(err) => {
                    let key = MetaKey::deserialize_from(&mut key);
                    error!(
                        ?err,
                        ?key,
                        "[full merge] Failed to decode vqueue meta batched updates ({} bytes)",
                        op.len(),
                    );
                    return None;
                }
                Ok(updates) => updates,
            };
            if let Err(err) = vqueue_meta.apply_updates(&updates) {
                let key = MetaKey::deserialize_from(&mut key);
                error!(
                    ?key,
                    ?err,
                    "[full merge] Failed to apply vqueue meta update"
                );
                return None;
            }
        }
        Some(vqueue_meta.encode_to_vec())
    }

    pub fn partial_merge(mut _key: &[u8], operands: &MergeOperands) -> Option<Vec<u8>> {
        let mut updates =
            VQueueMetaUpdates::with_capacity(operands.len() * VQueueMetaUpdates::INLINED_UPDATES);
        for op in operands {
            let partial_updates = match VQueueMetaUpdates::decode(op) {
                Err(err) => {
                    error!(
                        ?err,
                        "[partial merge] Failed to decode vqueue meta batched updates"
                    );
                    return None;
                }
                Ok(u) => u,
            };
            updates.extend(partial_updates);
        }
        Some(updates.encode_to_vec())
    }
}
