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
    use bilrost::{DistinguishedOwnedMessage, Message, OwnedMessage};
    use rocksdb::MergeOperands;
    use tracing::error;

    use restate_memory::ByteCount;
    use restate_storage_api::vqueue_table::metadata::{Update, UpdateBatch, VQueueMeta};

    use crate::keys::DecodeTableKey;

    use super::MetaKey;

    // A zero byte cannot start a non-empty Bilrost message because top-level field tags start at 1.
    const UPDATE_BATCH_MAGIC: &[u8] = b"\0VQM";
    const UPDATE_BATCH_PREFIX: &[u8] = b"\0VQM\x01";
    const MAX_UPDATE_BATCH_BYTES: usize = 4 * 1024 * 1024;

    #[derive(Debug)]
    enum BatchDecodeError {
        UnsupportedVersion(Option<u8>),
        TooLarge(usize),
        Decode(bilrost::DecodeError),
    }

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

        let mut update = <Update as bilrost::encoding::RawMessage>::empty();
        for op in operands {
            match decode_batch(op) {
                Ok(Some(batch)) => {
                    if !batch.try_apply(&mut vqueue_meta) {
                        let key = MetaKey::deserialize_from(&mut key);
                        error!(
                            ?key,
                            "[full merge] Vqueue meta update batch overflowed a stage counter"
                        );
                        return None;
                    }
                }
                Ok(None) => {
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
                Err(err) => {
                    let key = MetaKey::deserialize_from(&mut key);
                    match err {
                        BatchDecodeError::UnsupportedVersion(version) => error!(
                            ?version,
                            ?key,
                            "[full merge] Unsupported vqueue meta update batch version"
                        ),
                        BatchDecodeError::TooLarge(size) => error!(
                            ?key,
                            size = %ByteCount::from(size),
                            limit = %ByteCount::from(MAX_UPDATE_BATCH_BYTES),
                            "[full merge] Vqueue meta update batch exceeds the size limit"
                        ),
                        BatchDecodeError::Decode(err) => error!(
                            ?err,
                            ?key,
                            "[full merge] Failed to decode vqueue meta update batch ({} bytes)",
                            op.len(),
                        ),
                    }
                    return None;
                }
            }
        }

        Some(vqueue_meta.encode_contiguous().into_vec())
    }

    pub fn partial_merge(
        key: &[u8],
        _unused: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        partial_merge_slices(key, operands)
    }

    pub fn partial_merge_slices<'a>(
        mut key: &[u8],
        operands: impl IntoIterator<Item = &'a [u8]>,
    ) -> Option<Vec<u8>> {
        let mut merged = UpdateBatch::default();
        let mut update = <Update as bilrost::encoding::RawMessage>::empty();
        let mut input_bytes = 0_usize;

        for op in operands {
            input_bytes = input_bytes.checked_add(op.len())?;
            if input_bytes > MAX_UPDATE_BATCH_BYTES {
                return None;
            }

            match decode_batch(op) {
                Ok(Some(batch)) => {
                    if !merged.try_append(batch) {
                        let key = MetaKey::deserialize_from(&mut key);
                        error!(
                            ?key,
                            "[partial merge] Vqueue meta update batch overflowed a stage counter"
                        );
                        return None;
                    }
                }
                Ok(None) => {
                    if let Err(err) = update.replace_from_slice(op) {
                        let key = MetaKey::deserialize_from(&mut key);
                        error!(
                            ?err,
                            ?key,
                            "[partial merge] Failed to decode vqueue meta update ({} bytes)",
                            op.len(),
                        );
                        return None;
                    }
                    if !merged.try_push(&update) {
                        let key = MetaKey::deserialize_from(&mut key);
                        error!(
                            ?key,
                            "[partial merge] Vqueue meta update batch overflowed a stage counter"
                        );
                        return None;
                    }
                }
                Err(err) => {
                    let key = MetaKey::deserialize_from(&mut key);
                    match err {
                        BatchDecodeError::UnsupportedVersion(version) => error!(
                            ?version,
                            ?key,
                            "[partial merge] Unsupported vqueue meta update batch version"
                        ),
                        BatchDecodeError::TooLarge(size) => error!(
                            ?key,
                            size = %ByteCount::from(size),
                            limit = %ByteCount::from(MAX_UPDATE_BATCH_BYTES),
                            "[partial merge] Vqueue meta update batch exceeds the size limit"
                        ),
                        BatchDecodeError::Decode(err) => error!(
                            ?err,
                            ?key,
                            "[partial merge] Failed to decode vqueue meta update batch ({} bytes)",
                            op.len(),
                        ),
                    }
                    return None;
                }
            }
        }

        if merged.encoded_len() + UPDATE_BATCH_PREFIX.len() > MAX_UPDATE_BATCH_BYTES {
            return None;
        }
        let encoded = merged.encode_contiguous().into_vec();
        let mut operand = Vec::with_capacity(UPDATE_BATCH_PREFIX.len() + encoded.len());
        operand.extend_from_slice(UPDATE_BATCH_PREFIX);
        operand.extend_from_slice(&encoded);
        Some(operand)
    }

    fn decode_batch(operand: &[u8]) -> Result<Option<UpdateBatch>, BatchDecodeError> {
        if let Some(payload) = operand.strip_prefix(UPDATE_BATCH_PREFIX) {
            if operand.len() > MAX_UPDATE_BATCH_BYTES {
                return Err(BatchDecodeError::TooLarge(operand.len()));
            }
            return UpdateBatch::decode_canonical(payload)
                .map(Some)
                .map_err(BatchDecodeError::Decode);
        }
        if let Some(versioned) = operand.strip_prefix(UPDATE_BATCH_MAGIC) {
            return Err(BatchDecodeError::UnsupportedVersion(
                versioned.first().copied(),
            ));
        }
        Ok(None)
    }

    #[cfg(test)]
    mod tests {
        use bilrost::Message;

        use restate_clock::UniqueTimestamp;
        use restate_clock::time::MillisSinceEpoch;
        use restate_limiter::LimitKey;
        use restate_storage_api::vqueue_table::Stage;
        use restate_storage_api::vqueue_table::metadata::{
            Action, MoveMetrics, Update, VQueueLink, VQueueMeta,
        };
        use restate_storage_api::vqueue_table::stats::WaitStats;
        use restate_types::vqueues::VQueueId;

        use crate::keys::KeyKind;

        use super::*;

        const BASE_TS_MS: u64 = 1_744_000_000_000;

        fn ts(offset_ms: u64) -> UniqueTimestamp {
            UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(
                BASE_TS_MS + offset_ms,
            ))
        }

        fn metrics(
            last_transition_ms: u64,
            first_runnable_ms: u64,
            has_started: bool,
            wait_stats: Option<WaitStats>,
        ) -> MoveMetrics {
            MoveMetrics {
                last_transition_at: ts(last_transition_ms),
                has_started,
                first_runnable_at: ts(first_runnable_ms).to_unix_millis(),
                scheduler_wait_stats: wait_stats,
            }
        }

        fn raw_operands() -> Vec<Vec<u8>> {
            [
                Update::new(
                    ts(1),
                    Action::Move {
                        prev_stage: None,
                        next_stage: Stage::Inbox,
                        metrics: metrics(1, 1, false, None),
                    },
                ),
                Update::new(
                    ts(2),
                    Action::Move {
                        prev_stage: Some(Stage::Inbox),
                        next_stage: Stage::Running,
                        metrics: metrics(
                            1,
                            1,
                            false,
                            Some(WaitStats {
                                blocked_on_concurrency_rules_ms: 100,
                                ..WaitStats::default()
                            }),
                        ),
                    },
                ),
                Update::new(
                    ts(3),
                    Action::Move {
                        prev_stage: Some(Stage::Running),
                        next_stage: Stage::Suspended,
                        metrics: metrics(2, 1, true, None),
                    },
                ),
                Update::new(
                    ts(4),
                    Action::Move {
                        prev_stage: Some(Stage::Suspended),
                        next_stage: Stage::Inbox,
                        metrics: metrics(3, 1, true, None),
                    },
                ),
                Update::new(
                    ts(5),
                    Action::Move {
                        prev_stage: Some(Stage::Inbox),
                        next_stage: Stage::Running,
                        metrics: metrics(
                            4,
                            1,
                            true,
                            Some(WaitStats {
                                blocked_on_invoker_throttling_ms: 200,
                                ..WaitStats::default()
                            }),
                        ),
                    },
                ),
                Update::new(
                    ts(6),
                    Action::Move {
                        prev_stage: Some(Stage::Running),
                        next_stage: Stage::Finished,
                        metrics: metrics(5, 1, true, None),
                    },
                ),
                Update::new(
                    ts(7),
                    Action::RemoveEntry {
                        stage: Stage::Finished,
                    },
                ),
                Update::new(ts(8), Action::PauseVQueue {}),
                Update::new(ts(9), Action::ResumeVQueue {}),
            ]
            .into_iter()
            .map(|update| update.encode_contiguous().into_vec())
            .collect()
        }

        #[test]
        fn partial_merge_matches_raw_updates_and_composes_batches() {
            let key = MetaKey::from(&VQueueId::custom(1, "partial-merge-test")).to_bytes();
            let existing = VQueueMeta::new(ts(0), None, LimitKey::None, VQueueLink::None)
                .encode_contiguous()
                .into_vec();
            let operands = raw_operands();
            let operand_slices = operands.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let expected = full_merge_slices(&key, Some(&existing), operand_slices.iter().copied())
                .expect("raw updates should merge");

            let first = partial_merge_slices(&key, operand_slices[..4].iter().copied())
                .expect("first update group should partially merge");
            let second = partial_merge_slices(&key, operand_slices[4..].iter().copied())
                .expect("second update group should partially merge");
            assert!(first.starts_with(UPDATE_BATCH_PREFIX));
            assert!(second.starts_with(UPDATE_BATCH_PREFIX));

            let combined = partial_merge_slices(&key, [first.as_slice(), second.as_slice()])
                .expect("update batches should compose");
            let actual = full_merge_slices(&key, Some(&existing), [combined.as_slice()])
                .expect("composed update batch should fully merge");
            assert_eq!(actual, expected);

            let mixed = partial_merge_slices(
                &key,
                std::iter::once(first.as_slice()).chain(operand_slices[4..].iter().copied()),
            )
            .expect("legacy updates and an update batch should partially merge");
            let actual = full_merge_slices(&key, Some(&existing), [mixed.as_slice()])
                .expect("mixed update batch should fully merge");
            assert_eq!(actual, expected);
        }

        #[test]
        fn unknown_update_batch_version_is_rejected() {
            let key = MetaKey::from(&VQueueId::custom(1, "partial-merge-version-test")).to_bytes();
            let operand = b"\0VQM\x02";

            assert!(partial_merge_slices(&key, [operand.as_slice()]).is_none());
            let existing = VQueueMeta::new(ts(0), None, LimitKey::None, VQueueLink::None)
                .encode_contiguous()
                .into_vec();
            assert!(full_merge_slices(&key, Some(&existing), [operand.as_slice()]).is_none());
        }

        #[test]
        fn update_batch_with_unknown_fields_is_rejected() {
            let key = MetaKey::from(&VQueueId::custom(1, "partial-merge-fields-test")).to_bytes();
            // Canonical Bilrost encoding for unknown varint field 17 with value 1.
            let operand = b"\0VQM\x01\x44\x01";

            assert!(partial_merge_slices(&key, [operand.as_slice()]).is_none());
            let existing = VQueueMeta::new(ts(0), None, LimitKey::None, VQueueLink::None)
                .encode_contiguous()
                .into_vec();
            assert!(full_merge_slices(&key, Some(&existing), [operand.as_slice()]).is_none());
        }

        #[test]
        fn oversized_update_batch_is_rejected_before_decoding() {
            let key = MetaKey::from(&VQueueId::custom(1, "partial-merge-size-test")).to_bytes();
            let mut operand = vec![0; MAX_UPDATE_BATCH_BYTES + 1];
            operand[..UPDATE_BATCH_PREFIX.len()].copy_from_slice(UPDATE_BATCH_PREFIX);

            assert!(partial_merge_slices(&key, [operand.as_slice()]).is_none());
            let existing = VQueueMeta::new(ts(0), None, LimitKey::None, VQueueLink::None)
                .encode_contiguous()
                .into_vec();
            assert!(full_merge_slices(&key, Some(&existing), [operand.as_slice()]).is_none());
        }

        #[test]
        fn rocksdb_flush_and_compaction_preserve_partially_merged_updates() {
            let directory = tempfile::tempdir().expect("temporary directory should be created");
            let mut options = rocksdb::Options::default();
            options.create_if_missing(true);
            options.set_merge_operator(
                "VQueueMetaPartialMergeTest",
                KeyKind::full_merge,
                partial_merge,
            );

            let key = MetaKey::from(&VQueueId::custom(1, "rocksdb-partial-merge-test")).to_bytes();
            let existing = VQueueMeta::new(ts(0), None, LimitKey::None, VQueueLink::None)
                .encode_contiguous()
                .into_vec();
            let operands = raw_operands();
            let expected =
                full_merge_slices(&key, Some(&existing), operands.iter().map(Vec::as_slice))
                    .expect("raw updates should merge");

            {
                let db = rocksdb::DB::open(&options, directory.path())
                    .expect("rocksdb should be opened");
                db.put(key, &existing)
                    .expect("base value should be written");
                db.flush().expect("base value should be flushed");
                for operand in &operands {
                    db.merge(key, operand)
                        .expect("merge operand should be written");
                }
                db.flush().expect("merge operands should be flushed");
            }

            let db =
                rocksdb::DB::open(&options, directory.path()).expect("rocksdb should be reopened");
            assert_eq!(
                db.get(key).expect("merged value should be read").as_deref(),
                Some(expected.as_slice())
            );
            db.compact_range::<&[u8], &[u8]>(None, None);
            assert_eq!(
                db.get(key)
                    .expect("compacted merged value should be read")
                    .as_deref(),
                Some(expected.as_slice())
            );
        }
    }
}
