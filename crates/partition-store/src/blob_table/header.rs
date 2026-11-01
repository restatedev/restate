// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;

use anyhow::{Context, anyhow};
use restate_storage_api::{Result, StorageError, blob_table::Header};
use restate_types::blob::BlobKind;
use zerocopy::TryFromBytes;

/// Fixed-size header prefixed to every encoded blob value.
#[derive(
    Debug,
    Clone,
    Copy,
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct BlobHeader {
    version: u8,
    kind: BlobKind,
    ref_count: zerocopy::big_endian::U16,
}

impl BlobHeader {
    pub const VERSION: u8 = 0;

    pub fn new(kind: BlobKind, ref_count: u16) -> Self {
        Self {
            version: Self::VERSION,
            kind,
            ref_count: ref_count.into(),
        }
    }

    pub const fn serialized_length_fixed() -> usize {
        size_of::<u8>() + size_of::<BlobKind>() + size_of::<u16>()
    }

    /// Verifies the header version then decodes the header, returning the remaining payload.
    pub fn decode(value: &[u8]) -> Result<(&Self, &[u8])> {
        let version = *value
            .first()
            .ok_or_else(|| StorageError::Conversion(anyhow!("blob value is empty")))?;
        if version != Self::VERSION {
            return Err(StorageError::Conversion(anyhow!(
                "unknown blob header version {version}"
            )));
        }

        Self::try_ref_from_prefix(value)
            .map_err(|err| StorageError::Conversion(anyhow!("malformed blob header: {err}")))
    }
}

impl Header for BlobHeader {
    fn kind(&self) -> BlobKind {
        self.kind
    }

    fn ref_count(&self) -> u16 {
        self.ref_count.get()
    }
}

impl Header for &BlobHeader {
    fn kind(&self) -> BlobKind {
        self.kind
    }

    fn ref_count(&self) -> u16 {
        self.ref_count.get()
    }
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, bilrost::Oneof, bilrost::Message)]
enum Action {
    #[default]
    #[bilrost(empty)]
    Unknown,
    #[bilrost(tag(1))]
    Increment { count: u16 },
    #[bilrost(tag(2))]
    Decrement { count: u16 },
}

#[derive(Debug, Default, Clone, Eq, PartialEq, bilrost::Message)]
pub(super) struct HeaderUpdate {
    #[bilrost(tag(1))]
    actions: Vec<Action>,
}

impl HeaderUpdate {
    pub fn increment(count: u16) -> Self {
        Self {
            actions: vec![Action::Increment { count }],
        }
    }

    pub fn decrement(count: u16) -> Self {
        Self {
            actions: vec![Action::Decrement { count }],
        }
    }

    fn apply(&self, value: &mut BlobHeader) {
        for action in &self.actions {
            let current = value.ref_count.get();
            match action {
                Action::Unknown => {}
                Action::Increment { count } => value.ref_count.set(current.saturating_add(*count)),
                Action::Decrement { count } => value.ref_count.set(current.saturating_sub(*count)),
            }
        }
    }

    fn merge(mut self, update: &HeaderUpdate) -> anyhow::Result<Self> {
        let mut increments = 0u16;
        let mut decrements = 0u16;

        for action in self.actions.iter().chain(&update.actions) {
            match action {
                Action::Unknown => {}
                Action::Increment { count } => {
                    increments = increments
                        .checked_add(*count)
                        .context("merge will cause overflow")?
                }
                Action::Decrement { count } => {
                    decrements = decrements
                        .checked_add(*count)
                        .context("merge will cause overflow")?
                }
            }

            match increments.cmp(&decrements) {
                Ordering::Greater => {
                    increments -= decrements;
                    decrements = 0;
                }
                Ordering::Less => {
                    decrements -= increments;
                    increments = 0;
                }
                Ordering::Equal => {
                    increments = 0;
                    decrements = 0;
                }
            }
        }

        let mut actions = std::mem::take(&mut self.actions);
        actions.clear();

        if increments > 0 {
            actions.push(Action::Increment { count: increments });
        }
        if decrements > 0 {
            actions.push(Action::Decrement { count: decrements });
        }
        let merged = HeaderUpdate { actions };

        Ok(merged)
    }
}

pub mod merge {
    use anyhow::Context;
    use bilrost::{Message, OwnedMessage};
    use bytes::{BufMut, BytesMut};
    use rocksdb::{CompactionDecision, MergeOperands};
    use tracing::warn;
    use zerocopy::IntoBytes;

    use crate::{
        blob_table::{BlobHeader, BlobKey, HeaderUpdate},
        keys::DecodeTableKey,
    };

    pub fn full_merge(
        mut key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        match full_merge_inner(existing_val, operands) {
            Ok(value) => value,
            Err(err) => {
                let key = BlobKey::deserialize_from(&mut key);
                warn!(key = ?key, "[full merge][blob] {err}");
                None
            }
        }
    }

    fn full_merge_inner(
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let existing_val =
            existing_val.context("Failed to merge blob updates with a non-existent value")?;

        let (header, payload) =
            BlobHeader::decode(existing_val).context("Failed to decode blob header")?;

        if header.ref_count == 0 {
            // if ref_count is already 0 the blob is already
            // a tombstone that is marked for deletion and should
            // never be updated again.
            // return the header as is
            return Ok(Some(header.as_bytes().to_vec()));
        }

        let mut header = *header;

        let mut merged = HeaderUpdate::default();
        let mut update = HeaderUpdate::default();
        for operand in operands {
            update
                .replace_from(operand)
                .context("Failed to decode blob update operand")?;

            merged = merged.merge(&update)?;
        }

        merged.apply(&mut header);

        if header.ref_count == 0 {
            // ref count reached zero. mark the object for deletion
            Ok(Some(header.as_bytes().to_vec()))
        } else {
            // return new blob data
            let mut buf =
                BytesMut::with_capacity(BlobHeader::serialized_length_fixed() + payload.len());
            buf.put_slice(header.as_bytes());
            buf.put_slice(payload);

            Ok(Some(buf.to_vec()))
        }
    }

    pub fn partial_merge(mut key: &[u8], operands: &MergeOperands) -> Option<Vec<u8>> {
        match partial_merge_inner(operands) {
            Ok(value) => value,
            Err(err) => {
                let key = BlobKey::deserialize_from(&mut key);
                warn!(key = ?key, "[partial merge][blob] {err}");
                None
            }
        }
    }

    fn partial_merge_inner(operands: &MergeOperands) -> anyhow::Result<Option<Vec<u8>>> {
        let mut merged = HeaderUpdate::default();
        let mut update = HeaderUpdate::default();
        for operand in operands {
            update
                .replace_from(operand)
                .context("Failed to decode blob update operand")?;

            merged = merged.merge(&update)?;
        }

        Ok(Some(merged.encode_to_vec()))
    }

    pub fn compaction_filter(_key: &[u8], value: &[u8]) -> CompactionDecision {
        let (header, _) = match BlobHeader::decode(value) {
            Ok(value) => value,
            Err(err) => {
                warn!(err =%err, "[compaction filter] Failed to decode blob header");
                return CompactionDecision::Keep;
            }
        };

        if header.ref_count > 0 {
            CompactionDecision::Keep
        } else {
            CompactionDecision::Remove
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge_all(updates: impl IntoIterator<Item = HeaderUpdate>) -> anyhow::Result<HeaderUpdate> {
        updates
            .into_iter()
            .try_fold(HeaderUpdate::default(), |merged, update| {
                merged.merge(&update)
            })
    }

    #[test]
    fn merge_nets_out_actions() {
        // merging into an empty update keeps the single action
        assert_eq!(
            merge_all([HeaderUpdate::increment(3)]).unwrap(),
            HeaderUpdate::increment(3)
        );
        assert_eq!(
            merge_all([HeaderUpdate::decrement(3)]).unwrap(),
            HeaderUpdate::decrement(3)
        );

        // same-direction actions are summed
        assert_eq!(
            merge_all([HeaderUpdate::increment(1), HeaderUpdate::increment(2)]).unwrap(),
            HeaderUpdate::increment(3)
        );
        assert_eq!(
            merge_all([HeaderUpdate::decrement(1), HeaderUpdate::decrement(2)]).unwrap(),
            HeaderUpdate::decrement(3)
        );

        // opposite actions net out to a single action, or none at all
        assert_eq!(
            merge_all([HeaderUpdate::increment(5), HeaderUpdate::decrement(2)]).unwrap(),
            HeaderUpdate::increment(3)
        );
        assert_eq!(
            merge_all([HeaderUpdate::increment(2), HeaderUpdate::decrement(5)]).unwrap(),
            HeaderUpdate::decrement(3)
        );
        assert_eq!(
            merge_all([HeaderUpdate::increment(4), HeaderUpdate::decrement(4)]).unwrap(),
            HeaderUpdate::default()
        );
        assert_eq!(
            merge_all([
                HeaderUpdate::decrement(1),
                HeaderUpdate::increment(3),
                HeaderUpdate::decrement(1),
                HeaderUpdate::increment(1),
            ])
            .unwrap(),
            HeaderUpdate::increment(2)
        );

        // unknown actions are ignored
        let unknown = HeaderUpdate {
            actions: vec![Action::Unknown],
        };
        assert_eq!(
            merge_all([unknown, HeaderUpdate::increment(1)]).unwrap(),
            HeaderUpdate::increment(1)
        );

        // a self-contained update with multiple actions is also normalized
        let mixed = HeaderUpdate {
            actions: vec![
                Action::Increment { count: 2 },
                Action::Decrement { count: 7 },
            ],
        };
        assert_eq!(
            HeaderUpdate::default().merge(&mixed).unwrap(),
            HeaderUpdate::decrement(5)
        );
    }

    #[test]
    fn merge_overflow_and_apply() {
        // netting keeps accumulators small, so opposite actions never overflow
        assert_eq!(
            merge_all([
                HeaderUpdate::increment(u16::MAX),
                HeaderUpdate::decrement(u16::MAX),
                HeaderUpdate::increment(u16::MAX),
            ])
            .unwrap(),
            HeaderUpdate::increment(u16::MAX)
        );

        // same-direction overflow is rejected
        assert!(
            merge_all([
                HeaderUpdate::increment(u16::MAX),
                HeaderUpdate::increment(1)
            ])
            .is_err()
        );
        assert!(
            merge_all([
                HeaderUpdate::decrement(u16::MAX),
                HeaderUpdate::decrement(1)
            ])
            .is_err()
        );

        // merged update applied to a header
        let mut header = BlobHeader::new(BlobKind::Output, 2);
        merge_all([HeaderUpdate::increment(3), HeaderUpdate::decrement(1)])
            .unwrap()
            .apply(&mut header);
        assert_eq!(header.ref_count(), 4);

        merge_all([HeaderUpdate::decrement(10)])
            .unwrap()
            .apply(&mut header);
        assert_eq!(header.ref_count(), 0);
    }
}
