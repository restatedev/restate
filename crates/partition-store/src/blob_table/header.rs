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
use bytes::BufMut;
use zerocopy::{IntoBytes, TryFromBytes};

use restate_storage_api::{Result, StorageError, blob_table::Header};
use restate_types::blob::BlobKind;

/// Header flags. No flags are defined yet; unknown bits are preserved on re-encode.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[repr(transparent)]
pub struct BlobFlags(u8);

/// Fields known to this version, stored at the start of the header field block.
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
struct KnownFields {
    kind: BlobKind,
    ref_count: zerocopy::big_endian::U16,
}

impl KnownFields {
    const SIZE: usize = size_of::<Self>();
}

/// Header prefixed to every encoded blob value.
///
/// On-disk layout:
///   [1 byte]  version
///   [1 byte]  flags
///   [1 byte]  fields_len (N)
///   [N bytes] field block: known fields (kind, ref_count u16 BE) followed by
///             fields unknown to this version, which are preserved as-is
///   [rest]    payload
#[derive(Debug, Clone, Copy)]
pub struct BlobHeader<'a> {
    version: u8,
    flags: BlobFlags,
    kind: BlobKind,
    ref_count: u16,
    extension: &'a [u8],
}

impl BlobHeader<'static> {
    pub fn new(kind: BlobKind, ref_count: u16) -> Self {
        Self {
            version: BlobHeader::VERSION,
            flags: BlobFlags::default(),
            kind,
            ref_count,
            extension: &[],
        }
    }
}

impl<'a> BlobHeader<'a> {
    pub const VERSION: u8 = 0;
    /// version + flags + fields_len
    const PRELUDE_LEN: usize = 3;

    /// Verifies the header version then decodes the header, returning the remaining payload.
    pub fn decode(value: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let conversion_err = |msg: String| StorageError::Conversion(anyhow!(msg));

        let Some((&[version, flags, fields_len], rest)) = value.split_first_chunk() else {
            return Err(conversion_err(format!(
                "blob header is truncated: {} bytes",
                value.len()
            )));
        };
        if version != Self::VERSION {
            return Err(conversion_err(format!(
                "unknown blob header version {version}"
            )));
        }

        let fields_len = usize::from(fields_len);
        if fields_len < KnownFields::SIZE {
            return Err(conversion_err(format!(
                "blob header fields are too short: {fields_len} bytes"
            )));
        }
        let (fields, payload) = rest.split_at_checked(fields_len).ok_or_else(|| {
            conversion_err(format!(
                "blob header fields length {fields_len} exceeds value length {}",
                value.len()
            ))
        })?;

        let (known, extension) = KnownFields::try_ref_from_prefix(fields)
            .map_err(|err| conversion_err(format!("malformed blob header: {err}")))?;

        Ok((
            Self {
                version,
                flags: BlobFlags(flags),
                kind: known.kind,
                ref_count: known.ref_count.get(),
                extension,
            },
            payload,
        ))
    }

    pub fn encoded_len(&self) -> usize {
        Self::PRELUDE_LEN + KnownFields::SIZE + self.extension.len()
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        let fields_len = u8::try_from(KnownFields::SIZE + self.extension.len())
            .expect("blob header fields fit in u8");
        buf.put_u8(self.version);
        buf.put_u8(self.flags.0);
        buf.put_u8(fields_len);
        let known = KnownFields {
            kind: self.kind,
            ref_count: self.ref_count.into(),
        };
        buf.put_slice(known.as_bytes());
        buf.put_slice(self.extension);
    }
}

impl Header for BlobHeader<'_> {
    fn kind(&self) -> BlobKind {
        self.kind
    }

    fn ref_count(&self) -> u16 {
        self.ref_count
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

    fn apply(&self, value: &mut BlobHeader<'_>) {
        for action in &self.actions {
            match action {
                Action::Unknown => {}
                Action::Increment { count } => {
                    value.ref_count = value.ref_count.saturating_add(*count)
                }
                Action::Decrement { count } => {
                    value.ref_count = value.ref_count.saturating_sub(*count)
                }
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

    pub(super) fn full_merge_inner<'a>(
        existing_val: Option<&[u8]>,
        operands: impl IntoIterator<Item = &'a [u8]>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let existing_val =
            existing_val.context("Failed to merge blob updates with a non-existent value")?;

        let (mut header, payload) =
            BlobHeader::decode(existing_val).context("Failed to decode blob header")?;

        if header.ref_count == 0 {
            // if ref_count is already 0 the blob is already
            // a tombstone that is marked for deletion and should
            // never be updated again.
            // return the header as is
            return Ok(Some(existing_val[..header.encoded_len()].to_vec()));
        }

        let mut merged = HeaderUpdate::default();
        let mut update = HeaderUpdate::default();
        for operand in operands {
            update
                .replace_from(operand)
                .context("Failed to decode blob update operand")?;

            merged = merged.merge(&update)?;
        }

        merged.apply(&mut header);

        // ref count reached zero: keep only the header to mark the object for deletion
        let payload = if header.ref_count == 0 {
            &[][..]
        } else {
            payload
        };
        let mut buf = BytesMut::with_capacity(header.encoded_len() + payload.len());
        header.encode(&mut buf);
        buf.put_slice(payload);

        Ok(Some(buf.to_vec()))
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

    #[test]
    fn full_merge_keeps_unknown_fields() {
        use bilrost::Message;

        let extension = [0xDE, 0xAD, 0xBE, 0xEF];
        let mut value = vec![BlobHeader::VERSION, 0x80, 3 + extension.len() as u8];
        value.push(BlobKind::Output as u8);
        value.extend_from_slice(&2u16.to_be_bytes());
        value.extend_from_slice(&extension);
        value.extend_from_slice(b"payload");

        let inc = HeaderUpdate::increment(3).encode_to_vec();
        let dec = HeaderUpdate::decrement(1).encode_to_vec();
        let merged = merge::full_merge_inner(Some(&value), [inc.as_slice(), dec.as_slice()])
            .unwrap()
            .unwrap();

        // only ref_count changed; flags, unknown fields and payload are preserved
        let (header, payload) = BlobHeader::decode(&merged).unwrap();
        assert_eq!(header.ref_count(), 4);
        assert_eq!(header.kind(), BlobKind::Output);
        assert_eq!(header.flags, BlobFlags(0x80));
        assert_eq!(header.extension, extension);
        assert_eq!(payload, b"payload");
        assert_eq!(merged[..5], value[..5]);
        assert_eq!(merged[6..], value[6..]);

        // reaching zero drops the payload but keeps the full header
        let dec = HeaderUpdate::decrement(10).encode_to_vec();
        let tombstone = merge::full_merge_inner(Some(&merged), [dec.as_slice()])
            .unwrap()
            .unwrap();
        let (header, payload) = BlobHeader::decode(&tombstone).unwrap();
        assert_eq!(header.ref_count(), 0);
        assert_eq!(header.flags, BlobFlags(0x80));
        assert_eq!(header.extension, extension);
        assert!(payload.is_empty());

        // a tombstone is never updated again
        let inc = HeaderUpdate::increment(1).encode_to_vec();
        assert_eq!(
            merge::full_merge_inner(Some(&tombstone), [inc.as_slice()])
                .unwrap()
                .unwrap(),
            tombstone
        );
    }
}
