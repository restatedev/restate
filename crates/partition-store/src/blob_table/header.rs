// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use bytes::BufMut;
use tracing::warn;
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

/// A ref count update that summarizes any sequence of increments and decrements.
///
/// The sequence is seen as a walk starting at 0: `delta` is where the walk ends and `floor`
/// (always <= 0) is its lowest point. Applied to a ref count `x > 0`, the count passes through
/// zero iff `x + floor <= 0`, in which case the blob becomes a tombstone that is never updated
/// again. Composing updates preserves this, so the merge result does not depend on how RocksDB
/// groups operands into full and partial merges.
///
/// The one exception is clamping the stored ref count at `u16::MAX`, which requires more than
/// `u16::MAX` live references.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, bilrost::Message)]
pub(super) struct HeaderUpdate {
    #[bilrost(tag(1))]
    delta: i64,
    #[bilrost(tag(2))]
    floor: i64,
}

impl HeaderUpdate {
    pub fn increment(count: u16) -> Self {
        Self {
            delta: count.into(),
            floor: 0,
        }
    }

    pub fn decrement(count: u16) -> Self {
        let delta = -i64::from(count);
        Self {
            delta,
            floor: delta,
        }
    }

    /// Composes `self` followed by `next`.
    fn merge(self, next: &HeaderUpdate) -> Self {
        Self {
            delta: self.delta.saturating_add(next.delta),
            floor: self.floor.min(self.delta.saturating_add(next.floor)),
        }
    }

    fn apply(&self, header: &mut BlobHeader<'_>) {
        let ref_count = i64::from(header.ref_count);
        if ref_count == 0 {
            // tombstone, never updated again
            return;
        }
        if ref_count.saturating_add(self.floor) <= 0 {
            header.ref_count = 0;
            return;
        }

        let ref_count = ref_count.saturating_add(self.delta);
        if ref_count > i64::from(u16::MAX) {
            warn!(
                "blob ref count {ref_count} exceeds the maximum, clamping to {}",
                u16::MAX
            );
        }
        header.ref_count = ref_count.clamp(1, u16::MAX.into()) as u16;
    }
}

pub mod merge {
    use anyhow::Context;
    use bilrost::{Message, OwnedMessage};
    use bytes::{BufMut, BytesMut};
    use restate_types::blob::BlobKind;
    use rocksdb::{CompactionDecision, MergeOperands};
    use tracing::warn;

    use crate::{
        blob_table::{BlobHeader, BlobKey, HeaderUpdate},
        keys::DecodeTableKey,
    };

    /// Errors (and thus RocksDB merge failures) are only possible on corrupted values or
    /// operands. A missing base value or any sequence of updates always merges successfully.
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
        let Some(existing_val) = existing_val else {
            // Avoid returning None here if we hit a non existing record to
            // avoid rocksdb going into read-only mode.
            let empty_header = BlobHeader::new(BlobKind::Generic, 0);
            let mut buf = BytesMut::with_capacity(empty_header.encoded_len());
            empty_header.encode(&mut buf);
            return Ok(Some(buf.to_vec()));
        };

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

            merged = merged.merge(&update);
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

    pub(super) fn partial_merge_inner<'a>(
        operands: impl IntoIterator<Item = &'a [u8]>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let mut merged = HeaderUpdate::default();
        let mut update = HeaderUpdate::default();
        for operand in operands {
            update
                .replace_from(operand)
                .context("Failed to decode blob update operand")?;

            merged = merged.merge(&update);
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
    use bilrost::Message;

    use super::*;

    fn merge_all(updates: impl IntoIterator<Item = HeaderUpdate>) -> HeaderUpdate {
        updates
            .into_iter()
            .fold(HeaderUpdate::default(), |merged, update| {
                merged.merge(&update)
            })
    }

    fn blob_value(ref_count: u16) -> Vec<u8> {
        let mut value = Vec::new();
        BlobHeader::new(BlobKind::Output, ref_count).encode(&mut value);
        value.extend_from_slice(b"payload");
        value
    }

    fn ref_count(value: &[u8]) -> u16 {
        BlobHeader::decode(value).unwrap().0.ref_count()
    }

    #[test]
    fn merge_composes_updates() {
        let (inc, dec) = (HeaderUpdate::increment, HeaderUpdate::decrement);

        // same-direction updates are summed, opposite ones net out
        assert_eq!(merge_all([inc(1), inc(2)]), inc(3));
        assert_eq!(merge_all([dec(1), dec(2)]), dec(3));
        assert_eq!(merge_all([inc(5), dec(2)]), inc(3));
        assert_eq!(merge_all([inc(2), dec(5)]), dec(3));
        // but passing through a lower point is remembered
        assert_eq!(
            merge_all([dec(1), inc(1)]),
            HeaderUpdate {
                delta: 0,
                floor: -1
            }
        );
        // accumulating many updates never fails
        assert_eq!(
            merge_all([inc(u16::MAX); 10]).delta,
            10 * i64::from(u16::MAX)
        );

        // merged update applied to a header
        let mut header = BlobHeader::new(BlobKind::Output, 2);
        merge_all([inc(3), dec(1)]).apply(&mut header);
        assert_eq!(header.ref_count(), 4);
        inc(u16::MAX).apply(&mut header);
        assert_eq!(header.ref_count(), u16::MAX);
        dec(u16::MAX).apply(&mut header);
        assert_eq!(header.ref_count(), 0);
        // a tombstone is never revived
        inc(1).apply(&mut header);
        assert_eq!(header.ref_count(), 0);
    }

    #[test]
    fn merge_is_independent_of_grouping() {
        let (inc, dec) = (HeaderUpdate::increment, HeaderUpdate::decrement);
        let cases: [(u16, &[HeaderUpdate], u16); 4] = [
            (1, &[dec(1), inc(1)], 0),
            (2, &[dec(1), inc(1)], 2),
            (3, &[dec(2), inc(5), dec(4)], 2),
            (2, &[dec(2), inc(5), dec(4)], 0),
        ];

        for (base, ops, expected) in cases {
            let ops: Vec<_> = ops.iter().map(Message::encode_to_vec).collect();

            // full merges of `chunk` operands each, from one at a time to all at once
            for chunk in 1..=ops.len() {
                let mut value = blob_value(base);
                for chunk in ops.chunks(chunk) {
                    value = merge::full_merge_inner(Some(&value), chunk.iter().map(Vec::as_slice))
                        .unwrap()
                        .unwrap();
                }
                assert_eq!(ref_count(&value), expected, "base {base}, chunk {chunk}");
            }

            // all operands combined by a partial merge first
            let combined = merge::partial_merge_inner(ops.iter().map(Vec::as_slice))
                .unwrap()
                .unwrap();
            let value = merge::full_merge_inner(Some(&blob_value(base)), [combined.as_slice()])
                .unwrap()
                .unwrap();
            assert_eq!(ref_count(&value), expected, "base {base}, partial merge");
        }

        // a missing base value (already collected) merges into a tombstone
        let inc = inc(1).encode_to_vec();
        let value = merge::full_merge_inner(None, [inc.as_slice()])
            .unwrap()
            .unwrap();
        assert_eq!(ref_count(&value), 0);
    }

    #[test]
    fn full_merge_keeps_unknown_fields() {
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
