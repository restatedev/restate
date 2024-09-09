// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BytesMut;
use restate_types::logs::{LogletOffset, SequenceNumber};
use rocksdb::MergeOperands;
use tracing::{error, trace};

use crate::rocksdb_logstore::keys::{KeyPrefixKind, MetadataKey};

pub(super) fn metadata_full_merge(
    key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let key = MetadataKey::from_slice(key);
    trace!(key = ?key, "metadata_full_merge");
    if key.kind() != KeyPrefixKind::TrimPoint {
        error!(key = ?key, "Merge is only supported for trim-points");
        return None;
    }

    let mut current_trim_point = existing_val
        .map(LogletOffset::decode)
        .unwrap_or(LogletOffset::INVALID);

    for op in operands {
        let updated_trim_point = LogletOffset::decode(op);
        // trim point can only move forward
        current_trim_point = updated_trim_point.max(current_trim_point);
    }
    let mut buf = BytesMut::with_capacity(LogletOffset::estimated_encode_size());
    current_trim_point.encode(&mut buf);
    Some(buf.into())
}

pub(super) fn metadata_partial_merge(
    key: &[u8],
    _unused: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    metadata_full_merge(key, None, operands)
}
