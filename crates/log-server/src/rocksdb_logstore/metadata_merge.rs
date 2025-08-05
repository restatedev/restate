// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::MergeOperands;
use tracing::{error, trace};

use restate_types::logs::{LogletOffset, SequenceNumber};

use crate::rocksdb_logstore::keys::{KeyPrefixKind, MetadataKey};

/// The merge operator for the metadata column family.
///
/// This merges some metadata updates to ensure that trimpoints can be processed out of order but
/// it strictly moves forward on the storage layer.
pub(super) fn metadata_full_merge(
    mut key_buf: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let key = MetadataKey::from_slice(&mut key_buf);
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
    Some(current_trim_point.to_binary_array().into())
}

pub(super) fn metadata_partial_merge(
    key: &[u8],
    _unused: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    metadata_full_merge(key, None, operands)
}
