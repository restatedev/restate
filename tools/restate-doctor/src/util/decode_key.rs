// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoding helpers for partition-store keys with custom layouts.

use restate_partition_store::stats::AggregateKeyPrefix;

/// Statistics are keyed by a replica-local index ID; the payload's meaning
/// depends on the registered definition, so it is shown raw.
pub fn decode_stat_key(key: &[u8]) -> Option<String> {
    let (prefix, kind, payload) = AggregateKeyPrefix::decode(key).ok()?;
    Some(format!(
        "partition={}, index={}, stat_kind={kind:?}, payload={payload:02x?}",
        prefix.partition_id(),
        prefix.id()
    ))
}
