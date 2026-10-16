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

use restate_partition_store::stats::StatKeyPrefix;

use super::hex_encode;

/// Decodes an aggregated statistic key, including known statistic-specific payloads.
pub fn decode_aggregated_stat_key(key: &[u8]) -> Option<String> {
    let (prefix, payload) = StatKeyPrefix::decode_prefix(key).ok()?;
    let stat_id = prefix.stat_id_raw();
    let details = match prefix.stat_id() {
        None => format!("payload=0x{}", hex_encode(payload.remaining)),
    };

    Some(format!(
        "stat_kind={:?}, stat_id={}, partition_id={}, {details}",
        prefix.stat_kind(),
        stat_id,
        prefix.partition_id()
    ))
}
