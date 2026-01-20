// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod colorize;
pub mod colorize_id;
pub mod decode_value;
pub mod rocksdb;

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Encode bytes as lowercase hex string
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        result.push(HEX_CHARS[(b >> 4) as usize] as char);
        result.push(HEX_CHARS[(b & 0xf) as usize] as char);
    }
    result
}
