// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compile-time generated string interning for ID types.
//!
//! This module provides zero-allocation string lookups for common ID types.
//! IDs 0-1024 use compile-time generated strings, while IDs > 1024 fall back
//! to a lazily-populated cache.

use std::sync::{Mutex, OnceLock};

include!(concat!(env!("OUT_DIR"), "/short_id_strings.rs"));

/// Maximum ID with a pre-computed string (inclusive).
const MAX_PRECOMPUTED_ID: u64 = 1024;

/// Overflow storage for IDs > 1024 (extremely rare in practice).
static OVERFLOW_ID_STRINGS: OnceLock<Mutex<Vec<&'static str>>> = OnceLock::new();
static OVERFLOW_NODE_ID_STRINGS: OnceLock<Mutex<Vec<&'static str>>> = OnceLock::new();

/// Returns a static string representation of the given numeric ID.
/// Optimized for IDs 0-1024 (compile-time generated, single array lookup).
#[inline]
pub(crate) fn id_to_str(id: u64) -> &'static str {
    if id <= MAX_PRECOMPUTED_ID {
        // SAFETY: bounds check above, MAX_PRECOMPUTED_ID < SHORT_ID_STRINGS.len()
        return SHORT_ID_STRINGS[id as usize];
    }

    // Cold path: IDs > 1024 (should be extremely rare)
    overflow_id_lookup(id)
}

#[cold]
#[inline(never)]
fn overflow_id_lookup(id: u64) -> &'static str {
    let overflow = OVERFLOW_ID_STRINGS.get_or_init(|| Mutex::new(Vec::new()));
    let idx = (id - MAX_PRECOMPUTED_ID - 1) as usize;

    let mut guard = overflow.lock().unwrap();

    // Extend if necessary
    if idx >= guard.len() {
        guard.resize(idx + 1, "");
    }

    if guard[idx].is_empty() {
        // Leak the string - this only happens once per ID
        guard[idx] = id.to_string().leak();
    }

    guard[idx]
}

/// Returns a static N-prefixed string for the given node ID.
/// Optimized for IDs 0-1024 (compile-time generated, single array lookup).
#[inline]
pub(crate) fn node_id_to_str(id: u32) -> &'static str {
    if (id as u64) <= MAX_PRECOMPUTED_ID {
        // SAFETY: bounds check above
        return NODE_ID_STRINGS[id as usize];
    }

    // Cold path: IDs > 1024 (should be extremely rare)
    overflow_node_id_lookup(id)
}

#[cold]
#[inline(never)]
fn overflow_node_id_lookup(id: u32) -> &'static str {
    let overflow = OVERFLOW_NODE_ID_STRINGS.get_or_init(|| Mutex::new(Vec::new()));
    let idx = (id as u64 - MAX_PRECOMPUTED_ID - 1) as usize;

    let mut guard = overflow.lock().unwrap();

    if idx >= guard.len() {
        guard.resize(idx + 1, "");
    }

    if guard[idx].is_empty() {
        guard[idx] = format!("N{id}").leak();
    }

    guard[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_to_str() {
        // Fast path
        assert_eq!(id_to_str(0), "0");
        assert_eq!(id_to_str(1), "1");
        assert_eq!(id_to_str(1024), "1024");

        // Overflow path
        assert_eq!(id_to_str(1025), "1025");
        assert_eq!(id_to_str(2000), "2000");
    }

    #[test]
    fn test_node_id_to_str() {
        // Fast path
        assert_eq!(node_id_to_str(0), "N0");
        assert_eq!(node_id_to_str(1), "N1");
        assert_eq!(node_id_to_str(1024), "N1024");

        // Overflow path
        assert_eq!(node_id_to_str(1025), "N1025");
        assert_eq!(node_id_to_str(2000), "N2000");
    }
}
