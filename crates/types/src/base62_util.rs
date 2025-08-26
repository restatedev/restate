// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Note: We use base62 [0-9a-zA-Z], that's why we use encode_alternative functions on
//! base62 crate. Otherwise, by default it uses [0-9A-Za-z].

use std::mem::size_of;

const BITS_PER_BASE62_CHAR: usize = 6;
const BITS_PER_BYTE: usize = 8;

/// Encodes a u64 into base62 string, this function pads the string with
/// trailing zeros to ensure the string is of fixed length 11.
pub fn base62_encode_fixed_width_u64(i: u64, mut f: impl std::fmt::Write) -> std::fmt::Result {
    const MAX_LENGTH: usize = base62_max_length_for_type::<u64>();

    let i = i.to_be();

    let mut buf = [b'0'; MAX_LENGTH];
    let digits =
        base62::encode_alternative_bytes(i, &mut buf).expect("a u64 must fit into 11 digits");

    if digits < MAX_LENGTH {
        // SAFETY; the array was initialised with valid utf8 and encode_alternative_bytes only writes utf8
        f.write_str(unsafe { std::str::from_utf8_unchecked(&buf[digits..]) })?;
    }

    // SAFETY; the array was initialised with valid utf8 and encode_alternative_bytes only writes utf8
    f.write_str(unsafe { std::str::from_utf8_unchecked(&buf[0..digits]) })
}

/// Encodes a u128 into base62 string, this function pads the string with
/// trailing zeros to ensure the string is of fixed length 22.
pub fn base62_encode_fixed_width_u128(i: u128, mut f: impl std::fmt::Write) -> std::fmt::Result {
    const MAX_LENGTH: usize = base62_max_length_for_type::<u128>();

    let i = i.to_be();

    let mut buf = [b'0'; MAX_LENGTH];
    let digits =
        base62::encode_alternative_bytes(i, &mut buf).expect("a u128 must fit into 22 digits");

    if digits < MAX_LENGTH {
        // SAFETY; the array was initialised with valid utf8 and encode_alternative_bytes only writes utf8
        f.write_str(unsafe { std::str::from_utf8_unchecked(&buf[digits..]) })?;
    }

    // SAFETY; the array was initialised with valid utf8 and encode_alternative_bytes only writes utf8
    f.write_str(unsafe { std::str::from_utf8_unchecked(&buf[0..digits]) })
}

/// Calculate the max number of chars needed to encode this type as base62
pub const fn base62_max_length_for_type<T>() -> usize {
    (size_of::<T>() * BITS_PER_BYTE).div_ceil(BITS_PER_BASE62_CHAR)
}
