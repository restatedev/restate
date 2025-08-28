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

const BASE: u64 = 62;
const BASE_TO_2: u64 = BASE * BASE;
const BASE_TO_3: u64 = BASE_TO_2 * BASE;
const BASE_TO_4: u64 = BASE_TO_3 * BASE;
const BASE_TO_5: u64 = BASE_TO_4 * BASE;
const BASE_TO_6: u64 = BASE_TO_5 * BASE;
const BASE_TO_7: u64 = BASE_TO_6 * BASE;
const BASE_TO_8: u64 = BASE_TO_7 * BASE;
const BASE_TO_9: u64 = BASE_TO_8 * BASE;
const BASE_TO_10: u128 = (BASE_TO_9 * BASE) as u128;
const BASE_TO_11: u128 = BASE_TO_10 * BASE as u128;
const BASE_TO_12: u128 = BASE_TO_11 * BASE as u128;
const BASE_TO_13: u128 = BASE_TO_12 * BASE as u128;
const BASE_TO_14: u128 = BASE_TO_13 * BASE as u128;
const BASE_TO_15: u128 = BASE_TO_14 * BASE as u128;
const BASE_TO_16: u128 = BASE_TO_15 * BASE as u128;
const BASE_TO_17: u128 = BASE_TO_16 * BASE as u128;
const BASE_TO_18: u128 = BASE_TO_17 * BASE as u128;
const BASE_TO_19: u128 = BASE_TO_18 * BASE as u128;
const BASE_TO_20: u128 = BASE_TO_19 * BASE as u128;
const BASE_TO_21: u128 = BASE_TO_20 * BASE as u128;

fn digit_count(n: u128) -> usize {
    const POWERS: [u128; 22] = [
        0,
        BASE as u128,
        BASE_TO_2 as u128,
        BASE_TO_3 as u128,
        BASE_TO_4 as u128,
        BASE_TO_5 as u128,
        BASE_TO_6 as u128,
        BASE_TO_7 as u128,
        BASE_TO_8 as u128,
        BASE_TO_9 as u128,
        BASE_TO_10,
        BASE_TO_11,
        BASE_TO_12,
        BASE_TO_13,
        BASE_TO_14,
        BASE_TO_15,
        BASE_TO_16,
        BASE_TO_17,
        BASE_TO_18,
        BASE_TO_19,
        BASE_TO_20,
        BASE_TO_21,
    ];

    match POWERS.binary_search(&n) {
        Ok(n) => n.wrapping_add(1),
        Err(n) => n,
    }
}

/// Encodes a u64 into base62 string, this function offsets the string with
/// enough bytes (assuming the input buffer is zeroed with b'0') to ensure the string is of fixed length 11.
pub fn base62_encode_fixed_width_u64(i: u64, out: &mut [u8]) -> usize {
    const MAX_LENGTH: usize = base62_max_length_for_type::<u64>();

    let i = i.to_be();

    let digits = digit_count(i.into());
    let offset = MAX_LENGTH - digits;

    let digits2 = base62::encode_alternative_bytes(i, &mut out[offset..offset + digits])
        .expect("a u64 must fit into 11 digits");
    debug_assert_eq!(digits, digits2);

    MAX_LENGTH
}

/// Encodes a u128 into base62 string, this function offsets the string with
/// enough bytes (assuming the input buffer is zeroed) to ensure the string is of fixed length 22.
pub fn base62_encode_fixed_width_u128(i: u128, out: &mut [u8]) -> usize {
    const MAX_LENGTH: usize = base62_max_length_for_type::<u128>();

    let i = i.to_be();
    let digits = digit_count(i);
    let offset = MAX_LENGTH - digits;

    let digits2 = base62::encode_alternative_bytes(i, &mut out[offset..offset + digits])
        .expect("a u128 must fit into 22 digits");
    debug_assert_eq!(digits, digits2);

    MAX_LENGTH
}

/// Calculate the max number of chars needed to encode this type as base62
pub const fn base62_max_length_for_type<T>() -> usize {
    (size_of::<T>() * BITS_PER_BYTE).div_ceil(BITS_PER_BASE62_CHAR)
}
