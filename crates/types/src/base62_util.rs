// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use std::{mem::size_of, num::NonZeroU128};

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
    const THRESHOLDS: [u128; 23] = [
        0,
        BASE as u128 - 1,
        BASE_TO_2 as u128 - 1,
        BASE_TO_3 as u128 - 1,
        BASE_TO_4 as u128 - 1,
        BASE_TO_5 as u128 - 1,
        BASE_TO_6 as u128 - 1,
        BASE_TO_7 as u128 - 1,
        BASE_TO_8 as u128 - 1,
        BASE_TO_9 as u128 - 1,
        BASE_TO_10 - 1,
        BASE_TO_11 - 1,
        BASE_TO_12 - 1,
        BASE_TO_13 - 1,
        BASE_TO_14 - 1,
        BASE_TO_15 - 1,
        BASE_TO_16 - 1,
        BASE_TO_17 - 1,
        BASE_TO_18 - 1,
        BASE_TO_19 - 1,
        BASE_TO_20 - 1,
        BASE_TO_21 - 1,
        u128::MAX, // sentinel, u128 cannot be larger than this value
    ];

    let Some(n) = NonZeroU128::new(n) else {
        return 1;
    };
    // We want to find floor(log62(n)) + 1 = floor(log2(n) / log2(62)) + 1
    // First, approximate log2(n) with ilog2 = floor(log2(n)), underestimating by 0 <= err < 1
    let ilog2 = n.ilog2() as usize;

    // Next, we find floor(ilog2/log2(62)) + 1, which is exactly equal to floor(ilog2 * 43/256) + 1 for all ilog2 in [0, 127]
    // The result is an underestimate by up to 1, purely because ilog2 is an underestimate
    let estimate = ((ilog2 * 43) >> 8) + 1;

    // SAFETY: estimate is in [1,22] since ilog2 is in [0,127] and (127*43)>>8 + 1 = 22
    let threshold = unsafe { *THRESHOLDS.get_unchecked(estimate) };
    let bump = (n.get() > threshold) as usize;
    estimate + bump
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
