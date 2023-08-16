// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use uuid::Uuid;

#[inline]
pub(crate) fn try_decode_restate_key_as_utf8(mut key_slice: &[u8]) -> Option<&str> {
    let len = prost::encoding::decode_varint(&mut key_slice).ok()?;
    if len != key_slice.len() as u64 {
        return None;
    }
    std::str::from_utf8(key_slice).ok()
}

#[inline]
pub(crate) fn try_decode_restate_key_as_int32(mut key_slice: &[u8]) -> Option<i32> {
    let value = prost::encoding::decode_varint(&mut key_slice).ok()?;
    i32::try_from(value).ok()
}

#[inline]
pub(crate) fn try_decode_restate_key_as_uuid<'a>(
    key_slice: &[u8],
    temp_buffer: &'a mut [u8],
) -> Option<&'a str> {
    if key_slice.len() != 16 {
        return None;
    }
    let uuid = Uuid::from_slice(key_slice).ok()?;
    Some(uuid.simple().encode_lower(temp_buffer))
}
