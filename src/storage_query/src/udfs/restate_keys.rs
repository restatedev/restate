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
