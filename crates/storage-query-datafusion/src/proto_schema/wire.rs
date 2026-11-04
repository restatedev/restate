// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Zero-allocation binary Protobuf wire-format scanner and dynamic JSON transcoder.

use std::borrow::Cow;

use serde_json::{Map, Number, Value};

use super::{CompiledDescriptorPool, CompiledField, CompiledMessageSchema, FieldKind, PathStep};

const WIRE_VARINT: u8 = 0;
const WIRE_I64: u8 = 1;
const WIRE_LEN: u8 = 2;
const WIRE_I32: u8 = 5;

#[inline]
pub fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    if *pos < buf.len() {
        let b0 = buf[*pos];
        if (b0 & 0x80) == 0 {
            *pos += 1;
            return Some(u64::from(b0));
        }
    }
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    while *pos < buf.len() {
        let byte = buf[*pos];
        *pos += 1;
        result |= u64::from(byte & 0x7F) << shift;
        if (byte & 0x80) == 0 {
            return Some(result);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

#[inline]
pub fn skip_wire_value(buf: &[u8], pos: &mut usize, wire_type: u8) -> bool {
    match wire_type {
        WIRE_VARINT => read_varint(buf, pos).is_some(),
        WIRE_I64 if *pos + 8 <= buf.len() => {
            *pos += 8;
            true
        }
        WIRE_LEN => {
            if let Some(len) = read_varint(buf, pos) {
                let len = len as usize;
                if *pos + len <= buf.len() {
                    *pos += len;
                    return true;
                }
            }
            false
        }
        WIRE_I32 if *pos + 4 <= buf.len() => {
            *pos += 4;
            true
        }
        _ => false,
    }
}

#[inline]
fn decode_zigzag64(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

#[inline]
fn decode_zigzag32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

/// Navigate intermediate submessage steps and return the slice containing the leaf message.
#[inline]
fn resolve_parent_slice<'a>(mut buf: &'a [u8], steps: &[PathStep]) -> Option<&'a [u8]> {
    if steps.is_empty() {
        return None;
    }
    for step in &steps[..steps.len() - 1] {
        let PathStep::SubMessage { tag } = step else {
            return None;
        };
        let mut pos = 0;
        let mut found_slice: Option<&'a [u8]> = None;
        while pos < buf.len() {
            let key = read_varint(buf, &mut pos)?;
            let field_num = (key >> 3) as u32;
            let wire_type = (key & 0x07) as u8;
            if field_num == *tag && wire_type == WIRE_LEN {
                let len = read_varint(buf, &mut pos)? as usize;
                if pos + len > buf.len() {
                    return None;
                }
                found_slice = Some(&buf[pos..pos + len]);
                break;
            } else if !skip_wire_value(buf, &mut pos, wire_type) {
                return None;
            }
        }
        buf = found_slice?;
    }
    Some(buf)
}

/// Extract a string (or stringified scalar/enum/map-value) using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_str<'a>(
    raw: &'a [u8],
    steps: &[PathStep],
    pool: &CompiledDescriptorPool,
) -> Option<Cow<'a, str>> {
    let buf = resolve_parent_slice(raw, steps)?;
    let last = steps.last()?;

    match last {
        PathStep::Field { tag, kind } => {
            let mut pos = 0;
            while pos < buf.len() {
                let key = read_varint(buf, &mut pos)?;
                let field_num = (key >> 3) as u32;
                let wire_type = (key & 0x07) as u8;
                if field_num == *tag {
                    return match (kind, wire_type) {
                        (FieldKind::String, WIRE_LEN) => {
                            let len = read_varint(buf, &mut pos)? as usize;
                            if pos + len > buf.len() {
                                return None;
                            }
                            let s = std::str::from_utf8(&buf[pos..pos + len]).ok()?;
                            Some(Cow::Borrowed(s))
                        }
                        (FieldKind::Enum(enum_name), WIRE_VARINT) => {
                            let v = read_varint(buf, &mut pos)? as i32;
                            if let Some(name) = pool.resolve_enum_name(enum_name, v) {
                                Some(Cow::Owned(name.to_string()))
                            } else {
                                Some(Cow::Owned(v.to_string()))
                            }
                        }
                        (_, WIRE_VARINT) => {
                            let v = read_varint(buf, &mut pos)?;
                            let formatted = match kind {
                                FieldKind::Bool => (v != 0).to_string(),
                                FieldKind::Sint32 => decode_zigzag32(v as u32).to_string(),
                                FieldKind::Sint64 => decode_zigzag64(v).to_string(),
                                FieldKind::Int32 | FieldKind::Int64 => (v as i64).to_string(),
                                _ => v.to_string(),
                            };
                            Some(Cow::Owned(formatted))
                        }
                        _ => None,
                    };
                } else if !skip_wire_value(buf, &mut pos, wire_type) {
                    return None;
                }
            }
            None
        }
        PathStep::MapLookup {
            tag,
            key_str,
            value_kind,
        } => {
            let mut pos = 0;
            while pos < buf.len() {
                let key = read_varint(buf, &mut pos)?;
                let field_num = (key >> 3) as u32;
                let wire_type = (key & 0x07) as u8;
                if field_num == *tag && wire_type == WIRE_LEN {
                    let len = read_varint(buf, &mut pos)? as usize;
                    if pos + len > buf.len() {
                        return None;
                    }
                    let entry_buf = &buf[pos..pos + len];
                    pos += len;
                    if let Some(v) = extract_map_entry_str(entry_buf, key_str, value_kind) {
                        return Some(v);
                    }
                } else if !skip_wire_value(buf, &mut pos, wire_type) {
                    return None;
                }
            }
            None
        }
        PathStep::SubMessage { .. } => None,
    }
}

#[inline]
fn extract_map_entry_str<'a>(
    entry_buf: &'a [u8],
    expected_key: &str,
    value_kind: &FieldKind,
) -> Option<Cow<'a, str>> {
    let mut pos = 0;
    let mut key_matched = false;
    let mut val_out: Option<Cow<'a, str>> = None;

    while pos < entry_buf.len() {
        let tag_wire = read_varint(entry_buf, &mut pos)?;
        let fnum = (tag_wire >> 3) as u32;
        let wtype = (tag_wire & 0x07) as u8;
        if fnum == 1 && wtype == WIRE_LEN {
            let klen = read_varint(entry_buf, &mut pos)? as usize;
            if pos + klen > entry_buf.len() {
                return None;
            }
            let k_bytes = &entry_buf[pos..pos + klen];
            pos += klen;
            if k_bytes != expected_key.as_bytes() {
                return None;
            }
            key_matched = true;
        } else if fnum == 2 {
            match (value_kind, wtype) {
                (FieldKind::String, WIRE_LEN) => {
                    let vlen = read_varint(entry_buf, &mut pos)? as usize;
                    if pos + vlen > entry_buf.len() {
                        return None;
                    }
                    let s = std::str::from_utf8(&entry_buf[pos..pos + vlen]).ok()?;
                    val_out = Some(Cow::Borrowed(s));
                    pos += vlen;
                }
                (_, WIRE_VARINT) => {
                    let v = read_varint(entry_buf, &mut pos)?;
                    val_out = Some(Cow::Owned((v as i64).to_string()));
                }
                _ => {
                    if !skip_wire_value(entry_buf, &mut pos, wtype) {
                        return None;
                    }
                }
            }
        } else if !skip_wire_value(entry_buf, &mut pos, wtype) {
            return None;
        }
    }

    if key_matched { val_out } else { None }
}

/// Extract an integer (`i64`) field using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_i64(raw: &[u8], steps: &[PathStep]) -> Option<i64> {
    let buf = resolve_parent_slice(raw, steps)?;
    let last = steps.last()?;

    match last {
        PathStep::Field { tag, kind } => {
            let mut pos = 0;
            while pos < buf.len() {
                let key = read_varint(buf, &mut pos)?;
                let field_num = (key >> 3) as u32;
                let wire_type = (key & 0x07) as u8;
                if field_num == *tag {
                    return match (kind, wire_type) {
                        (FieldKind::Sint32, WIRE_VARINT) => {
                            let v = read_varint(buf, &mut pos)? as u32;
                            Some(i64::from(decode_zigzag32(v)))
                        }
                        (FieldKind::Sint64, WIRE_VARINT) => {
                            let v = read_varint(buf, &mut pos)?;
                            Some(decode_zigzag64(v))
                        }
                        (FieldKind::Int32, WIRE_VARINT) => {
                            let v = read_varint(buf, &mut pos)? as i32;
                            Some(i64::from(v))
                        }
                        (_, WIRE_VARINT) => {
                            let v = read_varint(buf, &mut pos)?;
                            Some(v as i64)
                        }
                        (FieldKind::Fixed64 | FieldKind::Sfixed64, WIRE_I64) => {
                            if pos + 8 > buf.len() {
                                return None;
                            }
                            let bytes: [u8; 8] = buf[pos..pos + 8].try_into().ok()?;
                            Some(i64::from_le_bytes(bytes))
                        }
                        (FieldKind::Fixed32 | FieldKind::Sfixed32, WIRE_I32) => {
                            if pos + 4 > buf.len() {
                                return None;
                            }
                            let bytes: [u8; 4] = buf[pos..pos + 4].try_into().ok()?;
                            Some(if matches!(kind, FieldKind::Sfixed32) {
                                i64::from(i32::from_le_bytes(bytes))
                            } else {
                                i64::from(u32::from_le_bytes(bytes))
                            })
                        }
                        _ => None,
                    };
                } else if !skip_wire_value(buf, &mut pos, wire_type) {
                    return None;
                }
            }
            Some(0)
        }
        _ => None,
    }
}

/// Extract a floating-point (`f64`) field using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_f64(raw: &[u8], steps: &[PathStep]) -> Option<f64> {
    let buf = resolve_parent_slice(raw, steps)?;
    let PathStep::Field { tag, kind } = steps.last()? else {
        return None;
    };
    let mut pos = 0;
    while pos < buf.len() {
        let key = read_varint(buf, &mut pos)?;
        let field_num = (key >> 3) as u32;
        let wire_type = (key & 0x07) as u8;
        if field_num == *tag {
            return match (kind, wire_type) {
                (FieldKind::Double, WIRE_I64) => {
                    if pos + 8 > buf.len() {
                        return None;
                    }
                    let bytes: [u8; 8] = buf[pos..pos + 8].try_into().ok()?;
                    Some(f64::from_le_bytes(bytes))
                }
                (FieldKind::Float, WIRE_I32) => {
                    if pos + 4 > buf.len() {
                        return None;
                    }
                    let bytes: [u8; 4] = buf[pos..pos + 4].try_into().ok()?;
                    Some(f64::from(f32::from_le_bytes(bytes)))
                }
                (_, WIRE_VARINT) => {
                    let v = read_varint(buf, &mut pos)?;
                    Some(v as i64 as f64)
                }
                _ => None,
            };
        } else if !skip_wire_value(buf, &mut pos, wire_type) {
            return None;
        }
    }
    Some(0.0)
}

/// Extract a boolean field using zero-allocation wire-tag skipping.
#[inline]
pub fn extract_bool(raw: &[u8], steps: &[PathStep]) -> Option<bool> {
    let buf = resolve_parent_slice(raw, steps)?;
    let PathStep::Field { tag, .. } = steps.last()? else {
        return None;
    };
    let mut pos = 0;
    while pos < buf.len() {
        let key = read_varint(buf, &mut pos)?;
        let field_num = (key >> 3) as u32;
        let wire_type = (key & 0x07) as u8;
        if field_num == *tag && wire_type == WIRE_VARINT {
            let v = read_varint(buf, &mut pos)?;
            return Some(v != 0);
        } else if !skip_wire_value(buf, &mut pos, wire_type) {
            return None;
        }
    }
    Some(false)
}

/// Dynamically transcode a binary Protobuf payload into a `serde_json::Value` using its descriptor.
pub fn transcode_to_json(
    buf: &[u8],
    msg: &CompiledMessageSchema,
    pool: &CompiledDescriptorPool,
) -> Option<Value> {
    let mut map = Map::with_capacity(msg.fields_by_tag.len());
    let mut pos = 0;

    while pos < buf.len() {
        let key = read_varint(buf, &mut pos)?;
        let field_num = (key >> 3) as u32;
        let wire_type = (key & 0x07) as u8;

        let Some(field) = msg.fields_by_tag.get(&field_num) else {
            if !skip_wire_value(buf, &mut pos, wire_type) {
                return None;
            }
            continue;
        };

        if let Some((_k_kind, v_kind)) = &field.map_entry {
            if wire_type != WIRE_LEN {
                return None;
            }
            let len = read_varint(buf, &mut pos)? as usize;
            if pos + len > buf.len() {
                return None;
            }
            let entry_buf = &buf[pos..pos + len];
            pos += len;
            if let Some((k_str, v_val)) = decode_map_entry_json(entry_buf, v_kind, pool) {
                let obj = map
                    .entry(field.name.clone())
                    .or_insert_with(|| Value::Object(Map::new()));
                if let Value::Object(inner) = obj {
                    inner.insert(k_str, v_val);
                }
            }
            continue;
        }

        let decoded_val = decode_single_field_json(buf, &mut pos, wire_type, field, pool)?;
        if field.is_repeated {
            let arr = map
                .entry(field.name.clone())
                .or_insert_with(|| Value::Array(Vec::new()));
            if let Value::Array(vec) = arr {
                vec.push(decoded_val);
            }
        } else {
            map.insert(field.name.clone(), decoded_val);
        }
    }

    Some(Value::Object(map))
}

fn decode_map_entry_json(
    entry_buf: &[u8],
    value_kind: &FieldKind,
    pool: &CompiledDescriptorPool,
) -> Option<(String, Value)> {
    let mut pos = 0;
    let mut key_str = String::new();
    let mut val_json = Value::Null;

    let dummy_field = CompiledField {
        name: String::new(),
        number: 2,
        kind: value_kind.clone(),
        is_repeated: false,
        map_entry: None,
    };

    while pos < entry_buf.len() {
        let tag_wire = read_varint(entry_buf, &mut pos)?;
        let fnum = (tag_wire >> 3) as u32;
        let wtype = (tag_wire & 0x07) as u8;
        if fnum == 1 && wtype == WIRE_LEN {
            let klen = read_varint(entry_buf, &mut pos)? as usize;
            if pos + klen > entry_buf.len() {
                return None;
            }
            key_str = std::str::from_utf8(&entry_buf[pos..pos + klen])
                .ok()?
                .to_string();
            pos += klen;
        } else if fnum == 2 {
            val_json = decode_single_field_json(entry_buf, &mut pos, wtype, &dummy_field, pool)?;
        } else if !skip_wire_value(entry_buf, &mut pos, wtype) {
            return None;
        }
    }
    Some((key_str, val_json))
}

fn decode_single_field_json(
    buf: &[u8],
    pos: &mut usize,
    wire_type: u8,
    field: &CompiledField,
    pool: &CompiledDescriptorPool,
) -> Option<Value> {
    match (&field.kind, wire_type) {
        (FieldKind::String, WIRE_LEN) => {
            let len = read_varint(buf, pos)? as usize;
            if *pos + len > buf.len() {
                return None;
            }
            let s = std::str::from_utf8(&buf[*pos..*pos + len]).ok()?;
            *pos += len;
            Some(Value::String(s.to_string()))
        }
        (FieldKind::Bytes, WIRE_LEN) => {
            let len = read_varint(buf, pos)? as usize;
            if *pos + len > buf.len() {
                return None;
            }
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(&buf[*pos..*pos + len]);
            *pos += len;
            Some(Value::String(b64))
        }
        (FieldKind::Message(msg_name), WIRE_LEN) => {
            let len = read_varint(buf, pos)? as usize;
            if *pos + len > buf.len() {
                return None;
            }
            let sub_buf = &buf[*pos..*pos + len];
            *pos += len;
            let sub_schema = pool.resolve_message(msg_name)?;
            transcode_to_json(sub_buf, sub_schema, pool)
        }
        (FieldKind::Bool, WIRE_VARINT) => {
            let v = read_varint(buf, pos)?;
            Some(Value::Bool(v != 0))
        }
        (FieldKind::Int32, WIRE_VARINT) => {
            let v = read_varint(buf, pos)? as i32;
            Some(Value::Number(Number::from(v)))
        }
        (FieldKind::Int64, WIRE_VARINT) => {
            let v = read_varint(buf, pos)? as i64;
            Some(Value::Number(Number::from(v)))
        }
        (FieldKind::Uint32 | FieldKind::Uint64, WIRE_VARINT) => {
            let v = read_varint(buf, pos)?;
            Some(Value::Number(Number::from(v)))
        }
        (FieldKind::Sint32, WIRE_VARINT) => {
            let v = read_varint(buf, pos)? as u32;
            Some(Value::Number(Number::from(decode_zigzag32(v))))
        }
        (FieldKind::Sint64, WIRE_VARINT) => {
            let v = read_varint(buf, pos)?;
            Some(Value::Number(Number::from(decode_zigzag64(v))))
        }
        (FieldKind::Enum(enum_name), WIRE_VARINT) => {
            let v = read_varint(buf, pos)? as i32;
            if let Some(name) = pool.resolve_enum_name(enum_name, v) {
                Some(Value::String(name.to_string()))
            } else {
                Some(Value::Number(Number::from(v)))
            }
        }
        (FieldKind::Double, WIRE_I64) => {
            if *pos + 8 > buf.len() {
                return None;
            }
            let bytes: [u8; 8] = buf[*pos..*pos + 8].try_into().ok()?;
            *pos += 8;
            Number::from_f64(f64::from_le_bytes(bytes)).map(Value::Number)
        }
        (FieldKind::Float, WIRE_I32) => {
            if *pos + 4 > buf.len() {
                return None;
            }
            let bytes: [u8; 4] = buf[*pos..*pos + 4].try_into().ok()?;
            *pos += 4;
            Number::from_f64(f64::from(f32::from_le_bytes(bytes))).map(Value::Number)
        }
        (FieldKind::Fixed64, WIRE_I64) => {
            if *pos + 8 > buf.len() {
                return None;
            }
            let bytes: [u8; 8] = buf[*pos..*pos + 8].try_into().ok()?;
            *pos += 8;
            Some(Value::Number(Number::from(u64::from_le_bytes(bytes))))
        }
        (FieldKind::Sfixed64, WIRE_I64) => {
            if *pos + 8 > buf.len() {
                return None;
            }
            let bytes: [u8; 8] = buf[*pos..*pos + 8].try_into().ok()?;
            *pos += 8;
            Some(Value::Number(Number::from(i64::from_le_bytes(bytes))))
        }
        (FieldKind::Fixed32, WIRE_I32) => {
            if *pos + 4 > buf.len() {
                return None;
            }
            let bytes: [u8; 4] = buf[*pos..*pos + 4].try_into().ok()?;
            *pos += 4;
            Some(Value::Number(Number::from(u32::from_le_bytes(bytes))))
        }
        (FieldKind::Sfixed32, WIRE_I32) => {
            if *pos + 4 > buf.len() {
                return None;
            }
            let bytes: [u8; 4] = buf[*pos..*pos + 4].try_into().ok()?;
            *pos += 4;
            Some(Value::Number(Number::from(i32::from_le_bytes(bytes))))
        }
        _ => {
            skip_wire_value(buf, pos, wire_type);
            Some(Value::Null)
        }
    }
}
