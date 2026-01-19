// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Colorized hex display for partition-store keys.
//!
//! This module provides utilities to display key bytes with color-coding
//! to help visualize the key structure. Each part of the key is colored
//! differently to make it easier to understand the byte layout.

use std::fmt::Write;

use crossterm::style::{Color, Stylize};
use restate_cli_util::CliContext;
use restate_partition_store::keys::KeyKind;

use super::HEX_CHARS;

/// Color scheme for different key segments.
/// Uses distinct colors that work well on both light and dark terminals.
#[derive(Debug, Clone, Copy)]
pub enum KeySegment {
    /// KeyKind prefix (2 bytes) - always first
    Kind,
    /// Partition key or partition id (8 bytes) - always second
    Partition,
    /// Fixed-size fields like InvocationUuid (16 bytes), u32, u64
    FixedField,
    /// Variable-length fields (service_name, service_key, etc.)
    VariableField,
    /// Length prefix for variable-length fields
    LengthPrefix,
}

impl KeySegment {
    fn color(self) -> Color {
        match self {
            KeySegment::Kind => Color::Cyan,
            KeySegment::Partition => Color::Yellow,
            KeySegment::FixedField => Color::Green,
            KeySegment::VariableField => Color::Magenta,
            KeySegment::LengthPrefix => Color::DarkGrey,
        }
    }
}

/// A segment of bytes with its semantic meaning
#[derive(Debug)]
struct Segment {
    kind: KeySegment,
    start: usize,
    len: usize,
    /// Label for future debugging/verbose output (currently unused)
    #[allow(dead_code)]
    label: &'static str,
}

/// Build segments for a key based on its KeyKind.
/// Returns the list of segments describing the key structure.
fn build_segments(key: &[u8]) -> Vec<Segment> {
    let mut segments = Vec::new();

    if key.len() < 2 {
        return segments;
    }

    // KeyKind is always first 2 bytes
    segments.push(Segment {
        kind: KeySegment::Kind,
        start: 0,
        len: 2,
        label: "kind",
    });

    let key_kind = match KeyKind::from_bytes(key[..2].try_into().unwrap()) {
        Some(k) => k,
        None => return segments, // Unknown key type, only show kind
    };

    if key.len() < 10 {
        return segments;
    }

    // Partition key/id is always next 8 bytes
    segments.push(Segment {
        kind: KeySegment::Partition,
        start: 2,
        len: 8,
        label: "partition",
    });

    // Now handle table-specific fields starting at byte 10
    let remaining = key.len() - 10;
    if remaining == 0 {
        return segments;
    }

    match key_kind {
        // Fixed-size keys after partition
        #[allow(deprecated)]
        KeyKind::InvocationStatus | KeyKind::InvocationStatusV1 => {
            // InvocationUuid (16 bytes)
            if remaining >= 16 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 16,
                    label: "invocation_uuid",
                });
            }
        }
        KeyKind::Journal | KeyKind::JournalV2 => {
            // InvocationUuid (16 bytes) + journal_index (4 bytes)
            if remaining >= 16 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 16,
                    label: "invocation_uuid",
                });
            }
            if remaining >= 20 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 26,
                    len: 4,
                    label: "index",
                });
            }
        }
        KeyKind::JournalV2CompletionIdToCommandIndex => {
            // InvocationUuid (16 bytes) + completion_id (4 bytes)
            if remaining >= 16 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 16,
                    label: "invocation_uuid",
                });
            }
            if remaining >= 20 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 26,
                    len: 4,
                    label: "completion_id",
                });
            }
        }
        KeyKind::JournalEvent => {
            // InvocationUuid (16 bytes) + event_index (4 bytes)
            if remaining >= 16 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 16,
                    label: "invocation_uuid",
                });
            }
            if remaining >= 20 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 26,
                    len: 4,
                    label: "event_index",
                });
            }
        }
        KeyKind::Outbox => {
            // message_index (8 bytes)
            if remaining >= 8 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 8,
                    label: "message_index",
                });
            }
        }
        KeyKind::Fsm => {
            // state_id (8 bytes)
            if remaining >= 8 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 8,
                    label: "state_id",
                });
            }
        }
        KeyKind::Timers => {
            // timestamp (8 bytes) + timer_kind (variable)
            if remaining >= 8 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 8,
                    label: "timestamp",
                });
            }
            if remaining > 8 {
                // Rest is timer kind (discriminant + payload)
                segments.push(Segment {
                    kind: KeySegment::VariableField,
                    start: 18,
                    len: remaining - 8,
                    label: "timer_kind",
                });
            }
        }
        KeyKind::VQueueMeta | KeyKind::VQueueActive => {
            // parent (4 bytes) + instance (4 bytes)
            if remaining >= 4 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 4,
                    label: "parent",
                });
            }
            if remaining >= 8 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 14,
                    len: 4,
                    label: "instance",
                });
            }
        }
        KeyKind::VQueueInbox => {
            // parent(4) + instance(4) + stage(1) + priority(1) + visible_at(8) + created_at(8) + kind(1) + id(16)
            let mut pos = 10;
            let fields = [
                (4, "parent"),
                (4, "instance"),
                (1, "stage"),
                (1, "priority"),
                (8, "visible_at"),
                (8, "created_at"),
                (1, "entry_kind"),
                (16, "entry_id"),
            ];
            for (len, label) in fields {
                if pos + len <= key.len() {
                    segments.push(Segment {
                        kind: KeySegment::FixedField,
                        start: pos,
                        len,
                        label,
                    });
                    pos += len;
                } else {
                    break;
                }
            }
        }
        KeyKind::VQueueEntryState => {
            // kind (1 byte) + id (16 bytes)
            if remaining >= 1 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 1,
                    label: "entry_kind",
                });
            }
            if remaining >= 17 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 11,
                    len: 16,
                    label: "entry_id",
                });
            }
        }
        KeyKind::VQueueItems => {
            // parent(4) + instance(4) + kind(1) + id(16) + index(4)
            let mut pos = 10;
            let fields = [
                (4, "parent"),
                (4, "instance"),
                (1, "entry_kind"),
                (16, "entry_id"),
                (4, "index"),
            ];
            for (len, label) in fields {
                if pos + len <= key.len() {
                    segments.push(Segment {
                        kind: KeySegment::FixedField,
                        start: pos,
                        len,
                        label,
                    });
                    pos += len;
                } else {
                    break;
                }
            }
        }
        // Variable-length key types - parse varint-prefixed fields
        KeyKind::State => {
            // service_name (var) + service_key (var) + state_key (var)
            parse_variable_fields(
                &key[10..],
                &mut segments,
                &["service_name", "service_key", "state_key"],
            );
        }
        KeyKind::ServiceStatus => {
            // service_name (var) + service_key (var)
            parse_variable_fields(&key[10..], &mut segments, &["service_name", "service_key"]);
        }
        KeyKind::Inbox => {
            // service_name (var) + service_key (var) + sequence_number (8)
            let var_end =
                parse_variable_fields(&key[10..], &mut segments, &["service_name", "service_key"]);
            if var_end > 0 && 10 + var_end + 8 <= key.len() {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10 + var_end,
                    len: 8,
                    label: "sequence",
                });
            }
        }
        KeyKind::Promise => {
            // service_name (var) + service_key (var) + key (var)
            parse_variable_fields(
                &key[10..],
                &mut segments,
                &["service_name", "service_key", "promise_key"],
            );
        }
        KeyKind::Idempotency => {
            // service_name (var) + service_key (var) + service_handler (var) + idempotency_key (var)
            parse_variable_fields(
                &key[10..],
                &mut segments,
                &["service_name", "service_key", "handler", "idempotency_key"],
            );
        }
        KeyKind::Deduplication => {
            // producer_id - discriminant byte + payload
            if remaining >= 1 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 1,
                    label: "producer_kind",
                });
                // Rest is producer payload
                if remaining > 1 {
                    segments.push(Segment {
                        kind: KeySegment::VariableField,
                        start: 11,
                        len: remaining - 1,
                        label: "producer_id",
                    });
                }
            }
        }
        KeyKind::JournalV2NotificationIdToNotificationIndex => {
            // InvocationUuid (16 bytes) + notification_id (var)
            if remaining >= 16 {
                segments.push(Segment {
                    kind: KeySegment::FixedField,
                    start: 10,
                    len: 16,
                    label: "invocation_uuid",
                });
            }
            if remaining > 16 {
                parse_variable_fields(&key[26..], &mut segments, &["notification_id"]);
            }
        }
    }

    segments
}

/// Parse varint-prefixed variable-length fields.
/// Returns the total bytes consumed for all fields.
fn parse_variable_fields(
    data: &[u8],
    segments: &mut Vec<Segment>,
    labels: &[&'static str],
) -> usize {
    let base_offset = segments.iter().map(|s| s.start + s.len).max().unwrap_or(0);
    let mut pos = 0;

    for &label in labels {
        if pos >= data.len() {
            break;
        }

        // Decode varint length
        let (len, varint_bytes) = match decode_varint(&data[pos..]) {
            Some(v) => v,
            None => break,
        };

        // Add length prefix segment
        if varint_bytes > 0 {
            segments.push(Segment {
                kind: KeySegment::LengthPrefix,
                start: base_offset + pos,
                len: varint_bytes,
                label: "len",
            });
        }
        pos += varint_bytes;

        // Add the actual field data
        let field_len = len.min(data.len() - pos);
        if field_len > 0 {
            segments.push(Segment {
                kind: KeySegment::VariableField,
                start: base_offset + pos,
                len: field_len,
                label,
            });
        }
        pos += field_len;
    }

    pos
}

/// Decode a protobuf-style varint. Returns (value, bytes_consumed).
fn decode_varint(data: &[u8]) -> Option<(usize, usize)> {
    let mut result: usize = 0;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if i >= 10 {
            // Varint too long
            return None;
        }
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }
        shift += 7;
    }
    None
}

/// Format bytes as colorized hex string for terminal display.
///
/// Each segment of the key is colored differently to help visualize
/// the key structure:
/// - Cyan: KeyKind prefix (2 bytes)
/// - Yellow: Partition key/id (8 bytes)  
/// - Green: Fixed-size fields (uuids, indices, etc.)
/// - Magenta: Variable-length field data
/// - Grey: Length prefixes for variable fields
pub fn colorize_key_hex(key: &[u8]) -> String {
    let colors_enabled = CliContext::get().colors_enabled();
    let segments = build_segments(key);

    if !colors_enabled || segments.is_empty() {
        return hex_encode_plain(key);
    }

    let mut result = String::with_capacity(key.len() * 3);
    let mut pos = 0;

    for seg in &segments {
        // Fill gap with uncolored hex (shouldn't happen normally)
        while pos < seg.start && pos < key.len() {
            write_hex_byte(&mut result, key[pos]);
            pos += 1;
        }

        // Write colored segment
        let end = (seg.start + seg.len).min(key.len());
        if pos < end {
            let hex_part = hex_encode_plain(&key[pos..end]);
            let colored = hex_part.with(seg.kind.color());
            let _ = write!(result, "{}", colored);
            pos = end;
        }
    }

    // Handle any remaining bytes (shouldn't happen if segments cover the key)
    while pos < key.len() {
        write_hex_byte(&mut result, key[pos]);
        pos += 1;
    }

    result
}

/// Generate a legend for the key colors
pub fn color_legend() -> String {
    let colors_enabled = CliContext::get().colors_enabled();
    if !colors_enabled {
        return String::from("[colors disabled]");
    }

    format!(
        "{} {} {} {} {}",
        "kind".with(KeySegment::Kind.color()),
        "partition".with(KeySegment::Partition.color()),
        "fixed".with(KeySegment::FixedField.color()),
        "variable".with(KeySegment::VariableField.color()),
        "len".with(KeySegment::LengthPrefix.color()),
    )
}

fn hex_encode_plain(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        result.push(HEX_CHARS[(b >> 4) as usize] as char);
        result.push(HEX_CHARS[(b & 0xf) as usize] as char);
    }
    result
}

fn write_hex_byte(result: &mut String, byte: u8) {
    result.push(HEX_CHARS[(byte >> 4) as usize] as char);
    result.push(HEX_CHARS[(byte & 0xf) as usize] as char);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_varint() {
        // Single byte values
        assert_eq!(decode_varint(&[0x00]), Some((0, 1)));
        assert_eq!(decode_varint(&[0x01]), Some((1, 1)));
        assert_eq!(decode_varint(&[0x7f]), Some((127, 1)));

        // Two byte values
        assert_eq!(decode_varint(&[0x80, 0x01]), Some((128, 2)));
        assert_eq!(decode_varint(&[0xff, 0x01]), Some((255, 2)));

        // Empty input
        assert_eq!(decode_varint(&[]), None);
    }

    #[test]
    fn test_hex_encode_plain() {
        assert_eq!(hex_encode_plain(&[]), "");
        assert_eq!(hex_encode_plain(&[0x00]), "00");
        assert_eq!(hex_encode_plain(&[0xab, 0xcd]), "abcd");
        assert_eq!(hex_encode_plain(&[0x73, 0x74]), "7374"); // "st" for State
    }
}
