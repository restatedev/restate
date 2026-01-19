// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Colorized display for Restate resource IDs.
//!
//! This module provides utilities to display ID strings with color-coding
//! to help visualize the ID structure. Each part of the ID is colored
//! differently to make it easier to understand the encoding layout.

use std::fmt::Write;

use crossterm::style::{Color, Stylize};
use restate_cli_util::CliContext;

/// Color scheme for different ID segments.
#[derive(Debug, Clone, Copy)]
pub enum IdSegment {
    /// Resource type prefix (inv, dp, sub, prom, sign, snap)
    Prefix,
    /// Separator character (_)
    Separator,
    /// Version character (1)
    Version,
    /// Partition key (base62 encoded, 11 chars for u64)
    PartitionKey,
    /// UUID/ULID component (base62 encoded, 22 chars for u128)
    Uuid,
    /// Base64 payload (for awakeable/signal IDs)
    Base64Payload,
}

impl IdSegment {
    fn color(self) -> Color {
        match self {
            IdSegment::Prefix => Color::Cyan,
            IdSegment::Separator => Color::DarkGrey,
            IdSegment::Version => Color::DarkGrey,
            IdSegment::PartitionKey => Color::Yellow,
            IdSegment::Uuid => Color::Green,
            IdSegment::Base64Payload => Color::Magenta,
        }
    }
}

/// A segment of the ID string with its semantic meaning
struct Segment {
    kind: IdSegment,
    start: usize,
    len: usize,
}

/// ID format types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdFormat {
    /// inv_1<partition_key><uuid> - 11 chars partition key + 22 chars uuid
    Invocation,
    /// dp_1<uuid>, sub_1<uuid>, snap_1<uuid> - 22 chars uuid
    UlidBased,
    /// prom_1<base64>, sign_1<base64> - base64 encoded payload
    Base64Based,
    /// Raw ULID (26 chars Crockford base32)
    RawUlid,
    /// Unknown format
    Unknown,
}

impl IdFormat {
    /// Detect the ID format from the prefix
    pub fn detect(id: &str) -> Self {
        if let Some(prefix) = id.split('_').next() {
            match prefix {
                "inv" => IdFormat::Invocation,
                "dp" | "sub" | "snap" => IdFormat::UlidBased,
                "prom" | "sign" => IdFormat::Base64Based,
                _ => {
                    // Check if it's a raw ULID (26 chars, no underscore)
                    if !id.contains('_') && id.len() == 26 {
                        IdFormat::RawUlid
                    } else {
                        IdFormat::Unknown
                    }
                }
            }
        } else if !id.contains('_') && id.len() == 26 {
            IdFormat::RawUlid
        } else {
            IdFormat::Unknown
        }
    }
}

/// Build segments for an ID string based on its format
fn build_segments(id: &str, format: IdFormat) -> Vec<Segment> {
    let mut segments = Vec::new();

    match format {
        IdFormat::Invocation => {
            // Format: inv_1<partition_key:11><uuid:22>
            // Total after prefix: 1 + 11 + 22 = 34 chars
            if let Some(sep_pos) = id.find('_') {
                // Prefix
                segments.push(Segment {
                    kind: IdSegment::Prefix,
                    start: 0,
                    len: sep_pos,
                });
                // Separator
                segments.push(Segment {
                    kind: IdSegment::Separator,
                    start: sep_pos,
                    len: 1,
                });
                // Version (1 char)
                let version_start = sep_pos + 1;
                if version_start < id.len() {
                    segments.push(Segment {
                        kind: IdSegment::Version,
                        start: version_start,
                        len: 1,
                    });
                }
                // Partition key (11 chars for base62 u64)
                let pk_start = version_start + 1;
                if pk_start + 11 <= id.len() {
                    segments.push(Segment {
                        kind: IdSegment::PartitionKey,
                        start: pk_start,
                        len: 11,
                    });
                }
                // UUID (22 chars for base62 u128)
                let uuid_start = pk_start + 11;
                if uuid_start < id.len() {
                    segments.push(Segment {
                        kind: IdSegment::Uuid,
                        start: uuid_start,
                        len: id.len() - uuid_start,
                    });
                }
            }
        }
        IdFormat::UlidBased => {
            // Format: <prefix>_1<uuid:22>
            if let Some(sep_pos) = id.find('_') {
                // Prefix
                segments.push(Segment {
                    kind: IdSegment::Prefix,
                    start: 0,
                    len: sep_pos,
                });
                // Separator
                segments.push(Segment {
                    kind: IdSegment::Separator,
                    start: sep_pos,
                    len: 1,
                });
                // Version (1 char)
                let version_start = sep_pos + 1;
                if version_start < id.len() {
                    segments.push(Segment {
                        kind: IdSegment::Version,
                        start: version_start,
                        len: 1,
                    });
                }
                // UUID (rest is base62 u128)
                let uuid_start = version_start + 1;
                if uuid_start < id.len() {
                    segments.push(Segment {
                        kind: IdSegment::Uuid,
                        start: uuid_start,
                        len: id.len() - uuid_start,
                    });
                }
            }
        }
        IdFormat::Base64Based => {
            // Format: <prefix>_1<base64_payload>
            if let Some(sep_pos) = id.find('_') {
                // Prefix
                segments.push(Segment {
                    kind: IdSegment::Prefix,
                    start: 0,
                    len: sep_pos,
                });
                // Separator
                segments.push(Segment {
                    kind: IdSegment::Separator,
                    start: sep_pos,
                    len: 1,
                });
                // Version (1 char)
                let version_start = sep_pos + 1;
                if version_start < id.len() {
                    segments.push(Segment {
                        kind: IdSegment::Version,
                        start: version_start,
                        len: 1,
                    });
                }
                // Base64 payload (rest)
                let payload_start = version_start + 1;
                if payload_start < id.len() {
                    segments.push(Segment {
                        kind: IdSegment::Base64Payload,
                        start: payload_start,
                        len: id.len() - payload_start,
                    });
                }
            }
        }
        IdFormat::RawUlid => {
            // Entire string is the ULID
            segments.push(Segment {
                kind: IdSegment::Uuid,
                start: 0,
                len: id.len(),
            });
        }
        IdFormat::Unknown => {
            // No colorization for unknown formats
        }
    }

    segments
}

/// Format an ID string with colorized components for terminal display.
///
/// Each segment of the ID is colored differently to help visualize
/// the ID structure:
/// - Cyan: Resource type prefix (inv, dp, sub, etc.)
/// - Grey: Separator (_) and version (1)
/// - Yellow: Partition key (for invocation IDs)
/// - Green: UUID/ULID component
/// - Magenta: Base64 payload (for awakeable/signal IDs)
pub fn colorize_id(id: &str) -> String {
    let colors_enabled = CliContext::get().colors_enabled();
    let format = IdFormat::detect(id);
    let segments = build_segments(id, format);

    if !colors_enabled || segments.is_empty() {
        return id.to_string();
    }

    let mut result = String::with_capacity(id.len() * 2); // Extra capacity for ANSI codes
    let mut pos = 0;

    for seg in &segments {
        // Fill gap with uncolored text (shouldn't happen normally)
        if pos < seg.start {
            result.push_str(&id[pos..seg.start]);
            pos = seg.start;
        }

        // Write colored segment
        let end = (seg.start + seg.len).min(id.len());
        if pos < end {
            let part = &id[pos..end];
            let colored = part.with(seg.kind.color());
            let _ = write!(result, "{}", colored);
            pos = end;
        }
    }

    // Handle any remaining characters
    if pos < id.len() {
        result.push_str(&id[pos..]);
    }

    result
}

/// Generate a legend for the ID colors
pub fn id_color_legend() -> String {
    let colors_enabled = CliContext::get().colors_enabled();
    if !colors_enabled {
        return String::from("[colors disabled]");
    }

    format!(
        "{} {} {} {}",
        "prefix".with(IdSegment::Prefix.color()),
        "partition".with(IdSegment::PartitionKey.color()),
        "uuid".with(IdSegment::Uuid.color()),
        "payload".with(IdSegment::Base64Payload.color()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_format() {
        assert_eq!(
            IdFormat::detect("inv_1fBuS9KMPNrt0zQBGdilypucn1EeT7AO5j"),
            IdFormat::Invocation
        );
        assert_eq!(
            IdFormat::detect("dp_11nGQpCRmau6ypL82KH2TnP"),
            IdFormat::UlidBased
        );
        assert_eq!(
            IdFormat::detect("sub_11nGQpCRmau6ypL82KH2TnP"),
            IdFormat::UlidBased
        );
        assert_eq!(
            IdFormat::detect("snap_11nGQpCRmau6ypL82KH2TnP"),
            IdFormat::UlidBased
        );
        assert_eq!(IdFormat::detect("prom_1abc123"), IdFormat::Base64Based);
        assert_eq!(IdFormat::detect("sign_1abc123"), IdFormat::Base64Based);
        assert_eq!(
            IdFormat::detect("01HZJ49VHPQ14ZM6YGG5NJD2RN"),
            IdFormat::RawUlid
        );
        assert_eq!(IdFormat::detect("invalid"), IdFormat::Unknown);
    }

    #[test]
    fn test_build_segments_invocation() {
        let id = "inv_1fBuS9KMPNrt0zQBGdilypucn1EeT7AO5j";
        let segments = build_segments(id, IdFormat::Invocation);

        assert_eq!(segments.len(), 5);
        assert_eq!(segments[0].len, 3); // "inv"
        assert_eq!(segments[1].len, 1); // "_"
        assert_eq!(segments[2].len, 1); // "1"
        assert_eq!(segments[3].len, 11); // partition key
        assert_eq!(segments[4].len, 22); // uuid
    }

    #[test]
    fn test_build_segments_ulid_based() {
        let id = "dp_11nGQpCRmau6ypL82KH2TnP";
        let segments = build_segments(id, IdFormat::UlidBased);

        assert_eq!(segments.len(), 4);
        assert_eq!(segments[0].len, 2); // "dp"
        assert_eq!(segments[1].len, 1); // "_"
        assert_eq!(segments[2].len, 1); // "1"
        assert_eq!(segments[3].len, 22); // uuid
    }
}
