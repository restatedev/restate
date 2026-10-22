// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::IdResourceType;

/// Color scheme for different ID segments.
#[derive(Debug, Clone, Copy)]
pub enum IdSegment {
    /// Resource type prefix (inv, mut, vq, dp, sub, prom, sign, snap)
    Prefix,
    /// Separator character (_)
    Separator,
    /// Version character (1)
    Version,
    /// Partition key (base62 encoded, 11 chars for u64)
    PartitionKey,
    /// UUID/ULID/digest component (base62 encoded, 22 chars for u128)
    Uuid,
    /// Base64 payload (for awakeable/signal IDs)
    Base64Payload,
    /// Decimal sequence number suffix (for canonical entry IDs)
    Sequence,
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
            IdSegment::Sequence => Color::Blue,
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
    /// inv_1<partition_key><uuid>, mut_1<partition_key><ulid>, vq_1<partition_key><digest>:
    /// 11 chars partition key + 22 chars uuid, optionally followed by `_<seq>` for
    /// canonical entry IDs
    Partitioned,
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
        match id
            .split_once('_')
            .map(|(prefix, _)| prefix.parse::<IdResourceType>())
        {
            Some(Ok(
                IdResourceType::Invocation | IdResourceType::StateMutation | IdResourceType::VQueue,
            )) => IdFormat::Partitioned,
            Some(Ok(
                IdResourceType::Deployment
                | IdResourceType::Subscription
                | IdResourceType::Snapshot,
            )) => IdFormat::UlidBased,
            Some(Ok(IdResourceType::Awakeable | IdResourceType::Signal)) => IdFormat::Base64Based,
            // Raw ULID: 26 chars, no underscore
            None if id.len() == 26 => IdFormat::RawUlid,
            _ => IdFormat::Unknown,
        }
    }
}

/// Build segments for an ID string based on its format
fn build_segments(id: &str, format: IdFormat) -> Vec<Segment> {
    let mut segments = Vec::new();

    match format {
        IdFormat::Partitioned => {
            // Format: <prefix>_1<partition_key:11><uuid:22>[_<seq>]
            // Total after prefix: 1 + 11 + 22 = 34 chars, plus the optional sequence suffix
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
                        len: 22.min(id.len() - uuid_start),
                    });
                }
                // Optional `_<seq>` suffix of canonical entry IDs
                let seq_sep = uuid_start + 22;
                if id.get(seq_sep..).is_some_and(|rest| rest.starts_with('_')) {
                    segments.push(Segment {
                        kind: IdSegment::Separator,
                        start: seq_sep,
                        len: 1,
                    });
                    segments.push(Segment {
                        kind: IdSegment::Sequence,
                        start: seq_sep + 1,
                        len: id.len() - seq_sep - 1,
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
/// - Yellow: Partition key (for invocation, state mutation and vqueue IDs)
/// - Green: UUID/ULID/digest component
/// - Magenta: Base64 payload (for awakeable/signal IDs)
/// - Blue: Sequence number (for canonical entry IDs)
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
        "{} {} {} {} {}",
        "prefix".with(IdSegment::Prefix.color()),
        "partition".with(IdSegment::PartitionKey.color()),
        "uuid".with(IdSegment::Uuid.color()),
        "payload".with(IdSegment::Base64Payload.color()),
        "seq".with(IdSegment::Sequence.color()),
    )
}

#[cfg(test)]
mod tests {
    use restate_types::identifiers::{BaseEntryId, StateMutationId};
    use restate_types::vqueues::{Seq, VQueueId};

    use super::*;

    fn segment_lens(id: &str) -> Vec<usize> {
        build_segments(id, IdFormat::detect(id))
            .iter()
            .map(|s| s.len)
            .collect()
    }

    #[test]
    fn detect_format() {
        assert_eq!(
            IdFormat::detect("inv_1fBuS9KMPNrt0zQBGdilypucn1EeT7AO5j"),
            IdFormat::Partitioned
        );
        assert_eq!(
            IdFormat::detect(&StateMutationId::generate(1).to_string()),
            IdFormat::Partitioned
        );
        assert_eq!(
            IdFormat::detect(&VQueueId::custom(1, "q").to_string()),
            IdFormat::Partitioned
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
    fn build_segments_partitioned() {
        // prefix, "_", "1", partition key, uuid
        assert_eq!(
            segment_lens("inv_1fBuS9KMPNrt0zQBGdilypucn1EeT7AO5j"),
            [3, 1, 1, 11, 22]
        );

        let mutation_id = StateMutationId::generate(42);
        assert_eq!(segment_lens(&mutation_id.to_string()), [3, 1, 1, 11, 22]);
        assert_eq!(
            segment_lens(&VQueueId::custom(42, "q").to_string()),
            [2, 1, 1, 11, 22]
        );

        // Canonical entry IDs append "_" and the sequence number
        let canonical = BaseEntryId::from(mutation_id).canonicalize(Seq::new(1234));
        assert_eq!(
            segment_lens(&canonical.to_string()),
            [3, 1, 1, 11, 22, 1, 4]
        );
    }

    #[test]
    fn build_segments_ulid_based() {
        let id = "dp_11nGQpCRmau6ypL82KH2TnP";
        let segments = build_segments(id, IdFormat::UlidBased);

        assert_eq!(segments.len(), 4);
        assert_eq!(segments[0].len, 2); // "dp"
        assert_eq!(segments[1].len, 1); // "_"
        assert_eq!(segments[2].len, 1); // "1"
        assert_eq!(segments[3].len, 22); // uuid
    }
}
