// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Colors of the rich output. Text uses mid-tone colors that read on dark and light terminals;
//! filled elements (pills, tiles) set both their background and foreground.

use std::sync::OnceLock;

use ratatui::style::{Color, Modifier, Style};

use restate_types::PlainNodeId;

use crate::observe::{PartitionHealth, Severity};

pub type Rgb = (u8, u8, u8);

pub const GREEN: Rgb = (0x3f, 0xb9, 0x50);
pub const AMBER: Rgb = (0xd2, 0x99, 0x22);
pub const RED: Rgb = (0xf8, 0x51, 0x49);
pub const BLUE: Rgb = (0x58, 0xa6, 0xff);
pub const MUTED: Rgb = (0x7d, 0x85, 0x90);
pub const BORDER: Rgb = (0x4a, 0x52, 0x5c);
pub const TRACK: Rgb = (0x30, 0x36, 0x3d);

/// Distinct colors that identify nodes, away from the green/amber/red of health.
const NODE_COLORS: [Rgb; 8] = [
    (0x58, 0xa6, 0xff),
    (0xbc, 0x8c, 0xff),
    (0x39, 0xc5, 0xcf),
    (0xff, 0xa6, 0x57),
    (0xf7, 0x78, 0xba),
    (0xa5, 0xd6, 0xff),
    (0xe3, 0xb3, 0x41),
    (0x8d, 0xdb, 0x8c),
];

/// Whether the terminal renders 24-bit colors; otherwise colors map to the 256-color palette.
fn truecolor() -> bool {
    static TRUECOLOR: OnceLock<bool> = OnceLock::new();
    *TRUECOLOR.get_or_init(|| {
        let colorterm = std::env::var("COLORTERM").unwrap_or_default();
        let program = std::env::var("TERM_PROGRAM").unwrap_or_default();
        matches!(colorterm.as_str(), "truecolor" | "24bit")
            || matches!(
                program.as_str(),
                "iTerm.app" | "WezTerm" | "vscode" | "ghostty" | "Apple_Terminal"
            )
    })
}

pub fn rgb((r, g, b): Rgb) -> Color {
    if truecolor() {
        Color::Rgb(r, g, b)
    } else {
        // nearest color of the 6x6x6 cube
        let level = |v: u8| ((u16::from(v) * 5 + 127) / 255) as u8;
        Color::Indexed(16 + 36 * level(r) + 6 * level(g) + level(b))
    }
}

pub fn fg(color: Rgb) -> Style {
    Style::new().fg(rgb(color))
}

pub fn muted() -> Style {
    fg(MUTED)
}

pub fn border() -> Style {
    fg(BORDER)
}

pub fn bold() -> Style {
    Style::new().add_modifier(Modifier::BOLD)
}

pub fn node(id: PlainNodeId) -> Rgb {
    NODE_COLORS[(u32::from(id).saturating_sub(1) as usize) % NODE_COLORS.len()]
}

pub fn node_style(id: PlainNodeId) -> Style {
    fg(node(id)).add_modifier(Modifier::BOLD)
}

pub fn severity(severity: Severity) -> Rgb {
    match severity {
        Severity::Critical => RED,
        Severity::Warning => AMBER,
        Severity::Info => BLUE,
    }
}

/// A filled label, e.g. ` WARN `.
pub fn pill(color: Rgb) -> Style {
    let text = if color == RED || color == BLUE {
        (0xff, 0xff, 0xff)
    } else {
        (0x0d, 0x11, 0x17)
    };
    Style::new()
        .bg(rgb(color))
        .fg(rgb(text))
        .add_modifier(Modifier::BOLD)
}

/// Background and text color of a partition tile.
pub fn tile(health: PartitionHealth) -> Style {
    let (bg, text) = match health {
        PartitionHealth::Healthy => ((0x1a, 0x4a, 0x2b), (0xaf, 0xf5, 0xb4)),
        PartitionHealth::Reconfiguring => ((0x1f, 0x5f, 0xc8), (0xff, 0xff, 0xff)),
        PartitionHealth::Degraded => ((0xc0, 0x8a, 0x10), (0x0d, 0x11, 0x17)),
        PartitionHealth::Unavailable => ((0xda, 0x36, 0x33), (0xff, 0xff, 0xff)),
    };
    let style = Style::new().bg(rgb(bg)).fg(rgb(text));
    if health == PartitionHealth::Healthy {
        style
    } else {
        style.add_modifier(Modifier::BOLD)
    }
}

pub fn health(health: PartitionHealth) -> Rgb {
    match health {
        PartitionHealth::Healthy => GREEN,
        PartitionHealth::Reconfiguring => BLUE,
        PartitionHealth::Degraded => AMBER,
        PartitionHealth::Unavailable => RED,
    }
}
