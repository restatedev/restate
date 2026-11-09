// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The rich output for terminals that render colors and Unicode: panels, tiles, bars and pills
//! printed inline, and a live dashboard for watch mode.

pub mod dashboard;
pub mod theme;
pub mod views;
pub mod widgets;

use std::fmt::Write;
use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};

use ratatui::buffer::Buffer;
use ratatui::style::{Color, Modifier};
use restate_cli_util::_unicode_width::UnicodeWidthStr;

use restate_cli_util::CliContext;

static PLAIN: AtomicBool = AtomicBool::new(false);

/// Asks for the plain output even where the rich one could be shown (`--plain`).
pub fn force_plain() {
    PLAIN.store(true, Ordering::Relaxed);
}

/// How a command presents its result.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Presentation {
    /// One JSON document (`--json`)
    Json,
    /// Text tables, for pipes, `NO_COLOR`, dumb terminals and `--plain`
    Plain,
    /// Panels and charts
    Rich,
}

impl Presentation {
    pub fn current() -> Self {
        let ctx = CliContext::get();
        if ctx.json_output() {
            Presentation::Json
        } else if PLAIN.load(Ordering::Relaxed) || !ctx.colors_enabled() {
            Presentation::Plain
        } else {
            Presentation::Rich
        }
    }
}

/// The live dashboard needs a terminal for its input and its output.
pub fn can_run_dashboard() -> bool {
    Presentation::current() == Presentation::Rich
        && std::io::stdout().is_terminal()
        && std::io::stdin().is_terminal()
}

/// Columns to render to: the terminal's width, else `$COLUMNS`, else 120.
pub fn width() -> u16 {
    crossterm::terminal::size()
        .map(|(w, _)| w)
        .ok()
        .or_else(|| std::env::var("COLUMNS").ok()?.parse().ok())
        .filter(|w| *w >= 60)
        .unwrap_or(120)
        .min(220)
}

/// Turns each row of `buffer` into a line with ANSI escapes, without trailing blanks.
pub fn to_ansi(buffer: &Buffer) -> Vec<String> {
    let area = buffer.area;
    let mut lines = Vec::with_capacity(usize::from(area.height));
    for y in area.top()..area.bottom() {
        let cells: Vec<_> = (area.left()..area.right())
            .map(|x| &buffer[(x, y)])
            .collect();
        let last = cells
            .iter()
            .rposition(|c| c.symbol() != " " || c.bg != Color::Reset)
            .map_or(0, |i| i + 1);

        let mut line = String::new();
        let mut current = None;
        let mut skip = 0;
        for cell in &cells[..last] {
            if skip > 0 {
                skip -= 1;
                continue;
            }
            let style = (cell.fg, cell.bg, cell.modifier);
            if current != Some(style) {
                line.push_str("\x1b[0m");
                push_sgr(&mut line, cell.fg, cell.bg, cell.modifier);
                current = Some(style);
            }
            line.push_str(cell.symbol());
            skip = cell.symbol().width().saturating_sub(1);
        }
        if current.is_some() {
            line.push_str("\x1b[0m");
        }
        lines.push(line);
    }
    while lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }
    lines
}

fn push_sgr(out: &mut String, fg: Color, bg: Color, modifier: Modifier) {
    let mut codes = Vec::new();
    for (flag, code) in [
        (Modifier::BOLD, "1"),
        (Modifier::DIM, "2"),
        (Modifier::ITALIC, "3"),
        (Modifier::UNDERLINED, "4"),
        (Modifier::REVERSED, "7"),
        (Modifier::CROSSED_OUT, "9"),
    ] {
        if modifier.contains(flag) {
            codes.push(code.to_owned());
        }
    }
    if let Some(code) = color_code(fg, 30) {
        codes.push(code);
    }
    if let Some(code) = color_code(bg, 40) {
        codes.push(code);
    }
    if !codes.is_empty() {
        let _ = write!(out, "\x1b[{}m", codes.join(";"));
    }
}

/// The SGR parameters of `color`; `base` is 30 for foreground and 40 for background colors.
fn color_code(color: Color, base: u8) -> Option<String> {
    let named = |n: u8| Some((base + n).to_string());
    let bright = |n: u8| Some((base + 60 + n).to_string());
    match color {
        Color::Reset => None,
        Color::Black => named(0),
        Color::Red => named(1),
        Color::Green => named(2),
        Color::Yellow => named(3),
        Color::Blue => named(4),
        Color::Magenta => named(5),
        Color::Cyan => named(6),
        Color::Gray => named(7),
        Color::DarkGray => bright(0),
        Color::LightRed => bright(1),
        Color::LightGreen => bright(2),
        Color::LightYellow => bright(3),
        Color::LightBlue => bright(4),
        Color::LightMagenta => bright(5),
        Color::LightCyan => bright(6),
        Color::White => bright(7),
        Color::Indexed(i) => Some(format!("{};5;{i}", base + 8)),
        Color::Rgb(r, g, b) => Some(format!("{};2;{r};{g};{b}", base + 8)),
    }
}
