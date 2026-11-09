// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Selecting text with the mouse in the dashboard, and copying it to the clipboard.

use std::io::Write;

use base64::Engine;
use ratatui::buffer::Buffer;
use ratatui::layout::{Position, Rect};
use ratatui::style::Modifier;

/// Text selected by dragging, from where the drag started to where it is, in reading order and
/// within the pane it started in.
#[derive(Clone, Copy, Debug)]
pub struct Selection {
    pane: Rect,
    anchor: Position,
    cursor: Position,
    /// whether the mouse moved since the button went down; a click doesn't select
    pub dragged: bool,
}

impl Selection {
    pub fn new(pane: Rect, at: Position) -> Self {
        Self {
            pane,
            anchor: at,
            cursor: at,
            dragged: false,
        }
    }

    pub fn extend_to(&mut self, at: Position) {
        let clamp = |v: u16, lo: u16, hi: u16| v.clamp(lo, hi.saturating_sub(1).max(lo));
        self.cursor = Position::new(
            clamp(at.x, self.pane.left(), self.pane.right()),
            clamp(at.y, self.pane.top(), self.pane.bottom()),
        );
        self.dragged |= self.cursor != self.anchor;
    }

    fn ordered(&self) -> (Position, Position) {
        if (self.anchor.y, self.anchor.x) <= (self.cursor.y, self.cursor.x) {
            (self.anchor, self.cursor)
        } else {
            (self.cursor, self.anchor)
        }
    }

    /// Shows the selection in `buffer`, in reverse video.
    pub fn highlight(&self, buffer: &mut Buffer) {
        let area = self.pane.intersection(buffer.area);
        for at in area.positions().filter(|at| self.contains(*at)) {
            buffer[at].modifier.insert(Modifier::REVERSED);
        }
    }

    /// Whether the cell at `at` is selected.
    pub fn contains(&self, at: Position) -> bool {
        let (start, end) = self.ordered();
        if !self.pane.contains(at) || at.y < start.y || at.y > end.y {
            return false;
        }
        (at.y > start.y || at.x >= start.x) && (at.y < end.y || at.x <= end.x)
    }

    /// The selected text of `buffer`, a line per row without trailing blanks. A row that
    /// continues a word wrapped from the row above, at `wraps`, joins it without its indent.
    pub fn text(&self, buffer: &Buffer, wraps: &[Position]) -> String {
        let (start, end) = self.ordered();
        let mut text = String::new();
        for y in start.y..=end.y {
            let mut from = if y == start.y {
                start.x
            } else {
                self.pane.left()
            };
            let to = if y == end.y {
                end.x + 1
            } else {
                self.pane.right()
            };
            if y > start.y {
                match resumes(wraps, self.pane, y) {
                    Some(x) => from = from.max(x),
                    None => text.push('\n'),
                }
            }
            text.push_str(row_text(buffer, y, from, to).trim_end());
        }
        text
    }
}

fn row_text(buffer: &Buffer, y: u16, from: u16, to: u16) -> String {
    let area = buffer.area;
    if y < area.top() || y >= area.bottom() {
        return String::new();
    }
    (from.max(area.left())..to.min(area.right()))
        .map(|x| buffer[(x, y)].symbol())
        .collect()
}

/// Where on row `y` of `pane` a word wrapped from the row above resumes, if one does.
fn resumes(wraps: &[Position], pane: Rect, y: u16) -> Option<u16> {
    wraps
        .iter()
        .find(|at| at.y == y && pane.contains(**at))
        .map(|at| at.x)
}

/// Whether `c` belongs to a token: ids, LSNs, addresses, paths. Box drawing, bars and markers
/// don't.
fn in_token(c: char) -> bool {
    c.is_alphanumeric() || "-_.:/@%+=~#?&,[]".contains(c)
}

/// The token around `row[i]`, as the range of `row` it spans.
fn token_bounds(row: &[char], i: usize) -> (usize, usize) {
    let start = row[..i]
        .iter()
        .rposition(|c| !in_token(*c))
        .map_or(0, |at| at + 1);
    let end = row[i..]
        .iter()
        .position(|c| !in_token(*c))
        .map_or(row.len(), |at| i + at);
    (start, end)
}

/// The token under `at` in `buffer`, within `pane`: what a double-click copies. A token wrapped
/// over several rows, at `wraps`, is copied whole.
pub fn token_at(buffer: &Buffer, pane: Rect, wraps: &[Position], at: Position) -> Option<String> {
    if !pane.contains(at) {
        return None;
    }
    // a char per cell, so that indexes are columns
    let cells = |y: u16| -> Vec<char> {
        (pane.left()..pane.right())
            .map(|x| {
                buffer
                    .cell((x, y))
                    .and_then(|cell| cell.symbol().chars().next())
                    .unwrap_or(' ')
            })
            .collect()
    };
    let column = |x: u16| usize::from(x - pane.left());
    let row = cells(at.y);
    let i = column(at.x);
    if !row.get(i).copied().is_some_and(in_token) {
        return None;
    }
    let (start, end) = token_bounds(&row, i);
    let mut token: String = row[start..end].iter().collect();

    // where it starts on the rows above, each ending with the part before
    let (mut y, mut start) = (at.y, start);
    while y > pane.top() && resumes(wraps, pane, y).map(column) == Some(start) {
        y -= 1;
        let above = cells(y);
        let Some(last) = above
            .iter()
            .rposition(|c| *c != ' ')
            .filter(|last| in_token(above[*last]))
        else {
            break;
        };
        let (from, _) = token_bounds(&above, last);
        token.insert_str(0, &above[from..=last].iter().collect::<String>());
        start = from;
    }
    // and where it ends on the rows below, as long as it ends its row
    let (mut y, mut row, mut end) = (at.y, row, end);
    while y + 1 < pane.bottom() && row[end..].iter().all(|c| *c == ' ') {
        let Some(resume) = resumes(wraps, pane, y + 1).map(column) else {
            break;
        };
        y += 1;
        row = cells(y);
        if !row.get(resume).copied().is_some_and(in_token) {
            break;
        }
        let (_, to) = token_bounds(&row, resume);
        token.extend(&row[resume..to]);
        end = to;
    }
    // a list's separators and a sentence's period aren't part of what was clicked
    let token = token.trim_end_matches([',', '.', ':']).to_owned();
    (!token.is_empty()).then_some(token)
}

/// Asks the terminal to report clicks, drags and the wheel (`enable`), or to stop. Unlike
/// crossterm's `EnableMouseCapture`, this doesn't ask for every move of the pointer.
pub fn capture(enable: bool) -> std::io::Result<()> {
    let mut out = std::io::stdout().lock();
    out.write_all(if enable {
        b"\x1b[?1000h\x1b[?1002h\x1b[?1006h"
    } else {
        b"\x1b[?1006l\x1b[?1002l\x1b[?1000l"
    })?;
    out.flush()
}

/// Asks the terminal to put `text` on the clipboard (OSC 52). Terminals that don't support it
/// ignore the request; nothing tells whether it worked.
pub fn copy_to_clipboard(text: &str) -> std::io::Result<()> {
    let encoded = base64::engine::general_purpose::STANDARD.encode(text);
    let mut out = std::io::stdout().lock();
    write!(out, "\x1b]52;c;{encoded}\x07")?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use ratatui::text::Line;
    use ratatui::widgets::{Paragraph, Widget};

    use super::*;

    fn screen() -> Buffer {
        let area = Rect::new(0, 0, 40, 5);
        let mut buffer = Buffer::empty(area);
        Paragraph::new(
            [
                " N1:13  leader  4190045",
                " unix:/var/lib/fabric.sock",
                " 0,19,32",
                " listens on unix:/var/lib/",
                "            restate/node.sock",
            ]
            .map(|row| Line::from(format!("│{row:<30}│")))
            .to_vec(),
        )
        .render(area, &mut buffer);
        buffer
    }

    /// A drag selects within its pane in reading order; a double-click picks the token. Both
    /// follow a token wrapped onto the next row.
    #[test]
    fn selects_text_and_tokens() {
        let buffer = screen();
        let pane = Rect::new(1, 0, 30, 5);
        // the path on the fourth row continues on the fifth
        let wraps = [Position::new(13, 4)];
        let token = |x, y| token_at(&buffer, pane, &wraps, Position::new(x, y));

        let mut selection = Selection::new(pane, Position::new(9, 0));
        selection.extend_to(Position::new(5, 1));
        assert!(selection.dragged);
        assert_eq!(selection.text(&buffer, &wraps), "leader  4190045\n unix");

        // dragging past the pane stays within it
        let mut selection = Selection::new(pane, Position::new(2, 0));
        selection.extend_to(Position::new(39, 0));
        assert_eq!(selection.text(&buffer, &wraps), "N1:13  leader  4190045");

        assert_eq!(token(4, 0).as_deref(), Some("N1:13"));
        assert_eq!(token(12, 1).as_deref(), Some("unix:/var/lib/fabric.sock"));
        assert_eq!(token(3, 2).as_deref(), Some("0,19,32"));
        assert_eq!(token(8, 0), None);

        // a wrapped path, from either of its rows, by a double-click or a drag
        let path = Some("unix:/var/lib/restate/node.sock");
        assert_eq!(token(15, 3).as_deref(), path);
        assert_eq!(token(25, 4).as_deref(), path);
        assert_eq!(token(4, 3).as_deref(), Some("listens"));
        let mut selection = Selection::new(pane, Position::new(13, 3));
        selection.extend_to(Position::new(29, 4));
        assert_eq!(Some(selection.text(&buffer, &wraps).as_str()), path);
    }
}
