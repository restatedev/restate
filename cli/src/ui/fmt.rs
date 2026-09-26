// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! High-level output formatter abstraction.
//!
//! Commands describe *what* to output using semantic building blocks — a
//! [`title`](OutputFormatter::title), a key-value [`detail`](OutputFormatter::detail)
//! view, a [`table`](OutputFormatter::table), or a single scalar
//! [`value`](OutputFormatter::value) — and the selected [`OutputFormatter`] decides
//! *how* to render them: human-friendly styled tables, or a single JSON document for
//! scripting and agents (`--json`).
//!
//! The formatter also owns all coloring: a caller attaches a semantic [`Style`] to a
//! [`Field`], and only the human formatter renders it — the JSON formatter ignores
//! styling entirely. This keeps commands free of `if json { … } else { … }` branches
//! and keeps the "what" separate from the "how".
//!
//! # Example
//!
//! ```ignore
//! use crate::ui::fmt::{Field, Formatter, OutputFormatter};
//! use restate_cli_util::ui::stylesheet::Style;
//!
//! let mut f = Formatter::new(); // picks human or JSON based on --json
//! f.title("📜", "Service Information");
//! f.detail("service", &[
//!     ("name", Field::new("greeter")),
//!     ("status", Field::styled("running", Style::Success)),
//!     ("revision", Field::new(3)),
//! ]);
//! f.finish()?;
//! ```

use std::borrow::Borrow;

use chrono::{DateTime, Local};
use comfy_table::{Cell, Table};
use dialoguer::console::measure_text_width;
use serde::Serialize;
use serde_json::{Map, Value};

use restate_cli_util::ui::console::{Styled, StyledTable, confirm_or_exit};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, exit};

/// A single output value: a JSON-native value, an optional human-display override,
/// and an optional semantic [`Style`].
///
/// The JSON formatter emits `value` verbatim; the human formatter renders `display`
/// (falling back to `value`) with `style` applied. Use [`with_display`](Field::with_display)
/// when the two representations genuinely differ — a duration shown as `"5 minutes
/// ago"` but stored as a number, a list shown on multiple lines but stored as an
/// array, an enum shown via `Debug` but stored as a string.
///
/// Construct from any type that converts into a `serde_json::Value` (`&str`,
/// `String`, integers, floats, `bool`, …).
#[derive(Clone)]
pub struct Field {
    value: Value,
    display: Option<String>,
    style: Option<Style>,
}

impl Field {
    /// A plain, unstyled value; the human rendering is derived from the value.
    pub fn new(value: impl Into<Value>) -> Self {
        Self {
            value: value.into(),
            display: None,
            style: None,
        }
    }

    /// A value carrying a semantic style (applied by the human formatter only).
    pub fn styled(value: impl Into<Value>, style: Style) -> Self {
        Self {
            value: value.into(),
            display: None,
            style: Some(style),
        }
    }

    /// A value with an explicit human rendering distinct from its machine value.
    pub fn with_display(value: impl Into<Value>, display: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            display: Some(display.into()),
            style: None,
        }
    }

    /// A value from an already-built JSON document (e.g. arbitrary nested data).
    pub fn json(value: Value) -> Self {
        Self {
            value,
            display: None,
            style: None,
        }
    }

    /// The value rendered as a plain string for human output.
    fn human_string(&self) -> String {
        if let Some(display) = &self.display {
            return display.clone();
        }
        match &self.value {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }
    }

    /// The value rendered for human output, with styling applied when enabled.
    fn human_display(&self) -> String {
        match self.style {
            Some(style) => Styled(style, self.human_string()).to_string(),
            None => self.human_string(),
        }
    }

    /// A comfy-table cell for human output, with styling applied when enabled.
    fn to_cell(&self) -> Cell {
        let text = self.human_string();
        match self.style {
            Some(style) => Cell::new(Styled(style, text)),
            None => Cell::new(text),
        }
    }
}

/// One entry in a journal-style timeline ([`OutputFormatter::journal`]).
///
/// The caller composes the human cells (`entry`, `name`, `details`) and `record` (the
/// machine JSON object) so the formatter stays agnostic of journal semantics.
pub struct JournalRow {
    /// The entry index, shown as `[index]` and used for elision math.
    pub index: u64,
    /// When the entry was appended: the WHEN column (age of the first shown entry,
    /// offsets from it for the others).
    pub appended_at: Option<DateTime<Local>>,
    /// The ENTRY column after `[index]:`, e.g. `Run command`.
    pub entry: String,
    /// The NAME column: the entry's name (runs, sleeps, calls) or, for notifications,
    /// that of the command they complete.
    pub name: Option<String>,
    /// Dimmed lines under the entry (keys, targets, completion links, failures).
    pub details: Vec<String>,
    /// Machine record emitted verbatim in JSON output.
    pub record: Value,
    /// Optional payload block shown indented under the row in human output.
    pub payload: Option<String>,
}

/// How a journal is rendered in human output. JSON always emits every provided row.
#[derive(Clone, Copy)]
pub enum JournalScope {
    /// A preview: rows may skip indices (the caller fetched only a head+tail slice), so
    /// an elision marker (`· · · (N more)`) is printed wherever consecutive rows are not
    /// index-contiguous.
    Preview,
    /// Show every provided row with no elision markers.
    Full,
}

/// An entry of a list view ([`OutputFormatter::list`]): summary columns aligned across
/// the list, optional detail lines under it, and its [`Serialize`] form for `--json`.
pub trait ListItem: Serialize {
    /// Column headers (machine keys, `snake_case`), shared by every item of this type.
    const HEADERS: &'static [&'static str];

    /// Summary cells, one per header.
    fn columns(&self) -> Vec<Field>;

    /// Lines shown dimmed under the item in human output.
    fn details(&self) -> Vec<String> {
        Vec::new()
    }
}

/// A higher-level output sink (think `std::fmt::Formatter`, but for whole command
/// results). Obtain one with [`Formatter::new`]; call the building blocks in any order,
/// then [`finish`](OutputFormatter::finish) once.
///
/// `section` names group the output in the JSON document (`detail` → an object,
/// `table` → an array of objects keyed by header, `value` → a scalar). Human output
/// ignores section names.
pub trait OutputFormatter {
    /// A section heading with a decorative icon (human only; ignored in JSON). The
    /// icon is dropped when colors are disabled.
    fn title(&mut self, icon: &str, title: &str);

    /// A key-value detail view. `rows` are `(key, value)` pairs where `key` is a
    /// machine key (`snake_case`); the human formatter derives a display label.
    /// Accepts any iterable of pairs, owned or borrowed: arrays, slices, or a
    /// `&Vec<(String, Field)>`.
    fn detail<K: AsRef<str>>(
        &mut self,
        section: &str,
        rows: impl IntoIterator<Item = impl Borrow<(K, Field)>>,
    );

    /// A list/collection table. `headers` are machine keys (`snake_case`); each row
    /// aligns positionally with `headers`. Rows can be owned or borrowed (e.g.
    /// `&Vec<Vec<Field>>`, `Vec<[Field; 3]>`).
    fn table(
        &mut self,
        section: &str,
        headers: &[impl AsRef<str>],
        rows: impl IntoIterator<Item = impl AsRef<[Field]>>,
    );

    /// A single scalar value.
    fn value(&mut self, section: &str, field: Field);

    /// A journal-style timeline of indexed entries. Human output renders ENTRY / NAME /
    /// WHEN columns with detail lines and optional payload blocks (head/tail elision
    /// per `scope`); JSON emits `section` as an array of the rows' `record`s.
    fn journal(&mut self, section: &str, rows: &[JournalRow], scope: JournalScope);

    /// A list of items. Human output renders a header row, the items' columns aligned,
    /// and each item's detail lines under it; JSON emits `section` as an array of the
    /// items' serialized form.
    fn list<T: ListItem>(&mut self, section: &str, items: &[T]) -> anyhow::Result<()>;

    /// Suggest a read-only follow-up command, ready to run (real ids filled in).
    /// `description` completes "Run `command` to …". Human output shows the
    /// accumulated steps in one tip at [`finish`](OutputFormatter::finish); JSON emits
    /// them as a top-level `next_steps` array, with ` --json` appended to `command`.
    fn next_step(&mut self, command: &str, description: &str);

    /// Gate a change on confirmation, after the planned changes were written to this
    /// formatter. Returns `Ok(())` when the command should go on and apply them.
    ///
    /// - `--dry-run`: shows the plan, then stops with
    ///   [`DryRunComplete`](exit::DryRunComplete) (exit 0, nothing changed).
    /// - Human: prompts (or auto-confirms with `--yes`).
    /// - JSON without `--yes`: emits the plan document (`"applied": false`, `hint`,
    ///   `apply_command`) and stops with
    ///   [`ConfirmationRequired`](exit::ConfirmationRequired) (exit 3). With `--yes`,
    ///   the final document carries `"dry_run": false, "applied": true`.
    fn confirm(&mut self, dry_run: &DryRun, prompt: &str) -> anyhow::Result<()>;

    /// Flush the output. The JSON formatter emits its accumulated document here.
    fn finish(self) -> anyhow::Result<()>;
}

/// The formatter selected for the current invocation (see [`Formatter::new`]): statically
/// dispatches to [`HumanFormatter`] or [`JsonFormatter`].
pub enum Formatter {
    Human(HumanFormatter),
    Json(JsonFormatter),
}

/// Forward a method call to the selected formatter.
macro_rules! dispatch {
    ($self:ident.$method:ident($($arg:expr),*)) => {
        match $self {
            Formatter::Human(f) => f.$method($($arg),*),
            Formatter::Json(f) => f.$method($($arg),*),
        }
    };
}

impl OutputFormatter for Formatter {
    fn title(&mut self, icon: &str, title: &str) {
        dispatch!(self.title(icon, title))
    }

    fn detail<K: AsRef<str>>(
        &mut self,
        section: &str,
        rows: impl IntoIterator<Item = impl Borrow<(K, Field)>>,
    ) {
        dispatch!(self.detail(section, rows))
    }

    fn table(
        &mut self,
        section: &str,
        headers: &[impl AsRef<str>],
        rows: impl IntoIterator<Item = impl AsRef<[Field]>>,
    ) {
        dispatch!(self.table(section, headers, rows))
    }

    fn value(&mut self, section: &str, field: Field) {
        dispatch!(self.value(section, field))
    }

    fn journal(&mut self, section: &str, rows: &[JournalRow], scope: JournalScope) {
        dispatch!(self.journal(section, rows, scope))
    }

    fn list<T: ListItem>(&mut self, section: &str, items: &[T]) -> anyhow::Result<()> {
        dispatch!(self.list(section, items))
    }

    fn next_step(&mut self, command: &str, description: &str) {
        dispatch!(self.next_step(command, description))
    }

    fn confirm(&mut self, dry_run: &DryRun, prompt: &str) -> anyhow::Result<()> {
        dispatch!(self.confirm(dry_run, prompt))
    }

    fn finish(self) -> anyhow::Result<()> {
        dispatch!(self.finish())
    }
}

/// `--dry-run` for commands that change state: flatten into the command's options
/// and pass to [`OutputFormatter::confirm`]. Per command (not global), so a command
/// that doesn't support previews rejects the flag instead of silently applying.
#[derive(clap::Args, Clone, Default)]
pub struct DryRun {
    /// Show the changes this command would make, without applying them.
    /// Combine with --json for a machine-readable plan.
    #[arg(long)]
    pub dry_run: bool,
}

/// The current command line with `--dry-run` removed and `--yes` added: the command
/// that applies the previewed changes.
fn apply_command() -> String {
    let mut args: Vec<String> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--dry-run" && arg != "--yes" && arg != "-y")
        .collect();
    args.insert(0, "restate".to_owned());
    args.push("--yes".to_owned());
    args.iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Single-quote `arg` for a POSIX shell when it contains anything but safe characters.
fn shell_quote(arg: &str) -> String {
    let safe = |c: char| c.is_ascii_alphanumeric() || "-_./:=@,+%".contains(c);
    if !arg.is_empty() && arg.chars().all(safe) {
        arg.to_owned()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}

impl Formatter {
    /// The formatter for the current invocation: [`JsonFormatter`] when `--json` is
    /// set, otherwise [`HumanFormatter`].
    ///
    /// This is the only place the formatters consult the [`CliContext`]: what they
    /// need from it is captured here, at creation.
    pub fn new() -> Self {
        let ctx = CliContext::get();
        if ctx.json_output() {
            Formatter::Json(JsonFormatter {
                auto_confirm: ctx.auto_confirm(),
                ..Default::default()
            })
        } else {
            Formatter::Human(HumanFormatter::default())
        }
    }
}

impl Default for Formatter {
    fn default() -> Self {
        Self::new()
    }
}

/// JSON key holding the [`OutputFormatter::next_step`] suggestions.
pub(crate) const NEXT_STEPS: &str = "next_steps";

/// Human rendering of a next step: "Run `command` to description.".
pub(crate) fn next_step_line(command: &str, description: &str) -> String {
    format!("Run `{command}` to {description}.")
}

/// JSON rendering of a next step, with ` --json` appended so the agent's next call
/// stays structured too.
pub(crate) fn next_step_json(command: &str, description: &str) -> Value {
    serde_json::json!({
        "command": format!("{command} --json"),
        "description": description,
    })
}

/// Print accumulated next-step lines as one tip on stderr, separated from the output
/// above by a blank line (no-op when empty).
pub(crate) fn print_next_steps(lines: &[String]) {
    if !lines.is_empty() {
        restate_cli_util::c_eprintln!();
        restate_cli_util::c_tip!("{}", lines.join("\n"));
    }
}

/// Renders human-friendly, styled tables through the broken-pipe-safe console sink.
#[derive(Default)]
pub struct HumanFormatter {
    next_steps: Vec<String>,
}

impl OutputFormatter for HumanFormatter {
    fn title(&mut self, icon: &str, title: &str) {
        // `c_title!` builds its own format string, so the text is a single argument.
        restate_cli_util::c_title!(icon, title);
    }

    fn detail<K: AsRef<str>>(
        &mut self,
        _section: &str,
        rows: impl IntoIterator<Item = impl Borrow<(K, Field)>>,
    ) {
        let mut table = Table::new_styled();
        for row in rows {
            let (key, field) = row.borrow();
            table.add_kv_row(
                &format!("{}:", humanize_label(key.as_ref())),
                field.to_cell(),
            );
        }
        restate_cli_util::c_println!("{table}");
    }

    fn table(
        &mut self,
        _section: &str,
        headers: &[impl AsRef<str>],
        rows: impl IntoIterator<Item = impl AsRef<[Field]>>,
    ) {
        let mut table = Table::new_styled();
        table.set_styled_header(headers.iter().map(|h| header_label(h.as_ref())).collect());
        for row in rows {
            table.add_row(row.as_ref().iter().map(Field::to_cell).collect::<Vec<_>>());
        }
        restate_cli_util::c_println!("{table}");
    }

    fn value(&mut self, _section: &str, field: Field) {
        // `c_println!` expands with a trailing semicolon, so call it in statement
        // position rather than as a match-arm expression.
        let rendered = match field.style {
            Some(style) => Styled(style, field.human_string()).to_string(),
            None => field.human_string(),
        };
        restate_cli_util::c_println!("{rendered}");
    }

    fn list<T: ListItem>(&mut self, _section: &str, items: &[T]) -> anyhow::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let headers: Vec<String> = T::HEADERS.iter().map(|h| header_label(h)).collect();
        let rows: Vec<Vec<String>> = items
            .iter()
            .map(|item| item.columns().iter().map(Field::human_display).collect())
            .collect();
        let mut widths: Vec<usize> = headers.iter().map(|h| measure_text_width(h)).collect();
        for row in &rows {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(measure_text_width(cell));
            }
        }
        // Columns separated by two spaces; the last one isn't padded.
        let line = |cells: &[String]| {
            let mut out = String::from(" ");
            for (i, (cell, width)) in cells.iter().zip(&widths).enumerate() {
                out.push_str(cell);
                if i + 1 < cells.len() {
                    out.push_str(&" ".repeat(width - measure_text_width(cell) + 2));
                }
            }
            out
        };

        let bold_headers: Vec<String> = headers
            .iter()
            .map(|h| dialoguer::console::style(h).bold().to_string())
            .collect();
        restate_cli_util::c_println!("{}", line(&bold_headers));
        for (row, item) in rows.iter().zip(items) {
            restate_cli_util::c_println!("{}", line(row));
            let details = item.details();
            for (i, detail) in details.iter().enumerate() {
                if detail.trim().is_empty() {
                    // A blank line inside a multi-line detail keeps the tree going.
                    restate_cli_util::c_println!("   │");
                    continue;
                }
                let branch = if i + 1 == details.len() { "└" } else { "├" };
                restate_cli_util::c_println!(
                    "   {branch} {}",
                    dialoguer::console::style(detail).dim()
                );
            }
        }
        Ok(())
    }

    fn journal(&mut self, _section: &str, rows: &[JournalRow], scope: JournalScope) {
        if rows.is_empty() {
            return;
        }
        // WHEN: the first shown entry's age, then each entry's offset from it.
        let base = rows.iter().find_map(|row| row.appended_at);
        // Zero-pad indices to the widest one (`[07]`), so the entry types line up.
        let index_digits = rows
            .iter()
            .map(|row| row.index.to_string().len())
            .max()
            .unwrap_or_default();
        let cells: Vec<[String; 3]> = rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                let when = match (row.appended_at, base) {
                    (Some(at), Some(base)) if i > 0 => {
                        format!("+{}", compact_duration(at.signed_duration_since(base)))
                    }
                    (Some(at), _) => format!(
                        "{} ago",
                        compact_duration(Local::now().signed_duration_since(at))
                    ),
                    (None, _) => String::new(),
                };
                [
                    format!("[{:0index_digits$}]: {}", row.index, row.entry),
                    row.name.clone().unwrap_or_default(),
                    when,
                ]
            })
            .collect();
        let headers = ["ENTRY", "NAME", "WHEN"].map(str::to_owned);
        let mut widths = headers.clone().map(|h| h.len());
        for row in &cells {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(measure_text_width(cell));
            }
        }
        let line = |cells: &[String; 3]| {
            let mut out = String::from(" ");
            for (i, (cell, width)) in cells.iter().zip(widths).enumerate() {
                out.push_str(cell);
                if i + 1 < cells.len() {
                    out.push_str(&" ".repeat(width - measure_text_width(cell) + 2));
                }
            }
            out.trim_end().to_owned()
        };

        restate_cli_util::c_println!(
            "{}",
            line(&headers.map(|h| dialoguer::console::style(h).bold().to_string()))
        );
        let mut previous_index: Option<u64> = None;
        for (row, cells) in rows.iter().zip(&cells) {
            if matches!(scope, JournalScope::Preview)
                && let Some(previous) = previous_index
                && row.index > previous + 1
            {
                let hidden = row.index - previous - 1;
                restate_cli_util::c_println!("   · · ·   ({hidden} more)");
            }
            restate_cli_util::c_println!("{}", line(cells));
            for (i, detail) in row.details.iter().enumerate() {
                let branch = if i + 1 == row.details.len() {
                    "└"
                } else {
                    "├"
                };
                restate_cli_util::c_println!(
                    "   {branch} {}",
                    dialoguer::console::style(detail).dim()
                );
            }
            if let Some(payload) = &row.payload {
                for payload_line in payload.lines() {
                    restate_cli_util::c_println!("     {payload_line}");
                }
            }
            previous_index = Some(row.index);
        }
    }

    fn next_step(&mut self, command: &str, description: &str) {
        self.next_steps.push(next_step_line(command, description));
    }

    fn confirm(&mut self, dry_run: &DryRun, prompt: &str) -> anyhow::Result<()> {
        if dry_run.dry_run {
            restate_cli_util::c_eprintln!();
            restate_cli_util::c_tip!(
                "Dry run: nothing was changed. To apply these changes, run:\n{}",
                apply_command()
            );
            print_next_steps(&std::mem::take(&mut self.next_steps));
            return Err(exit::DryRunComplete.into());
        }
        confirm_or_exit(prompt)
    }

    fn finish(self) -> anyhow::Result<()> {
        print_next_steps(&self.next_steps);
        Ok(())
    }
}

/// Accumulates one JSON document keyed by section and emits it on [`finish`].
#[derive(Default)]
pub struct JsonFormatter {
    doc: Map<String, Value>,
    next_steps: Vec<Value>,
    /// `--yes` (or CI): [`confirm`](OutputFormatter::confirm) applies instead of
    /// emitting the plan.
    auto_confirm: bool,
}

/// Keys the JSON formatter adds around a [`OutputFormatter::confirm`]ed change.
const DRY_RUN: &str = "dry_run";
const APPLIED: &str = "applied";

impl JsonFormatter {
    fn insert(&mut self, section: &str, value: Value) {
        debug_assert!(
            ![NEXT_STEPS, DRY_RUN, APPLIED, "hint", "apply_command"].contains(&section),
            "`{section}` is a reserved section"
        );
        self.doc.insert(section.to_owned(), value);
    }

    /// The final document: every section, plus `next_steps` when any were added.
    fn into_document(self) -> Value {
        let mut doc = self.doc;
        if !self.next_steps.is_empty() {
            doc.insert(NEXT_STEPS.to_owned(), Value::Array(self.next_steps));
        }
        Value::Object(doc)
    }
}

impl OutputFormatter for JsonFormatter {
    fn title(&mut self, _icon: &str, _title: &str) {
        // Titles are human decoration; nothing to emit in JSON.
    }

    fn detail<K: AsRef<str>>(
        &mut self,
        section: &str,
        rows: impl IntoIterator<Item = impl Borrow<(K, Field)>>,
    ) {
        let obj = rows
            .into_iter()
            .map(|row| {
                let (key, field) = row.borrow();
                (key.as_ref().to_owned(), field.value.clone())
            })
            .collect();
        self.insert(section, Value::Object(obj));
    }

    fn table(
        &mut self,
        section: &str,
        headers: &[impl AsRef<str>],
        rows: impl IntoIterator<Item = impl AsRef<[Field]>>,
    ) {
        let arr = rows
            .into_iter()
            .map(|row| {
                let obj = headers
                    .iter()
                    .zip(row.as_ref())
                    .map(|(header, field)| (header.as_ref().to_owned(), field.value.clone()))
                    .collect();
                Value::Object(obj)
            })
            .collect();
        self.insert(section, Value::Array(arr));
    }

    fn value(&mut self, section: &str, field: Field) {
        self.insert(section, field.value);
    }

    fn list<T: ListItem>(&mut self, section: &str, items: &[T]) -> anyhow::Result<()> {
        let items = items
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        self.insert(section, Value::Array(items));
        Ok(())
    }

    fn journal(&mut self, section: &str, rows: &[JournalRow], _scope: JournalScope) {
        // Elision is a human affordance; JSON emits every provided entry.
        let arr = rows.iter().map(|row| row.record.clone()).collect();
        self.insert(section, Value::Array(arr));
    }

    fn next_step(&mut self, command: &str, description: &str) {
        self.next_steps.push(next_step_json(command, description));
    }

    fn confirm(&mut self, dry_run: &DryRun, _prompt: &str) -> anyhow::Result<()> {
        let apply = !dry_run.dry_run && self.auto_confirm;
        self.doc.insert(DRY_RUN.to_owned(), Value::Bool(!apply));
        self.doc.insert(APPLIED.to_owned(), Value::Bool(apply));
        if apply {
            return Ok(());
        }

        let hint = if dry_run.dry_run {
            "Dry run: nothing was changed. Run the `apply_command` command to apply these changes."
        } else {
            "Confirmation required: nothing was changed. Review the changes, then run \
             the `apply_command` command to apply them."
        };
        self.doc.insert("hint".to_owned(), Value::from(hint));
        self.doc
            .insert("apply_command".to_owned(), Value::from(apply_command()));
        let rendered = serde_json::to_string_pretty(&std::mem::take(self).into_document())?;
        restate_cli_util::c_println!("{rendered}");

        Err(if dry_run.dry_run {
            exit::DryRunComplete.into()
        } else {
            exit::ConfirmationRequired { plan_emitted: true }.into()
        })
    }

    fn finish(self) -> anyhow::Result<()> {
        let rendered = serde_json::to_string_pretty(&self.into_document())?;
        restate_cli_util::c_println!("{rendered}");
        Ok(())
    }
}

/// `snake_case` / `kebab-case` machine key → `Title Case` human label.
fn humanize_label(key: &str) -> String {
    key.split(['_', '-'])
        .filter(|word| !word.is_empty())
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Machine key → `UPPER CASE` table header.
fn header_label(key: &str) -> String {
    key.to_uppercase().replace(['_', '-'], " ")
}

/// A compact duration with at most the three largest units, e.g. `2d 4h 10m`,
/// `5h 3m 12s`, `45s`; below a second, milliseconds (`9ms`).
pub fn compact_duration(duration: chrono::TimeDelta) -> String {
    let millis = duration.num_milliseconds().max(0);
    if millis < 1000 {
        return format!("{millis}ms");
    }
    let secs = millis / 1000;
    let units = [
        (secs / 86400, "d"),
        (secs / 3600 % 24, "h"),
        (secs / 60 % 60, "m"),
        (secs % 60, "s"),
    ];
    let first = units.iter().position(|(n, _)| *n > 0).unwrap_or(3);
    let mut parts: Vec<String> = units[first..(first + 3).min(units.len())]
        .iter()
        .map(|(n, unit)| format!("{n}{unit}"))
        .collect();
    // Drop trailing zero units: `1h 0m 0s` → `1h`.
    while parts.len() > 1 && parts.last().is_some_and(|p| p.starts_with('0')) {
        parts.pop();
    }
    parts.join(" ")
}

/// A journal timestamp in local time, e.g. `2026-09-24 16:23:14.137`.
pub fn journal_time(t: DateTime<Local>) -> String {
    t.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn json_formatter_builds_sectioned_document() {
        // `title` is human-only and must not appear; `detail` becomes an object and
        // `table` an array of objects keyed by header, ignoring any styling.
        let mut jf = JsonFormatter::default();
        jf.title("📜", "ignored");
        jf.detail(
            "service",
            [
                ("name", Field::new("greeter")),
                ("revision", Field::styled(3, Style::Info)),
            ],
        );
        jf.table(
            "handlers",
            &["handler", "public"],
            &[vec![Field::new("greet"), Field::new(true)]],
        );
        let value = jf.into_document();

        assert_eq!(value["service"]["name"], json!("greeter"));
        assert_eq!(value["service"]["revision"], json!(3));
        assert_eq!(value["handlers"][0]["handler"], json!("greet"));
        assert_eq!(value["handlers"][0]["public"], json!(true));
        assert!(value.get("ignored").is_none());
        assert!(value.get(NEXT_STEPS).is_none());
    }

    #[test]
    fn json_next_steps_are_collected_with_json_flag() {
        let mut jf = JsonFormatter::default();
        jf.value("id", Field::new("inv_1"));
        jf.next_step("restate invocations journal inv_1", "see the full journal");
        jf.next_step("restate services list", "list services");
        let value = jf.into_document();

        assert_eq!(value["id"], json!("inv_1"));
        assert_eq!(value[NEXT_STEPS].as_array().map(Vec::len), Some(2));
        assert_eq!(
            value[NEXT_STEPS][0],
            json!({
                "command": "restate invocations journal inv_1 --json",
                "description": "see the full journal",
            })
        );
    }

    #[test]
    fn labels_are_derived_from_machine_keys() {
        assert_eq!(humanize_label("deployment_id").as_str(), "Deployment Id");
        assert_eq!(header_label("deployment-type").as_str(), "DEPLOYMENT TYPE");
    }

    #[test]
    fn json_journal_emits_every_record_ignoring_elision() {
        let rows: Vec<JournalRow> = (0..3)
            .map(|index| JournalRow {
                index,
                appended_at: None,
                entry: format!("entry {index}"),
                name: None,
                details: Vec::new(),
                record: json!({ "index": index }),
                payload: None,
            })
            .collect();

        let mut jf = JsonFormatter::default();
        // Preview would print elision markers in human output; JSON keeps all rows.
        jf.journal("journal", &rows, JournalScope::Preview);
        let value = Value::Object(jf.doc);

        assert_eq!(value["journal"].as_array().map(Vec::len), Some(3));
        assert_eq!(value["journal"][2]["index"], json!(2));
    }

    #[test]
    fn shell_quote_leaves_safe_args_and_quotes_the_rest() {
        assert_eq!(
            shell_quote("http://localhost:9080/"),
            "http://localhost:9080/"
        );
        assert_eq!(shell_quote("--force"), "--force");
        assert_eq!(shell_quote("SELECT 1"), "'SELECT 1'");
        assert_eq!(shell_quote("it's"), r"'it'\''s'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn json_list_emits_serialized_items() {
        #[derive(Serialize)]
        struct Item {
            name: &'static str,
            tags: Vec<u8>,
        }
        impl ListItem for Item {
            const HEADERS: &'static [&'static str] = &["name"];
            fn columns(&self) -> Vec<Field> {
                vec![Field::new(self.name)]
            }
        }

        let mut jf = JsonFormatter::default();
        jf.list(
            "items",
            &[
                Item {
                    name: "a",
                    tags: vec![1],
                },
                Item {
                    name: "b",
                    tags: vec![],
                },
            ],
        )
        .unwrap();

        // The items' own serialized form, not the human columns.
        assert_eq!(
            jf.into_document(),
            json!({"items": [{"name": "a", "tags": [1]}, {"name": "b", "tags": []}]})
        );
    }
}
