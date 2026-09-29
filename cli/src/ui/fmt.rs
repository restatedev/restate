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
//! view, a [`table`](OutputFormatter::table), a single scalar
//! [`value`](OutputFormatter::value), or a list of nested items
//! ([`start_items`](OutputFormatter::start_items)) — and the selected [`OutputFormatter`] decides
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

use restate_cli_util::_unicode_width::UnicodeWidthStr;
use restate_cli_util::ui::console::{Icon, Styled, StyledTable, confirm_or_exit};
use restate_cli_util::ui::stylesheet::Style;
use restate_cli_util::{CliContext, exit};

use crate::error::{ErrorKind, RestateCliError};

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

    /// A `null` without a human rendering: JSON keeps the key, human detail rows skip it.
    fn is_unset(&self) -> bool {
        self.value.is_null() && self.display.is_none()
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
    /// machine key (`snake_case`); the human formatter derives a display label, and
    /// skips rows whose value is `null` without a display (JSON keeps them as `null`).
    /// Accepts any iterable of pairs, owned or borrowed: arrays, slices, or a
    /// `&Vec<(String, Field)>`.
    fn detail<K: AsRef<str>>(
        &mut self,
        section: &str,
        rows: impl IntoIterator<Item = impl Borrow<(K, Field)>>,
    );

    /// A list/collection table. `headers` are machine keys (`snake_case`); each row
    /// aligns positionally with `headers`. Rows can be owned or borrowed (e.g.
    /// `&Vec<Vec<Field>>`, `Vec<[Field; 3]>`). `if_empty` says what human output shows
    /// when there are no rows.
    fn table(
        &mut self,
        section: &str,
        headers: &[impl AsRef<str>],
        rows: impl IntoIterator<Item = impl AsRef<[Field]>>,
        if_empty: IfEmpty,
    );

    /// A single scalar value.
    fn value(&mut self, section: &str, field: Field);

    /// The command's outcome, e.g. `created` or `already_absent`: `section: value` in
    /// JSON; human output prints the field's display as a status line, styled per
    /// `outcome`.
    fn outcome(&mut self, section: &str, field: Field, outcome: Outcome);

    /// One key/value row in the current scope: `key: value` in JSON; in human output a
    /// `Label: value` row, aligned with the adjacent `field` rows (skipped when the value
    /// is `null` without a display, like in `detail`).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "no command writes single fields yet")
    )]
    fn field(&mut self, key: &str, field: Field);

    /// Start the `section` list of nested items (JSON: an array of objects, `[]` when
    /// empty). Describe each item on the formatter returned by [`Items::item`]. Items
    /// and the list are attached when finished or dropped, which gives this formatter
    /// back. Human
    /// output indents each item under a ` - ` marker.
    fn start_items(&mut self, section: &str) -> Items<'_, Self>
    where
        Self: Sized,
    {
        self.begin_items(section);
        Items { parent: self }
    }

    /// Hooks behind [`start_items`](OutputFormatter::start_items), which callers use
    /// instead: they open and close the list, and each item in it.
    fn begin_items(&mut self, section: &str);
    fn begin_item(&mut self);
    fn end_item(&mut self);
    fn end_items(&mut self);

    /// A journal-style timeline of indexed entries. Human output renders ENTRY / NAME /
    /// WHEN columns with detail lines and optional payload blocks (head/tail elision
    /// per `scope`); JSON emits `section` as an array of the rows' `record`s.
    fn journal(&mut self, section: &str, rows: &[JournalRow], scope: JournalScope);

    /// A list of items. Human output renders a header row, the items' columns aligned,
    /// and each item's detail lines under it; JSON emits `section` as an array of the
    /// items' serialized form. `if_empty` says what human output shows when there are
    /// no items.
    fn list<T: ListItem>(
        &mut self,
        section: &str,
        items: &[T],
        if_empty: IfEmpty,
    ) -> anyhow::Result<()>;

    /// Suggest a follow-up command, ready to run (real ids filled in).
    /// `description` completes "Run `command` to …". Human output shows the
    /// accumulated steps in one tip at [`finish`](OutputFormatter::finish); JSON emits
    /// them as a top-level `next_steps` array, with ` --json` appended to `command`
    /// unless `formatting` is [`IncludeFormatting::No`].
    fn next_step(&mut self, command: &str, description: &str, formatting: IncludeFormatting);

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

    /// Report the command's failure, with the next steps suggested so far. Human output
    /// goes to stderr (`Error: …`, the docs link, the causes, then a tip with the next
    /// steps); JSON prints
    /// `{"error": {"kind", "message", "docs_url"?, "causes"?, "next_steps"?}}` on stdout.
    fn error(&mut self, error: &RestateCliError) -> anyhow::Result<()>;

    /// Flush the output. The JSON formatter emits its accumulated document here.
    fn finish(self) -> anyhow::Result<()>;
}

/// A list of nested items being written, see [`OutputFormatter::start_items`]. It is
/// attached to the parent formatter when finished or dropped.
#[must_use = "an unused list is attached empty right away"]
pub struct Items<'a, F: OutputFormatter> {
    parent: &'a mut F,
}

impl<F: OutputFormatter> Items<'_, F> {
    /// Start the next item: describe it on the returned formatter.
    pub fn item(&mut self) -> Item<'_, F> {
        self.parent.begin_item();
        Item {
            parent: &mut *self.parent,
        }
    }

    /// Attach the list to the parent formatter (same as dropping it).
    pub fn finish(self) {}
}

impl<F: OutputFormatter> Drop for Items<'_, F> {
    fn drop(&mut self) {
        self.parent.end_items();
    }
}

/// One item of an [`Items`] list, attached to it when finished or dropped. The
/// command-level calls (`next_step`, `confirm`, `error`) go to the
/// parent formatter.
#[must_use = "an unused item is attached empty right away"]
pub struct Item<'a, F: OutputFormatter> {
    parent: &'a mut F,
}

impl<F: OutputFormatter> OutputFormatter for Item<'_, F> {
    fn title(&mut self, icon: &str, title: &str) {
        self.parent.title(icon, title)
    }

    fn detail<K: AsRef<str>>(
        &mut self,
        section: &str,
        rows: impl IntoIterator<Item = impl Borrow<(K, Field)>>,
    ) {
        self.parent.detail(section, rows)
    }

    fn table(
        &mut self,
        section: &str,
        headers: &[impl AsRef<str>],
        rows: impl IntoIterator<Item = impl AsRef<[Field]>>,
        if_empty: IfEmpty,
    ) {
        self.parent.table(section, headers, rows, if_empty)
    }

    fn value(&mut self, section: &str, field: Field) {
        self.parent.value(section, field)
    }

    fn outcome(&mut self, section: &str, field: Field, outcome: Outcome) {
        self.parent.outcome(section, field, outcome)
    }

    fn field(&mut self, key: &str, field: Field) {
        self.parent.field(key, field)
    }

    fn begin_items(&mut self, section: &str) {
        self.parent.begin_items(section)
    }

    fn begin_item(&mut self) {
        self.parent.begin_item()
    }

    fn end_item(&mut self) {
        self.parent.end_item()
    }

    fn end_items(&mut self) {
        self.parent.end_items()
    }

    fn journal(&mut self, section: &str, rows: &[JournalRow], scope: JournalScope) {
        self.parent.journal(section, rows, scope)
    }

    fn list<T: ListItem>(
        &mut self,
        section: &str,
        items: &[T],
        if_empty: IfEmpty,
    ) -> anyhow::Result<()> {
        self.parent.list(section, items, if_empty)
    }

    fn next_step(&mut self, command: &str, description: &str, formatting: IncludeFormatting) {
        self.parent.next_step(command, description, formatting)
    }

    fn confirm(&mut self, dry_run: &DryRun, prompt: &str) -> anyhow::Result<()> {
        self.parent.confirm(dry_run, prompt)
    }

    fn error(&mut self, error: &RestateCliError) -> anyhow::Result<()> {
        self.parent.error(error)
    }

    /// Attach the item to its list (same as dropping it).
    fn finish(self) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<F: OutputFormatter> Drop for Item<'_, F> {
    fn drop(&mut self) {
        self.parent.end_item();
    }
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
        if_empty: IfEmpty,
    ) {
        dispatch!(self.table(section, headers, rows, if_empty))
    }

    fn value(&mut self, section: &str, field: Field) {
        dispatch!(self.value(section, field))
    }

    fn outcome(&mut self, section: &str, field: Field, outcome: Outcome) {
        dispatch!(self.outcome(section, field, outcome))
    }

    fn field(&mut self, key: &str, field: Field) {
        dispatch!(self.field(key, field))
    }

    fn begin_items(&mut self, section: &str) {
        dispatch!(self.begin_items(section))
    }

    fn begin_item(&mut self) {
        dispatch!(self.begin_item())
    }

    fn end_item(&mut self) {
        dispatch!(self.end_item())
    }

    fn end_items(&mut self) {
        dispatch!(self.end_items())
    }

    fn journal(&mut self, section: &str, rows: &[JournalRow], scope: JournalScope) {
        dispatch!(self.journal(section, rows, scope))
    }

    fn list<T: ListItem>(
        &mut self,
        section: &str,
        items: &[T],
        if_empty: IfEmpty,
    ) -> anyhow::Result<()> {
        dispatch!(self.list(section, items, if_empty))
    }

    fn next_step(&mut self, command: &str, description: &str, formatting: IncludeFormatting) {
        dispatch!(self.next_step(command, description, formatting))
    }

    fn confirm(&mut self, dry_run: &DryRun, prompt: &str) -> anyhow::Result<()> {
        dispatch!(self.confirm(dry_run, prompt))
    }

    fn finish(self) -> anyhow::Result<()> {
        dispatch!(self.finish())
    }

    fn error(&mut self, error: &RestateCliError) -> anyhow::Result<()> {
        dispatch!(self.error(error))
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
    let mut args = command_args(|arg| matches!(arg, "--dry-run" | "--yes" | "-y"));
    args.push("--yes".to_owned());
    join_command(args)
}

/// The current command line without `--json`, to suggest as a next step (JSON output
/// appends `--json` again): e.g. to retry after a concurrent change.
pub(crate) fn rerun_command() -> String {
    join_command(command_args(|arg| arg == "--json"))
}

/// `restate` and the current arguments, without those `drop` matches.
fn command_args(drop: impl Fn(&str) -> bool) -> Vec<String> {
    std::iter::once("restate".to_owned())
        .chain(std::env::args().skip(1).filter(|arg| !drop(arg)))
        .collect()
}

/// `args` as one shell command line, secrets redacted.
fn join_command(args: Vec<String>) -> String {
    redact_secrets(args)
        .iter()
        .map(|arg| shell_quote(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Flags whose `name=value` argument may carry credentials (e.g. `authorization`
/// headers); their values are redacted before a command line is echoed back.
const SECRET_FLAGS: &[&str] = &["--extra-header"];

/// Replace the value of every [`SECRET_FLAGS`] argument (`--flag name=value` or
/// `--flag=name=value`) with `<REDACTED>`, keeping the name.
fn redact_secrets(args: Vec<String>) -> Vec<String> {
    let redact = |value: &str| match value.split_once('=') {
        Some((name, _)) => format!("{name}=<REDACTED>"),
        None => "<REDACTED>".to_owned(),
    };
    let mut redact_next = false;
    args.into_iter()
        .map(|arg| {
            if std::mem::take(&mut redact_next) {
                return redact(&arg);
            }
            if SECRET_FLAGS.contains(&arg.as_str()) {
                redact_next = true;
                return arg;
            }
            match arg.split_once('=') {
                Some((flag, value)) if SECRET_FLAGS.contains(&flag) => {
                    format!("{flag}={}", redact(value))
                }
                _ => arg,
            }
        })
        .collect()
}

/// Single-quote `arg` for a POSIX shell when it contains anything but safe characters.
pub(crate) fn shell_quote(arg: &str) -> String {
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
const NEXT_STEPS: &str = "next_steps";

/// Human rendering of a next step: "Run `command` to description.".
fn next_step_line(command: &str, description: &str) -> String {
    format!("Run `{command}` to {description}.")
}

/// Whether the JSON rendering of a [`OutputFormatter::next_step`] appends ` --json` to
/// its command, so the agent's next call stays structured too. `No` is for commands
/// where ` --json` makes no sense (e.g. `--help`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncludeFormatting {
    Yes,
    No,
}

/// How human output shows an [`OutputFormatter::outcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The command reached what was asked, including when there was nothing to change
    /// (`✅`/`[OK]:` on stdout).
    Success,
    /// The command could not do what was asked (`❌`/`[ERR]:` on stderr).
    #[expect(dead_code, reason = "no command reports a failed outcome yet")]
    Failure,
}

/// What human output shows for an empty [`OutputFormatter::list`] or
/// [`OutputFormatter::table`]. JSON always emits `[]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IfEmpty<'a> {
    /// Nothing, e.g. when the surrounding output already says it.
    Nothing,
    /// This message, where the rows would be.
    Say(&'a str),
}

/// JSON rendering of a next step.
fn next_step_json(command: &str, description: &str, formatting: IncludeFormatting) -> Value {
    let command = match formatting {
        IncludeFormatting::Yes => format!("{command} --json"),
        IncludeFormatting::No => command.to_owned(),
    };
    serde_json::json!({
        "command": command,
        "description": description,
    })
}

/// Print accumulated next-step lines as one tip on stderr, separated from the output
/// above by a blank line (no-op when empty).
fn print_next_steps(lines: &[String]) {
    if !lines.is_empty() {
        restate_cli_util::c_eprintln!();
        restate_cli_util::c_tip!("{}", lines.join("\n"));
    }
}

/// Renders human-friendly, styled tables through the broken-pipe-safe console sink.
#[derive(Default)]
pub struct HumanFormatter {
    next_steps: Vec<String>,
    /// How many items the current output is nested in.
    depth: usize,
    /// Items started so far in each open list, to separate them with a blank line.
    item_counts: Vec<usize>,
    /// The next line opens an item, so it gets the ` - ` marker.
    item_start: bool,
    /// `field` rows not printed yet, so adjacent ones align in one table.
    fields: Vec<(String, Field)>,
    /// Something was printed already, so a top-level title needs a blank line above.
    printed: bool,
}

impl HumanFormatter {
    /// Print `text` on stdout, indented to the current depth.
    /// The [`IfEmpty`] message of an empty list or table, indented like its rows.
    fn empty(&mut self, if_empty: IfEmpty) {
        if let IfEmpty::Say(message) = if_empty {
            self.println(&format!(" {message}"));
        }
    }

    fn println(&mut self, text: &str) {
        for line in text.lines() {
            if line.is_empty() {
                restate_cli_util::c_println!();
                continue;
            }
            let mut prefix = "  ".repeat(self.depth);
            if std::mem::take(&mut self.item_start) {
                // Lines start with a space, so ` -` lines up with the item's other rows.
                prefix.truncate(prefix.len() - 2);
                prefix.push_str(" -");
            }
            restate_cli_util::c_println!("{prefix}{line}");
            self.printed = true;
        }
    }

    /// Print the buffered `field` rows as one key/value table.
    fn flush_fields(&mut self) {
        if self.fields.is_empty() {
            return;
        }
        let mut table = Table::new_styled();
        for (key, field) in std::mem::take(&mut self.fields) {
            if field.is_unset() {
                continue;
            }
            table.add_kv_row(&format!("{}:", humanize_label(&key)), field.to_cell());
        }
        self.println(&table.to_string());
    }
}

/// Left-aligned columns sized to their widest cell, as used by `list` and `journal`.
struct Columns(Vec<usize>);

impl Columns {
    fn new(headers: &[impl AsRef<str>], rows: &[impl AsRef<[String]>]) -> Self {
        let mut widths: Vec<usize> = headers
            .iter()
            .map(|h| measure_text_width(h.as_ref()))
            .collect();
        for row in rows {
            for (width, cell) in widths.iter_mut().zip(row.as_ref()) {
                *width = (*width).max(measure_text_width(cell));
            }
        }
        Self(widths)
    }

    /// The bold header row; widths were measured on the unstyled text.
    fn header(&self, headers: &[impl AsRef<str>]) -> String {
        let bold: Vec<String> = headers
            .iter()
            .map(|h| dialoguer::console::style(h.as_ref()).bold().to_string())
            .collect();
        self.line(&bold)
    }

    /// Columns separated by two spaces; the last one isn't padded.
    fn line(&self, cells: &[String]) -> String {
        let mut out = String::from(" ");
        for (i, (cell, width)) in cells.iter().zip(&self.0).enumerate() {
            out.push_str(cell);
            if i + 1 < cells.len() {
                out.push_str(&" ".repeat(width - measure_text_width(cell) + 2));
            }
        }
        out
    }
}

impl OutputFormatter for HumanFormatter {
    fn title(&mut self, icon: &str, title: &str) {
        self.flush_fields();
        if self.depth == 0 {
            // Separated from the output above, but not from the command line.
            if self.printed {
                restate_cli_util::c_println!();
            }
            // The icon renders empty without colors; don't leave a leading space then.
            let icon = Icon(icon, "").to_string();
            let title = if icon.is_empty() {
                format!("{title}:")
            } else {
                format!("{icon} {title}:")
            };
            restate_cli_util::c_println!("{title}");
            restate_cli_util::c_println!("{}", "―".repeat(title.width_cjk()));
            self.printed = true;
            return;
        }
        let title = format!("{title}:");
        let underline = "―".repeat(measure_text_width(&title));
        self.println(&format!("\n {title}\n {underline}"));
    }

    fn detail<K: AsRef<str>>(
        &mut self,
        _section: &str,
        rows: impl IntoIterator<Item = impl Borrow<(K, Field)>>,
    ) {
        self.flush_fields();
        for row in rows {
            let (key, field) = row.borrow();
            self.fields.push((key.as_ref().to_owned(), field.clone()));
        }
        self.flush_fields();
    }

    fn table(
        &mut self,
        _section: &str,
        headers: &[impl AsRef<str>],
        rows: impl IntoIterator<Item = impl AsRef<[Field]>>,
        if_empty: IfEmpty,
    ) {
        self.flush_fields();
        let mut rows = rows.into_iter().peekable();
        // An empty table would be a lone header row; JSON still gets `[]`.
        if rows.peek().is_none() {
            self.empty(if_empty);
            return;
        }
        let mut table = Table::new_styled();
        table.set_styled_header(headers.iter().map(|h| header_label(h.as_ref())).collect());
        for row in rows {
            table.add_row(row.as_ref().iter().map(Field::to_cell).collect::<Vec<_>>());
        }
        self.println(&table.to_string());
    }

    fn outcome(&mut self, _section: &str, field: Field, outcome: Outcome) {
        self.flush_fields();
        let message = field.human_display();
        match outcome {
            Outcome::Success => restate_cli_util::c_success!("{message}"),
            Outcome::Failure => restate_cli_util::c_error!("{message}"),
        }
        self.printed = true;
    }

    fn value(&mut self, _section: &str, field: Field) {
        self.flush_fields();
        // Indented like the rows of detail tables and lists.
        for line in field.human_display().lines() {
            self.println(&format!(" {line}"));
        }
    }

    fn field(&mut self, key: &str, field: Field) {
        self.fields.push((key.to_owned(), field));
    }

    fn begin_items(&mut self, _section: &str) {
        self.flush_fields();
        self.item_counts.push(0);
    }

    fn begin_item(&mut self) {
        self.flush_fields();
        if let Some(count) = self.item_counts.last_mut() {
            *count += 1;
            if *count > 1 {
                restate_cli_util::c_println!();
            }
        }
        self.depth += 1;
        self.item_start = true;
    }

    fn end_item(&mut self) {
        self.flush_fields();
        self.depth -= 1;
        self.item_start = false;
    }

    fn end_items(&mut self) {
        self.item_counts.pop();
    }

    fn list<T: ListItem>(
        &mut self,
        _section: &str,
        items: &[T],
        if_empty: IfEmpty,
    ) -> anyhow::Result<()> {
        self.flush_fields();
        if items.is_empty() {
            self.empty(if_empty);
            return Ok(());
        }
        let headers: Vec<String> = T::HEADERS.iter().map(|h| header_label(h)).collect();
        let rows: Vec<Vec<String>> = items
            .iter()
            .map(|item| item.columns().iter().map(Field::human_display).collect())
            .collect();
        let columns = Columns::new(&headers, &rows);
        self.println(&columns.header(&headers));
        for (row, item) in rows.iter().zip(items) {
            self.println(&columns.line(row));
            let details = item.details();
            for (i, detail) in details.iter().enumerate() {
                if detail.trim().is_empty() {
                    // A blank line inside a multi-line detail keeps the bracket going.
                    self.println("   │");
                    continue;
                }
                let branch = if i + 1 == details.len() { "└" } else { "│" };
                self.println(&format!(
                    "   {branch} {}",
                    dialoguer::console::style(detail).dim()
                ));
            }
        }
        Ok(())
    }

    fn journal(&mut self, _section: &str, rows: &[JournalRow], scope: JournalScope) {
        self.flush_fields();
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
        let headers = ["ENTRY", "NAME", "WHEN"];
        let columns = Columns::new(&headers, &cells);
        self.println(&columns.header(&headers));
        let mut previous_index: Option<u64> = None;
        for (row, cells) in rows.iter().zip(&cells) {
            if matches!(scope, JournalScope::Preview)
                && let Some(previous) = previous_index
                && row.index > previous + 1
            {
                let hidden = row.index - previous - 1;
                self.println(&format!("   · · ·   ({hidden} more)"));
            }
            // NAME and WHEN can be empty, which would leave trailing padding.
            self.println(columns.line(cells).trim_end());
            for (i, detail) in row.details.iter().enumerate() {
                let branch = if i + 1 == row.details.len() {
                    "└"
                } else {
                    "├"
                };
                self.println(&format!(
                    "   {branch} {}",
                    dialoguer::console::style(detail).dim()
                ));
            }
            if let Some(payload) = &row.payload {
                for payload_line in payload.lines() {
                    self.println(&format!("     {payload_line}"));
                }
            }
            previous_index = Some(row.index);
        }
    }

    fn next_step(&mut self, command: &str, description: &str, _formatting: IncludeFormatting) {
        self.next_steps.push(next_step_line(command, description));
    }

    fn confirm(&mut self, dry_run: &DryRun, prompt: &str) -> anyhow::Result<()> {
        self.flush_fields();
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

    fn finish(mut self) -> anyhow::Result<()> {
        self.flush_fields();
        if self.next_steps.is_empty() {
            // Keep the shell prompt off the last line of output (the next-steps tip
            // brings its own leading blank line).
            restate_cli_util::c_println!();
        }
        print_next_steps(&self.next_steps);
        Ok(())
    }

    fn error(&mut self, error: &RestateCliError) -> anyhow::Result<()> {
        self.flush_fields();
        restate_cli_util::c_eprintln!("{}{}", Styled(Style::Danger, "Error: "), error.message());
        if let Some(docs_url) = error.docs_url() {
            restate_cli_util::c_eprintln!("  -> See {}", Styled(Style::Info, docs_url));
        }
        let causes: Vec<_> = error.causes().collect();
        if !causes.is_empty() {
            restate_cli_util::c_eprintln!();
            restate_cli_util::c_eprintln!("{}", Styled(Style::Warn, "Caused by:"));
            let last = causes.len() - 1;
            for (i, cause) in causes.iter().enumerate() {
                let symbol = if i == last { "└─" } else { "├─" };
                restate_cli_util::c_eprintln!("  {symbol} {cause}");
            }
        }
        print_next_steps(&self.next_steps);
        Ok(())
    }
}

/// Accumulates one JSON document keyed by section and emits it on [`finish`].
#[derive(Default)]
pub struct JsonFormatter {
    doc: Map<String, Value>,
    /// The open [`start_items`](OutputFormatter::start_items) lists and items, innermost
    /// last: sections go to the innermost open item, else to `doc`.
    open: Vec<Open>,
    next_steps: Vec<Value>,
    /// `--yes` (or CI): [`confirm`](OutputFormatter::confirm) applies instead of
    /// emitting the plan.
    auto_confirm: bool,
}

/// An open list or item of the [`JsonFormatter`].
enum Open {
    Items { section: String, items: Vec<Value> },
    Item(Map<String, Value>),
}

/// The JSON `error` object of a failed command, see [`OutputFormatter::error`].
#[derive(Serialize)]
struct ErrorReport {
    kind: ErrorKind,
    message: String,
    /// Where the Restate error code is documented.
    #[serde(skip_serializing_if = "Option::is_none")]
    docs_url: Option<String>,
    /// The underlying errors, outermost first.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    causes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    next_steps: Vec<Value>,
}

/// Strip ANSI CSI escape sequences: messages may be styled for the terminal, even with
/// `--json`.
fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for next in chars.by_ref() {
                if next == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
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
        let target = match self.open.last_mut() {
            Some(Open::Item(item)) => item,
            _ => &mut self.doc,
        };
        target.insert(section.to_owned(), value);
    }

    /// The failure document: `error`, with the next steps nested in it.
    fn error_document(self, error: &RestateCliError) -> Value {
        let report = ErrorReport {
            kind: error.kind(),
            message: strip_ansi(error.message()),
            docs_url: error.docs_url(),
            causes: error
                .causes()
                .map(|cause| strip_ansi(&cause.to_string()))
                .collect(),
            next_steps: self.next_steps,
        };
        serde_json::json!({ "error": report })
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
        _if_empty: IfEmpty,
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

    fn outcome(&mut self, section: &str, field: Field, _outcome: Outcome) {
        self.insert(section, field.value);
    }

    fn field(&mut self, key: &str, field: Field) {
        self.insert(key, field.value);
    }

    fn begin_items(&mut self, section: &str) {
        self.open.push(Open::Items {
            section: section.to_owned(),
            items: Vec::new(),
        });
    }

    fn begin_item(&mut self) {
        self.open.push(Open::Item(Map::new()));
    }

    fn end_item(&mut self) {
        if let Some(Open::Item(item)) = self.open.pop()
            && let Some(Open::Items { items, .. }) = self.open.last_mut()
        {
            items.push(Value::Object(item));
        }
    }

    fn end_items(&mut self) {
        if let Some(Open::Items { section, items }) = self.open.pop() {
            self.insert(&section, Value::Array(items));
        }
    }

    fn list<T: ListItem>(
        &mut self,
        section: &str,
        items: &[T],
        _if_empty: IfEmpty,
    ) -> anyhow::Result<()> {
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

    fn next_step(&mut self, command: &str, description: &str, formatting: IncludeFormatting) {
        self.next_steps
            .push(next_step_json(command, description, formatting));
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

    fn error(&mut self, error: &RestateCliError) -> anyhow::Result<()> {
        let rendered = serde_json::to_string_pretty(&std::mem::take(self).error_document(error))?;
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
    fn columns_pad_to_widest_cell_except_last() {
        let rows = [
            ["[0]: Input".to_owned(), String::new(), "5s ago".to_owned()],
            ["[1]: Run".to_owned(), "load".to_owned(), String::new()],
        ];
        let columns = Columns::new(&["ENTRY", "NAME", "WHEN"], &rows);

        assert_eq!(columns.line(&rows[0]), " [0]: Input        5s ago");
        // An empty last cell leaves the padding, which `journal` trims.
        assert_eq!(columns.line(&rows[1]), " [1]: Run    load  ");
    }

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
            IfEmpty::Nothing,
        );
        jf.outcome(
            "result",
            Field::with_display("created", "Created service 'greeter'"),
            Outcome::Success,
        );
        let value = jf.into_document();

        assert_eq!(value["result"], json!("created"));
        assert_eq!(value["service"]["name"], json!("greeter"));
        assert_eq!(value["service"]["revision"], json!(3));
        assert_eq!(value["handlers"][0]["handler"], json!("greet"));
        assert_eq!(value["handlers"][0]["public"], json!(true));
        assert!(value.get("ignored").is_none());
        assert!(value.get(NEXT_STEPS).is_none());
    }

    #[test]
    fn json_items_nest_objects_and_forward_next_steps() {
        let mut jf = JsonFormatter::default();
        let mut services = jf.start_items("services");
        let mut service = services.item();
        service.field("name", Field::styled("Greeter", Style::Info));
        service.table(
            "handlers",
            &["handler"],
            &[vec![Field::new("greet")]],
            IfEmpty::Nothing,
        );
        let mut tags = service.start_items("tags");
        let mut tag = tags.item();
        tag.field("tag", Field::new("beta"));
        tag.finish().unwrap();
        tags.finish();
        service.next_step(
            "restate services list",
            "list services",
            IncludeFormatting::Yes,
        );
        service.finish().unwrap();
        services.finish();
        jf.start_items("empty").finish();
        // Dropping attaches too.
        {
            let mut dropped = jf.start_items("dropped");
            dropped.item().field("name", Field::new("Counter"));
        }
        jf.value("id", Field::new("dp_1"));
        let value = jf.into_document();

        assert_eq!(
            value,
            json!({
                "services": [{
                    "name": "Greeter",
                    "handlers": [{"handler": "greet"}],
                    "tags": [{"tag": "beta"}],
                }],
                "empty": [],
                "dropped": [{"name": "Counter"}],
                "id": "dp_1",
                NEXT_STEPS: [{
                    "command": "restate services list --json",
                    "description": "list services",
                }],
            })
        );
    }

    #[test]
    fn json_next_steps_are_collected_with_json_flag() {
        let mut jf = JsonFormatter::default();
        jf.value("id", Field::new("inv_1"));
        jf.next_step(
            "restate invocations journal inv_1",
            "see the full journal",
            IncludeFormatting::Yes,
        );
        jf.next_step(
            "restate services list",
            "list services",
            IncludeFormatting::Yes,
        );
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
            IfEmpty::Say("No items."),
        )
        .unwrap();

        // The items' own serialized form, not the human columns.
        assert_eq!(
            jf.into_document(),
            json!({"items": [{"name": "a", "tags": [1]}, {"name": "b", "tags": []}]})
        );
    }

    #[test]
    fn json_error_document_nests_next_steps_and_omits_empty_fields() {
        let err = RestateCliError::from_error(
            ErrorKind::Network,
            &*anyhow::anyhow!("\u{1b}[31mtcp connect error\u{1b}[0m").context("Unable to connect"),
        );
        let mut jf = JsonFormatter::default();
        jf.next_step(
            "restate whoami",
            "check the admin URL",
            IncludeFormatting::Yes,
        );
        assert_eq!(
            jf.error_document(&err),
            json!({"error": {
                "kind": "network",
                "message": "Unable to connect",
                "causes": ["tcp connect error"],
                "next_steps": [{"command": "restate whoami --json", "description": "check the admin URL"}],
            }})
        );
        assert_eq!(
            JsonFormatter::default().error_document(&RestateCliError::not_found("nope")),
            json!({"error": {"kind": "not_found", "message": "nope"}})
        );
    }

    #[test]
    fn redact_secrets_hides_header_values() {
        let args = |args: &[&str]| args.iter().map(|a| a.to_string()).collect::<Vec<_>>();
        assert_eq!(
            redact_secrets(args(&[
                "restate",
                "deployments",
                "register",
                "http://localhost:9080/",
                "--extra-header",
                "authorization=Bearer s3cret",
                "--extra-header=x-api-key=abc",
                "--force",
            ])),
            args(&[
                "restate",
                "deployments",
                "register",
                "http://localhost:9080/",
                "--extra-header",
                "authorization=<REDACTED>",
                "--extra-header=x-api-key=<REDACTED>",
                "--force",
            ])
        );
    }
}
