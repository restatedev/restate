// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Some of the code in this file has been taken from tokio-rs/tracing
// https://github.com/tokio-rs/tracing/blob/8aae1c37b091963aafdd336b1168fe5a24c0b4f0/tracing-subscriber/src/fmt/format/pretty.rs
// License MIT

use super::*;

use nu_ansi_term::{Color, Style};
use std::fmt;
use std::fmt::Debug;
use std::fmt::Write;
use tracing::{
    Event, Level, Subscriber,
    field::{self, Field},
    span,
};
use tracing_subscriber::field::{MakeVisitor, RecordFields, VisitFmt, VisitOutput};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields};
use tracing_subscriber::registry::LookupSpan;

struct FmtLevel<'a> {
    level: &'a Level,
    ansi: bool,
}

impl<'a> FmtLevel<'a> {
    pub(crate) fn new(level: &'a Level, ansi: bool) -> Self {
        Self { level, ansi }
    }
}

impl Display for FmtLevel<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ansi {
            write!(f, "{}", level_color(self.level).paint(self.level.as_str()))
        } else {
            fmt::Display::fmt(&self.level, f)
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Pretty<T> {
    display_thread_name: bool,
    display_thread_id: bool,
    timer: T,
}

impl<T: Default> Default for Pretty<T> {
    fn default() -> Self {
        Self {
            display_thread_name: true,
            display_thread_id: false,
            timer: Default::default(),
        }
    }
}

impl<T: FormatTime> Pretty<T> {
    #[inline]
    fn format_timestamp(&self, writer: &mut Writer<'_>) -> fmt::Result
    where
        T: FormatTime,
    {
        // If ANSI color codes are enabled, format the timestamp with ANSI
        // colors.
        {
            if writer.has_ansi_escapes() {
                let style = Style::new().dimmed();
                write!(writer, "{}", style.prefix())?;

                // If getting the timestamp failed, don't bail --- only bail on
                // formatting errors.
                if self.timer.format_time(writer).is_err() {
                    writer.write_str("<unknown time>")?;
                }

                write!(writer, "{} ", style.suffix())?;
                return Ok(());
            }
        }

        // Otherwise, just format the timestamp without ANSI formatting.
        // If getting the timestamp failed, don't bail --- only bail on
        // formatting errors.
        if self.timer.format_time(writer).is_err() {
            writer.write_str("<unknown time>")?;
        }
        writer.write_char(' ')
    }
}

impl<C, N, T> FormatEvent<C, N> for Pretty<T>
where
    C: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    T: FormatTime,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, C, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();

        self.format_timestamp(&mut writer)?;

        let style = if writer.has_ansi_escapes() {
            level_style(meta.level())
        } else {
            Style::new()
        };

        write!(
            writer,
            "{} ",
            FmtLevel::new(meta.level(), writer.has_ansi_escapes())
        )?;

        let target_style = if writer.has_ansi_escapes() {
            style.bold().italic()
        } else {
            style
        };
        write!(
            writer,
            "{}{}{}",
            target_style.prefix(),
            meta.target(),
            target_style.infix(style)
        )?;

        let mut v = PrettyVisitor::new(writer.by_ref(), style, true, true);
        event.record(&mut v);
        v.finish()?;

        // Pretty print the restate error code description, if present
        let mut restate_error_code_and_details_visitor =
            RestateErrorCodeAndDetailsWriter::new(writer.by_ref(), style);
        event.record(&mut restate_error_code_and_details_visitor);
        restate_error_code_and_details_visitor.finish()?;
        writer.write_char('\n')?;

        let dimmed = if writer.has_ansi_escapes() {
            Style::new().dimmed().italic()
        } else {
            Style::new()
        };
        let span_name_style = if writer.has_ansi_escapes() {
            Style::new().bold().italic()
        } else {
            Style::new()
        };

        let thread = self.display_thread_name || self.display_thread_id;

        if thread {
            write!(writer, "{} ", dimmed.paint("on"))?;
            let thread = std::thread::current();
            if self.display_thread_name
                && let Some(name) = thread.name()
            {
                write!(writer, "{name}")?;
                if self.display_thread_id {
                    writer.write_char(' ')?;
                }
            }
            if self.display_thread_id {
                write!(writer, "{:?}", thread.id())?;
            }
            writer.write_char('\n')?;
        }

        let span = event
            .parent()
            .and_then(|id| ctx.span(id))
            .or_else(|| ctx.lookup_current());

        let scope = span.into_iter().flat_map(|span| span.scope());

        for span in scope {
            let meta = span.metadata();

            write!(
                writer,
                "  {} {}{}::{}{}",
                dimmed.paint("in"),
                span_name_style.prefix(),
                meta.target(),
                meta.name(),
                span_name_style.suffix()
            )?;

            let ext = span.extensions();
            let fields = &ext
                .get::<FormattedFields<N>>()
                .expect("Unable to find FormattedFields in extensions; this is a bug");
            if !fields.is_empty() {
                write!(writer, "{fields}")?;
            }
            writer.write_char('\n')?;
        }

        Ok(())
    }
}

impl<'writer, T> FormatFields<'writer> for Pretty<T> {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'writer>, fields: R) -> fmt::Result {
        let mut v = PrettyVisitor::new(writer, Style::default(), false, false);
        fields.record(&mut v);
        v.finish()
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &span::Record<'_>,
    ) -> fmt::Result {
        let writer = current.as_writer();
        let mut v = PrettyVisitor::new(writer, Style::default(), false, false);
        fields.record(&mut v);
        v.finish()
    }
}

/// The [visitor] produced by [`Pretty`]'s [`MakeVisitor`] implementation.
///
/// [visitor]: field::Visit
/// [`MakeVisitor`]: crate::field::MakeVisitor
#[derive(Debug)]
pub struct PrettyVisitor<'a> {
    writer: WriterWrapper<'a>,
    skip_restate_code: bool,
    skip_restate_invocation_error_stacktrace: bool,
}

#[derive(Debug, Default)]
pub struct PrettyFields;

impl<'a> MakeVisitor<Writer<'a>> for PrettyFields {
    type Visitor = PrettyVisitor<'a>;

    #[inline]
    fn make_visitor(&self, target: Writer<'a>) -> Self::Visitor {
        PrettyVisitor::new(target, Style::default(), false, false)
    }
}

// === impl PrettyVisitor ===

impl<'a> PrettyVisitor<'a> {
    pub fn new(
        writer: Writer<'a>,
        style: Style,
        skip_restate_code: bool,
        skip_restate_error_details: bool,
    ) -> Self {
        Self {
            writer: WriterWrapper::new(writer, style),
            skip_restate_code,
            skip_restate_invocation_error_stacktrace: skip_restate_error_details,
        }
    }
}

impl field::Visit for PrettyVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if self.writer.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.record_debug(field, &format_args!("{value}"))
        } else {
            self.record_debug(field, &value)
        }
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        if let Some(source) = value.source() {
            self.record_debug(
                field,
                &format_args!("{}, {}.sources: {}", value, field, ErrorSourceList(source)),
            )
        } else {
            self.record_debug(field, &format_args!("{value}"))
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.writer.result.is_err() {
            return;
        }
        match field.name() {
            "message" => {
                let bold = self.writer.bold();
                self.writer.write_padded(
                    &format_args!(
                        "{}{:?}{}",
                        bold.prefix(),
                        value,
                        bold.infix(self.writer.style())
                    ),
                    MESSAGE_INDENT,
                    MESSAGE_NEW_LINE_INDENT,
                )
            }
            "error" => {
                let bold = self.writer.bold();
                self.writer.write_padded(
                    &format_args!(
                        "{}error{}: {value:?}",
                        bold.prefix(),
                        bold.infix(self.writer.style)
                    ),
                    FIELD_INDENT,
                    FIELD_NEW_LINE_INDENT,
                )
            }
            RESTATE_ERROR_CODE if self.skip_restate_code => {
                // skip, this is printed by the restate specific event printer below
            }
            RESTATE_INVOCATION_ERROR_STACKTRACE
                if self.skip_restate_invocation_error_stacktrace =>
            {
                // skip, this is printed by the restate specific event printer below
            }
            RESTATE_INVOCATION_ID => {
                let bold = self.writer.bold();
                let italic = self.writer.italic();
                self.writer.write_padded(
                    &format_args!(
                        "{}{RESTATE_INVOCATION_ID}{}: {}{value:?}{}",
                        bold.prefix(),
                        bold.infix(self.writer.style()),
                        italic.prefix(),
                        italic.infix(self.writer.style())
                    ),
                    FIELD_INDENT,
                    FIELD_NEW_LINE_INDENT,
                )
            }
            RESTATE_INVOCATION_TARGET => {
                let bold = self.writer.bold();
                let italic = self.writer.italic();
                self.writer.write_padded(
                    &format_args!(
                        "{}{RESTATE_INVOCATION_TARGET}{}: {}{value:?}{}",
                        bold.prefix(),
                        bold.infix(self.writer.style()),
                        italic.prefix(),
                        italic.infix(self.writer.style())
                    ),
                    FIELD_INDENT,
                    FIELD_NEW_LINE_INDENT,
                )
            }
            name if name.starts_with("r#") => self.writer.write_padded(
                &format_args!("{}: {:?}", &name[2..], value),
                FIELD_INDENT,
                FIELD_NEW_LINE_INDENT,
            ),
            name => self.writer.write_padded(
                &format_args!("{name}: {value:?}"),
                FIELD_INDENT,
                FIELD_NEW_LINE_INDENT,
            ),
        };
    }
}

impl VisitOutput<fmt::Result> for PrettyVisitor<'_> {
    fn finish(self) -> fmt::Result {
        self.writer.end()
    }
}

impl VisitFmt for PrettyVisitor<'_> {
    fn writer(&mut self) -> &mut dyn fmt::Write {
        &mut self.writer.writer
    }
}

struct ErrorSourceList<'a>(&'a (dyn std::error::Error + 'static));

impl Display for ErrorSourceList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        let mut curr = Some(self.0);
        while let Some(curr_err) = curr {
            list.entry(&format_args!("{curr_err}"));
            curr = curr_err.source();
        }
        list.finish()
    }
}

// --- Visitor to record as alternate the restate.error.code field

#[derive(Debug)]
struct RestateErrorCodeAndDetailsWriter<'a>(WriterWrapper<'a>);

impl<'a> RestateErrorCodeAndDetailsWriter<'a> {
    pub fn new(writer: Writer<'a>, style: Style) -> Self {
        Self(WriterWrapper::new(writer, style))
    }
}

impl field::Visit for RestateErrorCodeAndDetailsWriter<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == RESTATE_ERROR_CODE {
            self.0.write_padded(
                &format_args!("{RESTATE_ERROR_CODE}: {value:?}"),
                FIELD_INDENT,
                FIELD_NEW_LINE_INDENT,
            );

            let italic_bold = self.0.italic_bold();
            self.0.write_padded(&format_args!(
                "Visit {}https://docs.restate.dev/references/errors#{value:?}{} for more info and suggestions.",
                italic_bold.prefix(),
                italic_bold.infix(self.0.style())
            ), FIELD_INDENT, FIELD_NEW_LINE_INDENT);
        } else if field.name() == RESTATE_INVOCATION_ERROR_STACKTRACE {
            self.0.write_padded(
                &format_args!("{RESTATE_INVOCATION_ERROR_STACKTRACE}:\n{value:?}"),
                FIELD_INDENT,
                FIELD_NEW_LINE_INDENT,
            );
        }
    }
}

impl VisitOutput<fmt::Result> for RestateErrorCodeAndDetailsWriter<'_> {
    fn finish(self) -> fmt::Result {
        self.0.end()
    }
}

// --- Utilities to write pretty print text

pub fn level_color(level: &Level) -> Color {
    match *level {
        Level::TRACE => Color::Purple,
        Level::DEBUG => Color::Blue,
        Level::INFO => Color::Green,
        Level::WARN => Color::Yellow,
        Level::ERROR => Color::Red,
    }
}

fn level_style(level: &Level) -> Style {
    Style::new().fg(level_color(level))
}

const MESSAGE_INDENT: &str = "  ";
const MESSAGE_NEW_LINE_INDENT: &str = "    ";
const FIELD_INDENT: &str = "    ";
const FIELD_NEW_LINE_INDENT: &str = "      ";

#[derive(Debug)]
pub struct WriterWrapper<'a> {
    writer: Writer<'a>,
    style: Style,
    result: fmt::Result,
}

impl<'a> WriterWrapper<'a> {
    pub fn new(writer: Writer<'a>, style: Style) -> Self {
        let mut this = Self {
            writer,
            style,
            result: Ok(()),
        };
        this.result = write!(&mut this.writer, "{}", this.style.prefix());
        this
    }

    fn write_padded(
        &mut self,
        value: &impl fmt::Debug,
        first_line_indent: &'static str,
        newline_indent: &'static str,
    ) {
        self.result = self
            .result
            .and_then(|_| write!(self.writer, "\n{first_line_indent}"))
            .and_then(|_| {
                write!(
                    indented_skipping_first_line(&mut self.writer, newline_indent),
                    "{value:?}"
                )
            })
    }

    fn style(&self) -> Style {
        if self.writer.has_ansi_escapes() {
            self.style
        } else {
            Style::new()
        }
    }

    fn bold(&self) -> Style {
        if self.writer.has_ansi_escapes() {
            self.style.bold()
        } else {
            Style::new()
        }
    }

    fn italic(&self) -> Style {
        if self.writer.has_ansi_escapes() {
            self.style.italic()
        } else {
            Style::new()
        }
    }

    fn italic_bold(&self) -> Style {
        if self.writer.has_ansi_escapes() {
            self.style.italic().bold()
        } else {
            Style::new()
        }
    }

    fn end(mut self) -> fmt::Result {
        self.result?;
        write!(&mut self.writer, "{}", self.style.suffix())?;
        Ok(())
    }
}

/// Inspired by https://docs.rs/indenter/0.3.3/indenter/index.html, MIT license
pub struct Indented<'a, W: ?Sized> {
    inner: &'a mut W,
    needs_indent: bool,
    indentation: &'static str,
}

/// Indents with the given static indentation, but skipping indenting the first line
fn indented_skipping_first_line<'a, W: ?Sized>(
    f: &'a mut W,
    indentation: &'static str,
) -> Indented<'a, W> {
    Indented {
        inner: f,
        needs_indent: false,
        indentation,
    }
}

impl<T> Write for Indented<'_, T>
where
    T: Write + ?Sized,
{
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for (ind, line) in s.split('\n').enumerate() {
            if ind > 0 {
                self.inner.write_char('\n')?;
                self.needs_indent = true;
            }

            if self.needs_indent {
                // Don't render the line unless its actually got text on it
                if line.is_empty() {
                    continue;
                }

                write!(&mut self.inner, "{}", self.indentation)?;
                self.needs_indent = false;
            }

            self.inner.write_fmt(format_args!("{line}"))?;
        }

        Ok(())
    }
}
