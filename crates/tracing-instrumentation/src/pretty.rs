// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use tracing::{
    field::{self, Field},
    span, Event, Level, Subscriber,
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

impl<'a> Display for FmtLevel<'a> {
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

        let mut v = PrettyVisitor::new(writer.by_ref()).with_style(style);
        event.record(&mut v);
        v.finish()?;
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
            if self.display_thread_name {
                if let Some(name) = thread.name() {
                    write!(writer, "{}", name)?;
                    if self.display_thread_id {
                        writer.write_char(' ')?;
                    }
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
                write!(writer, "{}", fields)?;
            }
            writer.write_char('\n')?;
        }

        let mut restate_error_code_visitor = RestateErrorCodeWriter::new(writer.by_ref());
        event.record(&mut restate_error_code_visitor);
        restate_error_code_visitor.finish()
    }
}

impl<'writer, T> FormatFields<'writer> for Pretty<T> {
    fn format_fields<R: RecordFields>(&self, writer: Writer<'writer>, fields: R) -> fmt::Result {
        let mut v = PrettyVisitor::new(writer);
        fields.record(&mut v);
        v.finish()
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &span::Record<'_>,
    ) -> fmt::Result {
        let writer = current.as_writer();
        let mut v = PrettyVisitor::new(writer);
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
    writer: Writer<'a>,
    style: Style,
    result: fmt::Result,
}

#[derive(Debug, Default)]
pub struct PrettyFields;

impl<'a> MakeVisitor<Writer<'a>> for PrettyFields {
    type Visitor = PrettyVisitor<'a>;

    #[inline]
    fn make_visitor(&self, target: Writer<'a>) -> Self::Visitor {
        PrettyVisitor::new(target)
    }
}

// === impl PrettyVisitor ===

impl<'a> PrettyVisitor<'a> {
    /// Returns a new default visitor that formats to the provided `writer`.
    ///
    /// # Arguments
    /// - `writer`: the writer to format to.
    /// - `is_empty`: whether or not any fields have been previously written to
    ///   that writer.
    pub fn new(writer: Writer<'a>) -> Self {
        Self {
            writer,
            style: Style::default(),
            result: Ok(()),
        }
    }

    pub(crate) fn with_style(self, style: Style) -> Self {
        Self { style, ..self }
    }

    fn write_padded(&mut self, value: &impl fmt::Debug) {
        self.result = write!(self.writer, "\n  {:?}", value);
    }

    fn bold(&self) -> Style {
        if self.writer.has_ansi_escapes() {
            self.style.bold()
        } else {
            Style::new()
        }
    }
}

impl<'a> field::Visit for PrettyVisitor<'a> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if self.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.record_debug(field, &format_args!("{}", value))
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
            self.record_debug(field, &format_args!("{}", value))
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.result.is_err() {
            return;
        }
        match field.name() {
            "message" => {
                let bold = self.bold();
                self.write_padded(&format_args!(
                    "{}{:?}{}",
                    bold.prefix(),
                    value,
                    bold.infix(self.style)
                ))
            }
            name if name.starts_with("r#") => {
                self.write_padded(&format_args!("  {}: {:?}", &name[2..], value))
            }
            name => self.write_padded(&format_args!("  {}: {:?}", name, value)),
        };
    }
}

impl<'a> VisitOutput<fmt::Result> for PrettyVisitor<'a> {
    fn finish(mut self) -> fmt::Result {
        write!(&mut self.writer, "{}", self.style.suffix())?;
        self.result
    }
}

impl<'a> VisitFmt for PrettyVisitor<'a> {
    fn writer(&mut self) -> &mut dyn fmt::Write {
        &mut self.writer
    }
}

struct ErrorSourceList<'a>(&'a (dyn std::error::Error + 'static));

impl<'a> Display for ErrorSourceList<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        let mut curr = Some(self.0);
        while let Some(curr_err) = curr {
            list.entry(&format_args!("{}", curr_err));
            curr = curr_err.source();
        }
        list.finish()
    }
}

// --- Visitor to record as alternate the restate.error.code field

#[derive(Debug)]
pub struct RestateErrorCodeWriter<'a> {
    writer: Writer<'a>,
    result: fmt::Result,
}

impl<'a> RestateErrorCodeWriter<'a> {
    fn new(writer: Writer<'a>) -> Self {
        Self {
            writer,
            result: Ok(()),
        }
    }
}

impl<'a> field::Visit for RestateErrorCodeWriter<'a> {
    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "restate.error.code" {
            self.result = writeln!(self.writer, "{:#?}", value)
        }
    }
}

impl<'a> VisitOutput<fmt::Result> for RestateErrorCodeWriter<'a> {
    fn finish(self) -> fmt::Result {
        self.result
    }
}

// --- Other utils

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
