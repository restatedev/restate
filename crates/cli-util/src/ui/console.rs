// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A set of utilities and macros to control terminal output.
//! Use this instead of println!()
//!
//! I hear you say: "Why not use std's `print*!()` macros?". I'm glad you asked.
//! Because in CLI applications we don't want to panic when stdout/stderr is
//! a broken pipe. We want to ignore the error and continue.
//!
//! An example:
//! `restate whoami | head -n1` would panic if whoami uses print*! macros. This
//! means that the user might see an error like this:
//!
//!  thread 'main' panicked at 'failed printing to stdout: Broken pipe (os error 32)', library/std/src/io/stdio.rs:1019:9
//!  note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

use std::fmt::{Display, Formatter};

use super::stylesheet::Style;
use crate::context::CliContext;

use dialoguer::console::Style as DStyle;

/// Emoji that fallback to a string if colors are disabled.
#[derive(Copy, Clone)]
pub struct Icon<'a, 'b>(pub &'a str, pub &'b str);

/// Text with a style that drops the style if colors are disabled.
#[derive(Copy, Clone)]
pub struct Styled<T: ?Sized>(pub Style, pub T);

impl<T> std::fmt::Debug for Styled<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // passthrough debug formatting to the actual wrapped value
        if CliContext::get().colors_enabled() {
            // unpack the style and the string.
            let dstyle = DStyle::from(self.0);
            write!(f, "{:?}", dstyle.apply_to(&self.1))
        } else {
            write!(f, "{:?}", self.1)
        }
    }
}

impl Display for Icon<'_, '_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if CliContext::get().colors_enabled() {
            write!(f, "{}", self.0)
        } else {
            write!(f, "{}", self.1)
        }
    }
}

impl<T> Display for Styled<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if CliContext::get().colors_enabled() {
            // unpack the style and the string.
            let dstyle = DStyle::from(self.0);
            write!(f, "{}", dstyle.apply_to(&self.1))
        } else {
            write!(f, "{}", self.1)
        }
    }
}

/// Factory trait to create styled tables that respect the UI config.
/// Impl is in stylesheets.
pub trait StyledTable {
    fn new_styled() -> Self;
    fn set_styled_header<T: ToString>(&mut self, headers: Vec<T>) -> &mut Self;
    fn add_kv_row<V: Into<comfy_table::Cell>>(&mut self, key: &str, value: V) -> &mut Self;
    fn add_kv_row_if<P: Fn() -> bool, V: Fn() -> D, D: Display>(
        &mut self,
        predicate: P,
        key: &str,
        value: V,
    ) -> &mut Self {
        if predicate() {
            self.add_kv_row(key, value())
        } else {
            self
        }
    }
}

pub fn confirm_or_exit(prompt: &str) -> anyhow::Result<()> {
    if !confirm(prompt) {
        return Err(anyhow::anyhow!("User aborted"));
    }
    Ok(())
}

pub fn choose<T: ToString + std::fmt::Display>(
    prompt: &str,
    choices: &[T],
) -> anyhow::Result<usize> {
    let theme = dialoguer::theme::ColorfulTheme::default();
    Ok(dialoguer::Select::with_theme(&theme)
        .with_prompt(prompt)
        .items(choices)
        .interact()?)
}

#[allow(dead_code)]
pub fn input(prompt: &str, default: String) -> anyhow::Result<String> {
    let theme = dialoguer::theme::ColorfulTheme::default();
    Ok(dialoguer::Input::with_theme(&theme)
        .with_prompt(prompt)
        .default(default)
        .interact_text()?)
}

pub fn confirm(prompt: &str) -> bool {
    let theme = dialoguer::theme::ColorfulTheme::default();
    if CliContext::get().auto_confirm() {
        c_println!(
            "{} {}",
            prompt,
            Styled(Style::Warn, "Auto-confirming --yes is set."),
        );
        true
    } else {
        dialoguer::Confirm::with_theme(&theme)
            .with_prompt(prompt)
            .default(false)
            .wait_for_newline(true)
            .interact_opt()
            .unwrap_or(Some(false))
            .unwrap_or(false)
    }
}

#[macro_export]
/// Internal macro used by c_*print*! macros
macro_rules! _gecho {
    // Ignore errors (don't panic on broken pipes, unlike default behaviour)
    (@empty_line, $where:tt) => {
        {
            use std::fmt::Write;
            let mut _lock = $crate::ui::output::$where();
            let _ = writeln!(_lock);
        }
    };
    (@newline, $where:tt, $($arg:tt)*) => {
        {
            use std::fmt::Write;
            let mut _lock = $crate::ui::output::$where();
            let _ = writeln!(_lock, $($arg)*);
        }
    };
    (@title, ($icon:expr), $where:tt, $($arg:tt)*) => {
        {
            use std::fmt::Write;
            use $crate::_unicode_width::UnicodeWidthStr;

            let mut _lock = $crate::ui::output::$where();
            let _icon = $crate::ui::console::Icon($icon, "");
            let _ = writeln!(_lock);
            let _message = format!("{_icon} {}:", $($arg)*);
            let _ = writeln!(_lock, "{_message}");
            let _ = writeln!(_lock, "{:―<1$}", "", _message.width_cjk());
        }
    };
    (@indented, ($indent:expr), $where:tt, $($arg:tt)*) => {
        {
            use std::fmt::Write;
            let mut _lock = $crate::ui::output::$where();
            let _padding = $indent * 2;
            let _ = write!(_lock, "{:>_padding$}", "");
            let _ = write!(_lock, $($arg)*);
        }
    };
    (@indented_newline, ($indent:expr), $where:tt, $($arg:tt)*) => {
        {
            use std::fmt::Write;

            let mut _lock = $crate::ui::output::$where();
            let _padding = $indent * 2;
            let _ = write!(_lock, "{:>_padding$}", "");
            let _ = writeln!(_lock, $($arg)*);
        }
    };
    (@nl_with_prefix, ($prefix:expr), $where:tt, $($arg:tt)*) => {
        {
            use std::fmt::Write;
            let mut _lock = $crate::ui::output::$where();
            let _ = write!(_lock, "{} ", $prefix);
            let _ = writeln!(_lock, $($arg)*);
        }
    };
    (@nl_with_prefix_styled, ($prefix:expr), ($style:expr), $where:tt, $($arg:tt)*) => {
        {
            use std::fmt::Write;
            let mut _lock = $crate::ui::output::$where();
            let _ = write!(_lock, " ❯ {}  ", $prefix);
            let formatted = format!($($arg)*);
            let _ = writeln!(_lock, "{}", $crate::ui::console::Styled($style ,formatted));
        }
    };
    (@bare, $where:tt, $($arg:tt)*) => {
        {
            use std::fmt::Write;
            let mut _lock = $crate::ui::output::$where();
            let _ = write!(_lock, $($arg)*);
        }
    };
}

#[macro_export]
macro_rules! c_println {
    () => {
        $crate::ui::console::_gecho!(@empty_line, stdout);
    };
    ($($arg:tt)*) => {
        $crate::ui::console::_gecho!(@newline, stdout, $($arg)*);
    };
}

#[macro_export]
macro_rules! c_print {
    ($($arg:tt)*) => {
        $crate::ui::console::_gecho!(@bare, stdout, $($arg)*);
    };
}

#[macro_export]
macro_rules! c_eprintln {
    () => {
        $crate::ui::console::_gecho!(@empty_line, stderr);
    };
    ($($arg:tt)*) => {
        $crate::ui::console::_gecho!(@newline, stderr, $($arg)*);
    };
}

#[macro_export]
macro_rules! c_eprint {
    ($($arg:tt)*) => {
        $crate::ui::console::_gecho!(@bare, stderr, $($arg)*);
    };
}

// Helpers with emojis/icons upfront
#[macro_export]
macro_rules! c_success {
    ($($arg:tt)*) => {
        $crate::ui::console::_gecho!(@nl_with_prefix, ($crate::ui::stylesheet::SUCCESS_ICON), stdout, $($arg)*);
    };
}

#[macro_export]
macro_rules! c_error {
    ($($arg:tt)*) => {
        $crate::ui::console::_gecho!(@nl_with_prefix, ($crate::ui::stylesheet::ERR_ICON), stderr, $($arg)*);
    };
}

/// Warning Sign
#[macro_export]
macro_rules! c_warn {
    ($($arg:tt)*) => {
        {
            let mut table = $crate::_comfy_table::Table::new();
            table.load_preset($crate::_comfy_table::presets::UTF8_BORDERS_ONLY);
            table.set_content_arrangement($crate::_comfy_table::ContentArrangement::Dynamic);
            table.set_width(120);
            let formatted = format!($($arg)*);

            table.add_row(vec![
                $crate::_comfy_table::Cell::new(format!(" {} ",
        $crate::ui::stylesheet::WARN_ICON)).set_alignment($crate::_comfy_table::CellAlignment::Center),
                $crate::_comfy_table::Cell::new(formatted).add_attribute($crate::_comfy_table::Attribute::Bold).fg($crate::_comfy_table::Color::Yellow),
            ]);
            $crate::ui::console::c_eprintln!("{}", table);
        }
    };
}

#[macro_export]
macro_rules! c_tip {
    ($($arg:tt)*) => {
        {
            let mut table = $crate::_comfy_table::Table::new();
            table.load_preset($crate::_comfy_table::presets::NOTHING);
            table.set_content_arrangement($crate::_comfy_table::ContentArrangement::Dynamic);
            table.set_width(120);
            let formatted = format!($($arg)*);

            table.add_row(vec![
                $crate::_comfy_table::Cell::new(format!(" {} ",
        $crate::ui::stylesheet::TIP_ICON)).set_alignment($crate::_comfy_table::CellAlignment::Center),
                $crate::_comfy_table::Cell::new(formatted).add_attribute($crate::_comfy_table::Attribute::Italic).add_attribute($crate::_comfy_table::Attribute::Dim),
            ]);
            $crate::ui::console::c_eprintln!("{}", table);
        }
    };
}

/// Title
#[macro_export]
macro_rules! c_title {
    ($icon:expr, $($arg:tt)*) => {
        $crate::ui::console::_gecho!(@title, ($icon), stdout, $($arg)*);
    };
}

/// Padded printing
#[macro_export]
macro_rules! c_indent {
    ($indent:expr, $($arg:tt)*) => {
        $crate::ui::console::_gecho!(@indented, ($indent), stdout, $($arg)*);
    };
}

#[macro_export]
macro_rules! c_indentln {
    ($indent:expr, $($arg:tt)*) => {
        $crate::ui::console::_gecho!(@indented_newline, ($indent), stdout, $($arg)*);
    };
}

#[macro_export]
macro_rules! c_indent_table {
    ($indent:expr, $table:expr) => {{
        use std::fmt::Write;

        let mut _lock = $crate::ui::output::stdout();
        let _padding = $indent * 2;
        for _l in $table.lines() {
            let _ = write!(_lock, "{:>_padding$}", "");
            let _ = writeln!(_lock, "{}", _l);
        }
    }};
}

// Macros with a "c_" prefix to emits console output with no panics.
#[allow(unused_imports)] // not all macros are used yet
pub use {_gecho, c_eprint, c_eprintln, c_print, c_println};
// Convenience macros with emojis/icons upfront
#[allow(unused_imports)] // not all macros are used yet
pub use {c_error, c_success, c_tip, c_title, c_warn};
// padded printing
#[allow(unused_imports)] // not all macros are used yet
pub use {c_indent, c_indent_table, c_indentln};
