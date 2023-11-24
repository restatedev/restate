// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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
use std::sync::atomic::{AtomicBool, Ordering};

use crate::app::UiConfig;

use super::stylesheet::Style;
use dialoguer::console::Style as DStyle;
use once_cell::sync::Lazy;

static SHOULD_COLORIZE: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));

#[inline]
pub fn colors_enabled() -> bool {
    SHOULD_COLORIZE.load(Ordering::Relaxed)
}

#[inline]
pub fn set_colors_enabled(val: bool) {
    // Override dialoguer/console to ensure it follows our colorful setting
    dialoguer::console::set_colors_enabled(val);
    SHOULD_COLORIZE.store(val, Ordering::Relaxed)
}

/// Emoji that fallback to a string if colors are disabled.
#[derive(Copy, Clone)]
pub struct Icon<'a, 'b>(pub &'a str, pub &'b str);

/// Text with a style that drops the style if colors are disabled.
#[derive(Copy, Clone)]
pub struct Styled<'a, T: ?Sized>(pub Style, pub &'a T);

impl Display for Icon<'_, '_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if colors_enabled() {
            write!(f, "{}", self.0)
        } else {
            write!(f, "{}", self.1)
        }
    }
}

impl<T> Display for Styled<'_, T>
where
    T: Display + ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if colors_enabled() {
            // unpack the style and the string.
            let dstyle: DStyle = self.0.into();
            write!(f, "{}", dstyle.apply_to(self.1))
        } else {
            write!(f, "{}", self.1)
        }
    }
}

/// Factory trait to create styled tables that respect the UI config.
/// Impl is in stylesheets.
pub trait StyledTable {
    fn new_styled(ui_config: &UiConfig) -> Self;
}

#[macro_export]
/// Internal macro used by c_*print*! macros
macro_rules! _gecho {
    // Ignore errors (don't panic on broken pipes, unlike default behaviour)
    (@empty_line, $where:tt) => {
        {
            use std::io::Write;
            let _ = writeln!(std::io::$where());
        }
    };
    (@newline, $where:tt, $($arg:tt)*) => {
        {
            use std::io::Write;
            let _ = writeln!(std::io::$where(), $($arg)*);
        }
    };
    (@nl_with_prefix, ($prefix:expr), $where:tt, $($arg:tt)*) => {
        {
            use std::io::Write;
            let mut lock = std::io::$where().lock();
            let _ = write!(lock, "{} ", $prefix);
            let _ = writeln!(lock, $($arg)*);
        }
    };
    (@bare, $where:tt, $($arg:tt)*) => {
        {
            use std::io::Write;
            let _ = write!(std::io::$where(), $($arg)*);
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

// Macros with a "c_" prefix to emits console output with no panics.
pub use {_gecho, c_eprint, c_eprintln, c_print, c_println};
// Convenience macros with emojis/icons upfront
pub use {c_error, c_success};
