// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Terminal output utilities and macros.
//!
//! This module provides the core output primitives for CLI applications. Use these
//! instead of `println!()` and friends.
//!
//! # Why Not `println!()`?
//!
//! In CLI applications, we don't want to panic when stdout/stderr is a broken pipe.
//! For example, `restate whoami | head -n1` would panic if `whoami` uses `println!`:
//!
//! ```text
//! thread 'main' panicked at 'failed printing to stdout: Broken pipe (os error 32)'
//! ```
//!
//! The macros in this module silently ignore write errors, enabling safe piping.
//!
//! # Output Macros
//!
//! | Macro | Channel | Purpose |
//! |-------|---------|---------|
//! | [`c_println!`] | stdout | Print line (primary output) |
//! | [`c_print!`] | stdout | Print without newline |
//! | [`c_eprintln!`] | stderr | Print line to stderr |
//! | [`c_eprint!`] | stderr | Print to stderr without newline |
//! | [`c_success!`] | stdout | Success message with checkmark icon |
//! | [`c_error!`] | stderr | Error message with X icon |
//! | [`c_warn!`] | stderr | Warning box (yellow, bold, bordered) |
//! | [`c_tip!`] | stderr | Tip box (italic, dim) |
//! | [`c_title!`] | stdout | Section title with icon and underline |
//! | [`c_indentln!`] | stdout | Indented line output |
//! | [`c_indent_table!`] | stdout | Print table with indentation |
//!
//! # Example
//!
//! ```ignore
//! use restate_cli_util::{c_println, c_success, c_error, c_warn, c_tip, c_title};
//! use restate_cli_util::ui::console::Styled;
//! use restate_cli_util::ui::stylesheet::Style;
//!
//! // Basic output
//! c_println!("Hello, {}!", name);
//!
//! // Success/error with icons
//! c_success!("Deployment registered!");
//! c_error!("Failed to connect: {}", error);
//!
//! // Prominent warning box
//! c_warn!("This action cannot be undone!");
//!
//! // Subtle tip
//! c_tip!("Use --force to skip confirmation");
//!
//! // Section title with icon and underline
//! c_title!("📜", "Service Information");
//!
//! // Styled text
//! c_println!("Status: {}", Styled(Style::Success, "running"));
//! ```

use std::fmt::{Display, Formatter};

use super::stylesheet::Style;
use crate::context::CliContext;

use dialoguer::console::Style as DStyle;

/// An emoji icon with a text fallback for non-color terminals.
///
/// When colors are enabled, displays the emoji (first field). When colors are
/// disabled, displays the fallback text (second field). This ensures output
/// remains meaningful in all terminal environments.
///
/// # Example
///
/// ```ignore
/// use restate_cli_util::ui::console::Icon;
///
/// // Define a custom icon
/// let rocket = Icon("🚀", "[LAUNCH]");
///
/// // In color mode: prints "🚀"
/// // In non-color mode: prints "[LAUNCH]"
/// c_println!("{} Starting deployment...", rocket);
/// ```
///
/// # Pre-defined Icons
///
/// See [`stylesheet`](super::stylesheet) for standard icons like `SUCCESS_ICON`,
/// `ERR_ICON`, `WARN_ICON`, etc.
#[derive(Copy, Clone)]
pub struct Icon<'a, 'b>(pub &'a str, pub &'b str);

/// Wrapper that applies a [`Style`] to any displayable value.
///
/// When colors are enabled, the style is applied. When colors are disabled,
/// the value is displayed without styling. This ensures consistent output
/// regardless of terminal capabilities.
///
/// # Example
///
/// ```ignore
/// use restate_cli_util::ui::console::Styled;
/// use restate_cli_util::ui::stylesheet::Style;
///
/// // Apply semantic styles to values
/// let status = Styled(Style::Success, "running");
/// let error = Styled(Style::Danger, "connection failed");
///
/// c_println!("Status: {}", status);  // Green in color mode
/// c_println!("Error: {}", error);    // Red+bold in color mode
/// ```
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

/// Factory trait for creating styled tables that respect CLI configuration.
///
/// This trait provides methods for creating tables that automatically adapt to:
/// - User's `--table-style` preference (compact vs. borders)
/// - Color settings (styling disabled when colors are off)
///
/// The implementation for `comfy_table::Table` is in [`stylesheet`](super::stylesheet).
///
/// # Example
///
/// ```ignore
/// use comfy_table::Table;
/// use restate_cli_util::ui::console::StyledTable;
///
/// // Create a key-value table (detail view)
/// let mut table = Table::new_styled();
/// table.add_kv_row("Name:", "my-service");
/// table.add_kv_row("Status:", "running");
/// table.add_kv_row_if(
///     || deployment.is_some(),
///     "Deployment:",
///     || deployment.as_ref().unwrap().id,
/// );
/// c_println!("{}", table);
///
/// // Create a list table (collection view)
/// let mut table = Table::new_styled();
/// table.set_styled_header(vec!["NAME", "TYPE", "STATUS"]);
/// for service in services {
///     table.add_row(vec![&service.name, &service.ty, &service.status]);
/// }
/// c_println!("{}", table);
/// ```
pub trait StyledTable {
    /// Create a new table with styling based on CLI configuration.
    fn new_styled() -> Self;

    /// Set bold column headers for list-style tables.
    fn set_styled_header<T: ToString>(&mut self, headers: Vec<T>) -> &mut Self;

    /// Add a key-value row with bold key (for detail views).
    fn add_kv_row<V: Into<comfy_table::Cell>>(&mut self, key: &str, value: V) -> &mut Self;

    /// Conditionally add a key-value row.
    ///
    /// Only adds the row if the predicate returns true. The value closure
    /// is only called when the predicate passes, enabling lazy evaluation.
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

/// Prompt for confirmation, returning an error if the user declines.
///
/// Use this for destructive operations where declining should abort the command.
///
/// # Auto-confirmation
///
/// Automatically confirms (returns `Ok(())`) when:
/// - `--yes` / `-y` flag is set
/// - `CI` environment variable is set
///
/// # Example
///
/// ```ignore
/// confirm_or_exit("This will delete all data. Continue?")?;
/// // Only reaches here if user confirmed
/// delete_all_data();
/// ```
pub fn confirm_or_exit(prompt: &str) -> anyhow::Result<()> {
    if !confirm(prompt) {
        return Err(anyhow::anyhow!("User aborted"));
    }
    Ok(())
}

/// Present an interactive selection menu and return the chosen index.
///
/// # Example
///
/// ```ignore
/// let options = ["Development", "Staging", "Production"];
/// let selected = choose("Select environment:", &options)?;
/// println!("You selected: {}", options[selected]);
/// ```
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

/// Prompt for text input with a default value.
///
/// # Example
///
/// ```ignore
/// let name = input("Service name:", "my-service".to_string())?;
/// ```
#[allow(dead_code)]
pub fn input(prompt: &str, default: String) -> anyhow::Result<String> {
    let theme = dialoguer::theme::ColorfulTheme::default();
    Ok(dialoguer::Input::with_theme(&theme)
        .with_prompt(prompt)
        .default(default)
        .interact_text()?)
}

/// Prompt for yes/no confirmation, returning the user's choice.
///
/// # Auto-confirmation
///
/// Automatically returns `true` when:
/// - `--yes` / `-y` flag is set
/// - `CI` environment variable is set
///
/// # Example
///
/// ```ignore
/// if confirm("Deploy to production?") {
///     deploy();
/// } else {
///     println!("Deployment cancelled");
/// }
/// ```
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
