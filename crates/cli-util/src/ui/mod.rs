// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! UI components for CLI output.
//!
//! This module provides all the building blocks for creating consistent CLI interfaces:
//!
//! - [`console`] - Output macros and types (`c_println!`, `Icon`, `Styled`, etc.)
//! - [`stylesheet`] - Visual constants (icons, styles, colors)
//! - [`output`] - Low-level output abstraction (stdout/stderr/in-memory)
//! - [`watcher`] - Watch mode for continuous refresh (`-w` flag support)
//!
//! # Time Formatting
//!
//! This module also provides time formatting functions that respect the user's
//! `--time-format` preference:
//!
//! ```ignore
//! use restate_cli_util::ui::{timestamp_as_human_duration, duration_to_human_precise};
//! use chrono_humanize::Tense;
//!
//! // Format a timestamp relative to now
//! let formatted = timestamp_as_human_duration(event.created_at, Tense::Past);
//! // --time-format=human: "5 minutes ago"
//! // --time-format=iso8601: "2024-01-15T10:30:00Z"
//!
//! // Format a duration precisely
//! let duration_str = duration_to_human_precise(elapsed, Tense::Present);
//! // "5 seconds and 78 ms"
//! ```

use chrono::{DateTime, TimeDelta};
pub use chrono_humanize::{Accuracy, Tense};

use crate::CliContext;

pub mod console;
pub mod output;
pub mod stylesheet;
pub mod watcher;

/// Format a timestamp as a human-readable duration, respecting `--time-format`.
///
/// The output format depends on the CLI's time format setting:
/// - `human`: "5 minutes ago" or "in 2 hours"
/// - `iso8601`: RFC-3339 format (e.g., "2024-01-15T10:30:00Z")
/// - `rfc2822`: RFC-2822 format (e.g., "Mon, 15 Jan 2024 10:30:00 +0000")
///
/// # Arguments
///
/// * `ts` - The timestamp to format
/// * `tense` - Whether to express as past ("ago") or future ("in")
pub fn timestamp_as_human_duration<Tz: chrono::TimeZone>(ts: DateTime<Tz>, tense: Tense) -> String {
    match CliContext::get().time_format() {
        crate::opts::TimeFormat::Human => {
            let since = ts.signed_duration_since(chrono::Local::now());
            duration_to_human_precise(since, tense)
        }
        crate::opts::TimeFormat::Iso8601 => ts.to_rfc3339(),
        crate::opts::TimeFormat::Rfc2822 => ts.to_rfc2822(),
    }
}

/// Format a duration with precise detail, respecting `--time-format`.
///
/// In human mode, produces detailed output like "5 seconds and 78 ms".
/// In ISO/RFC modes, outputs the raw duration string.
///
/// # Arguments
///
/// * `duration` - The duration to format
/// * `tense` - Affects phrasing in human mode (past vs. future)
///
/// # Example
///
/// ```ignore
/// let elapsed = chrono::Duration::milliseconds(5078);
/// let formatted = duration_to_human_precise(elapsed, Tense::Present);
/// // "5 seconds and 78 ms"
/// ```
pub fn duration_to_human_precise(duration: TimeDelta, tense: Tense) -> String {
    match CliContext::get().time_format() {
        crate::opts::TimeFormat::Iso8601 => duration.to_string(),
        crate::opts::TimeFormat::Rfc2822 => duration.to_string(),
        crate::opts::TimeFormat::Human => {
            let duration = chrono_humanize::HumanTime::from(
                // truncate nanos
                TimeDelta::try_milliseconds(duration.num_milliseconds())
                    .expect("valid milliseconds"),
            );
            duration.to_text_en(Accuracy::Precise, tense)
        }
    }
}

/// Format a duration with rough approximation, respecting `--time-format`.
///
/// In human mode, produces approximate output like "about 5 minutes".
/// In ISO/RFC modes, outputs the raw duration string.
///
/// # Arguments
///
/// * `duration` - The duration to format
/// * `tense` - Affects phrasing in human mode (past: "ago", future: "in")
///
/// # Example
///
/// ```ignore
/// let elapsed = chrono::Duration::minutes(5);
/// let formatted = duration_to_human_rough(elapsed, Tense::Past);
/// // "about 5 minutes ago"
/// ```
pub fn duration_to_human_rough(duration: TimeDelta, tense: Tense) -> String {
    match CliContext::get().time_format() {
        crate::opts::TimeFormat::Iso8601 => duration.to_string(),
        crate::opts::TimeFormat::Rfc2822 => duration.to_string(),
        crate::opts::TimeFormat::Human => {
            let duration = chrono_humanize::HumanTime::from(duration);
            duration.to_text_en(Accuracy::Rough, tense)
        }
    }
}
