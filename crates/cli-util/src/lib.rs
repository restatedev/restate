// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Restate CLI Utilities
//!
//! A framework for building consistent, user-friendly command-line interfaces.
//! This crate provides styling primitives, output macros, and utilities that
//! ensure a cohesive look and feel across all Restate CLI tools.
//!
//! # Quick Start
//!
//! ```ignore
//! use restate_cli_util::{
//!     CliContext, CommonOpts,
//!     c_println, c_success, c_error, c_warn, c_tip,
//! };
//! use restate_cli_util::ui::console::{Styled, StyledTable};
//! use restate_cli_util::ui::stylesheet::Style;
//! use comfy_table::Table;
//!
//! // Initialize context (once at startup)
//! let opts = CommonOpts::default();
//! CliContext::new(opts).set_as_global();
//!
//! // Styled output
//! c_success!("Operation completed!");
//! c_error!("Something went wrong: {}", error);
//! c_warn!("This is destructive!");
//! c_tip!("Use --force to skip confirmation");
//!
//! // Styled tables
//! let mut table = Table::new_styled();
//! table.add_kv_row("Status:", Styled(Style::Success, "running"));
//! c_println!("{}", table);
//! ```
//!
//! # Modules
//!
//! - [`ui`] - Output macros, styling, and display utilities
//! - [`completions`] - Shell completion generation and installation
//! - [`lambda`] - AWS Lambda runtime support
//!
//! # Key Types
//!
//! - [`CliContext`] - Global configuration (colors, table style, time format)
//! - [`CommonOpts`] - Standard CLI options (verbosity, confirmations, network)
//! - [`ui::console::Icon`] - Emoji with text fallback
//! - [`ui::console::Styled`] - Styled text wrapper
//! - [`ui::stylesheet::Style`] - Semantic style enum
//!
//! See the crate's README.md for the complete CLI style guide.

pub mod completions;
mod context;
pub mod lambda;
mod opts;
mod os_env;
pub mod ui;

pub use context::CliContext;
pub use opts::CommonOpts;
pub use os_env::OsEnv;

// Re-export comfy-table for console c_* macros (used internally by macros)
#[doc(hidden)]
pub use comfy_table as _comfy_table;
#[doc(hidden)]
pub use unicode_width as _unicode_width;
