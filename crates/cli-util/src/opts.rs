// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::{Args, ValueEnum};
use clap_verbosity_flag::LogLevel;
use cling::Collect;

const DEFAULT_CONNECT_TIMEOUT: u64 = 5_000;
const DEFAULT_REQUEST_TIMEOUT: u64 = 13_000;

#[derive(ValueEnum, Clone, Copy, Default, PartialEq, Eq)]
pub enum TableStyle {
    #[default]
    /// No borders, condensed layout
    Compact,
    /// UTF8 borders, good for multiline text
    Borders,
}

#[derive(ValueEnum, Clone, Copy, Eq, PartialEq, Default, Debug)]
#[clap(rename_all = "kebab-case")]
pub enum TimeFormat {
    /// Human friendly timestamps and durations
    #[default]
    Human,
    /// RFC-3339/ISO-8601 formatted (1996-12-19T16:39:57-08:00)
    Iso8601,
    /// RFC-2822 formatted (Tue, 1 Jul 2003 10:52:37 +0200)
    Rfc2822,
}

/// Silent (no) logging by default in CLI
#[derive(Clone, Default)]
pub(crate) struct Quiet;
impl LogLevel for Quiet {
    fn default() -> Option<tracing_log::log::Level> {
        None
    }
}

#[derive(Args, Clone, Default)]
pub(crate) struct UiOpts {
    /// Which table output style to use
    #[arg(long, default_value = "compact", global = true)]
    pub table_style: TableStyle,

    #[arg(long, default_value = "human", global = true)]
    pub time_format: TimeFormat,
}

#[derive(Args, Clone, Default)]
pub(crate) struct ConfirmMode {
    /// Auto answer "yes" to confirmation prompts
    #[arg(name = "yes", long, short, global = true)]
    pub yes: bool,
}

#[derive(Args, Clone, Default)]
pub(crate) struct NetworkOpts {
    /// Connection timeout for network calls, in milliseconds.
    #[arg(long, default_value_t = DEFAULT_CONNECT_TIMEOUT, global = true)]
    pub connect_timeout: u64,
    /// Overall request timeout for network calls, in milliseconds.
    #[arg(long, default_value_t = DEFAULT_REQUEST_TIMEOUT, global = true)]
    pub request_timeout: u64,
}

#[derive(Args, Collect, Clone, Default)]
pub struct CommonOpts {
    #[clap(flatten)]
    pub(crate) verbose: clap_verbosity_flag::Verbosity<Quiet>,
    #[clap(flatten)]
    pub(crate) ui: UiOpts,
    #[clap(flatten)]
    pub(crate) confirm: ConfirmMode,
    #[clap(flatten)]
    pub(crate) network: NetworkOpts,
}
