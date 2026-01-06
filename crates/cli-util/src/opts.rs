// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::time::Duration;

use clap::{Args, ValueEnum};
use clap_verbosity_flag::{LogLevel, VerbosityFilter};
use cling::Collect;

use restate_types::config::DEFAULT_MESSAGE_SIZE_LIMIT;
use restate_types::net::connect_opts::{
    CommonClientConnectionOptions, GrpcConnectionOptions, MESSAGE_SIZE_OVERHEAD,
};

const DEFAULT_CONNECT_TIMEOUT: u64 = 3_000;
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
    fn default_filter() -> VerbosityFilter {
        VerbosityFilter::Error
    }

    fn verbose_long_help() -> Option<&'static str> {
        None
    }

    fn quiet_help() -> Option<&'static str> {
        None
    }

    fn quiet_long_help() -> Option<&'static str> {
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
    /// Auto answer "yes" to confirmation prompts.
    /// Default to `false`, unless running on ci (when env variable 'CI' is set).
    #[arg(name = "yes", long, short, global = true)]
    pub yes: bool,
}

#[derive(Args, Clone)]
pub struct NetworkOpts {
    /// Connection timeout for network calls, in milliseconds.
    #[arg(long, default_value_t = DEFAULT_CONNECT_TIMEOUT, global = true)]
    pub connect_timeout: u64,
    /// Overall request timeout for network calls, in milliseconds.
    #[arg(long, default_value_t = DEFAULT_REQUEST_TIMEOUT, global = true)]
    pub request_timeout: u64,
    /// If true, the server's certificate will not be checked for validity. This will make your HTTPS connections
    /// insecure
    #[arg[long = "insecure-skip-tls-verify", default_value_t = false, global = true]]
    pub insecure_skip_tls_verify: bool,
    /// Sets the maximum size of a network messages.
    #[arg[long, default_value_t = DEFAULT_MESSAGE_SIZE_LIMIT, global = true, hide = true]]
    pub message_size_limit: NonZeroUsize,
}

impl Default for NetworkOpts {
    fn default() -> Self {
        Self {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            insecure_skip_tls_verify: false,
            message_size_limit: DEFAULT_MESSAGE_SIZE_LIMIT,
        }
    }
}

impl GrpcConnectionOptions for NetworkOpts {
    fn message_size_limit(&self) -> NonZeroUsize {
        self.message_size_limit
            .saturating_add(MESSAGE_SIZE_OVERHEAD)
    }
}

impl CommonClientConnectionOptions for NetworkOpts {
    fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout)
    }

    fn request_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(self.request_timeout))
    }

    fn keep_alive_interval(&self) -> Duration {
        Duration::from_secs(60)
    }

    fn keep_alive_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout)
    }

    fn http2_adaptive_window(&self) -> bool {
        true
    }
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
