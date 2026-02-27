// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Global CLI configuration context.
//!
//! The [`CliContext`] manages global settings that affect CLI behavior:
//! - Color detection and enforcement
//! - Table styling preferences
//! - Time format preferences
//! - Auto-confirmation mode
//! - Network timeouts
//!
//! # Initialization
//!
//! Initialize the context once at CLI startup:
//!
//! ```ignore
//! use restate_cli_util::{CliContext, CommonOpts};
//!
//! fn main() {
//!     let opts = CommonOpts::parse();
//!     CliContext::new(opts).set_as_global();
//!     
//!     // Now all cli-util functions will use these settings
//! }
//! ```
//!
//! # Color Detection
//!
//! Colors are automatically detected based on:
//! 1. `NO_COLOR` environment variable (any value except "0" disables colors)
//! 2. `TERM` environment variable (colors disabled if "dumb")
//! 3. TTY detection (colors disabled if stdout is not a terminal)
//! 4. `CLICOLOR_FORCE` environment variable (overrides all above if set)

use std::env;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use dotenvy::dotenv;
use tracing::{info, warn};
use tracing_log::AsTrace;

use crate::opts::{CommonOpts, ConfirmMode, NetworkOpts, TableStyle, TimeFormat, UiOpts};
use crate::os_env::OsEnv;

static GLOBAL_CLI_CONTEXT: OnceLock<ArcSwap<CliContext>> = OnceLock::new();

/// Global configuration for CLI behavior.
///
/// This struct holds all the settings that affect how the CLI operates:
/// output styling, time formatting, confirmations, and network settings.
///
/// Access the global instance via [`CliContext::get()`]. Initialize it
/// once at startup with [`CliContext::new()`] followed by [`set_as_global()`].
pub struct CliContext {
    confirm_mode: ConfirmMode,
    ui: UiOpts,
    pub network: NetworkOpts,
    colors_enabled: bool,
    loaded_dotenv: Option<PathBuf>,
}

impl Default for CliContext {
    fn default() -> Self {
        Self {
            confirm_mode: Default::default(),
            ui: Default::default(),
            network: Default::default(),
            colors_enabled: true,
            loaded_dotenv: None,
        }
    }
}

impl CliContext {
    /// Create a new CLI context from command-line options.
    ///
    /// This initializes color detection, logging, and loads `.env` files.
    /// Call [`set_as_global()`] after creation to make it the global context.
    pub fn new(opts: CommonOpts) -> Self {
        let os_env = OsEnv::default();
        Self::with_env(&os_env, opts)
    }

    /// Create a CLI context with a custom environment (for testing).
    ///
    /// In tests, use this to inject mock environment variables.
    pub fn with_env(os_env: &OsEnv, opts: CommonOpts) -> Self {
        let ctx = Self::build(os_env, &opts);

        // Setup logging from env and from -v .. -vvvv
        if let Err(err) = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_max_level(opts.verbose.log_level_filter().as_trace())
            .with_ansi(ctx.colors_enabled)
            .try_init()
        {
            warn!("Failed to initialize tracing subscriber: {}", err);
        }

        // We only log after we've initialized the logger with the desired log
        // level.
        match &ctx.loaded_dotenv {
            Some(path) => {
                info!("Loaded .env file from: {}", path.display())
            }
            None => info!("Didn't load '.env' file"),
        };

        ctx
    }

    /// Like [`new`](Self::new), but skips installing a tracing subscriber.
    ///
    /// Use this when the application manages its own tracing setup (e.g. via
    /// `restate_tracing_instrumentation`) but still wants CLI color detection,
    /// table styling, and other UI features.
    pub fn new_without_tracing(opts: CommonOpts) -> Self {
        let os_env = OsEnv::default();
        Self::build(&os_env, &opts)
    }

    /// Core builder: color detection, dotenv, and UI setup without tracing.
    fn build(os_env: &OsEnv, opts: &CommonOpts) -> Self {
        // Load .env file. Best effort.
        let maybe_dotenv = dotenv();

        // color setup
        // NO_COLOR=1 with any value other than "0" means user doesn't want colors.
        // e.g.
        //  NO_COLOR=1 (no colors)
        //  NO_COLOR=true (no colors)
        //  NO_COLOR=something (no colors)
        //  NO_COLOR=0 or unset (yes *color* if term supports it)
        let should_color = os_env
            .get("NO_COLOR")
            .map(|x| x == "0")
            .unwrap_or_else(|| true);

        // dumb terminal? no colors or fancy stuff
        let smart_term = os_env
            .get("TERM")
            .map(|x| x != "dumb")
            .unwrap_or_else(|| true);

        // CLICOLOR_FORCE is set? enforce coloring..
        // Se http://bixense.com/clicolors/ for details.
        let force_colorful = os_env
            .get("CLICOLOR_FORCE")
            .map(|x| x != "0")
            .unwrap_or_else(|| false);

        let colorful = if force_colorful {
            // CLICOLOR_FORCE is set, we enforce coloring
            true
        } else {
            // We colorize only if it's a smart terminal (not TERM=dumb, nor pipe)
            // and NO_COLOR is anything but "0"
            let is_terminal = std::io::stdout().is_terminal();
            is_terminal && smart_term && should_color
        };

        // Ensure we follows our colorful setting in our console utilities
        // without passing the environment around.
        dialoguer::console::set_colors_enabled(colorful);

        Self {
            confirm_mode: opts.confirm.clone(),
            ui: opts.ui.clone(),
            network: opts.network.clone(),
            colors_enabled: colorful,
            loaded_dotenv: maybe_dotenv.ok(),
        }
    }

    /// Get a reference to the global CLI context.
    ///
    /// Returns a default context if none has been set. In production code,
    /// always call [`set_as_global()`] before using this.
    pub fn get() -> arc_swap::Guard<Arc<CliContext>> {
        GLOBAL_CLI_CONTEXT.get_or_init(Default::default).load()
    }

    /// Set this context as the global instance.
    ///
    /// This should be called once at CLI startup, after parsing options.
    /// Subsequent calls will update the global context.
    pub fn set_as_global(self) {
        GLOBAL_CLI_CONTEXT
            .get_or_init(Default::default)
            .store(Arc::new(self));
    }

    /// Whether confirmations should be automatically accepted.
    ///
    /// Returns `true` if:
    /// - `--yes` / `-y` flag was passed
    /// - `CI` environment variable is set
    pub fn auto_confirm(&self) -> bool {
        self.confirm_mode.yes || env::var("CI").is_ok()
    }

    /// Get the user's preferred table style.
    ///
    /// - `Compact`: No borders, condensed layout (default)
    /// - `Borders`: UTF-8 borders, good for multiline content
    pub fn table_style(&self) -> TableStyle {
        self.ui.table_style
    }

    /// Get the user's preferred time format.
    ///
    /// - `Human`: "5 minutes ago" (default)
    /// - `Iso8601`: "2024-01-15T10:30:00Z"
    /// - `Rfc2822`: "Mon, 15 Jan 2024 10:30:00 +0000"
    pub fn time_format(&self) -> TimeFormat {
        self.ui.time_format
    }

    /// Whether colors and styling should be used in output.
    ///
    /// This is determined by color detection at context creation time.
    pub fn colors_enabled(&self) -> bool {
        self.colors_enabled
    }

    /// Get the connection timeout for network requests.
    pub fn connect_timeout(&self) -> std::time::Duration {
        Duration::from_millis(self.network.connect_timeout)
    }

    /// Get the overall request timeout for network requests.
    pub fn request_timeout(&self) -> std::time::Duration {
        Duration::from_millis(self.network.request_timeout)
    }

    /// Whether TLS certificate verification should be skipped.
    pub fn insecure_skip_tls_verify(&self) -> bool {
        self.network.insecure_skip_tls_verify
    }

    /// Get the path to the loaded `.env` file, if any.
    pub fn loaded_dotenv(&self) -> Option<&Path> {
        self.loaded_dotenv.as_deref()
    }
}
