// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    pub fn new(opts: CommonOpts) -> Self {
        let os_env = OsEnv::default();
        Self::with_env(&os_env, opts)
    }

    /// Loading CliEnv with a custom OsEnv. OsEnv can be customised in cfg(test)
    pub fn with_env(os_env: &OsEnv, opts: CommonOpts) -> Self {
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

        // Setup logging from env and from -v .. -vvvv
        if let Err(err) = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_max_level(opts.verbose.log_level_filter().as_trace())
            .with_ansi(colorful)
            .try_init()
        {
            warn!("Failed to initialize tracing subscriber: {}", err);
        }

        // We only log after we've initialized the logger with the desired log
        // level.
        let loaded_dotenv = maybe_dotenv.ok();
        match &loaded_dotenv {
            Some(path) => {
                info!("Loaded .env file from: {}", path.display())
            }
            None => info!("Didn't load '.env' file"),
        };

        Self {
            confirm_mode: opts.confirm,
            ui: opts.ui,
            network: opts.network,
            colors_enabled: colorful,
            loaded_dotenv,
        }
    }

    pub fn get() -> arc_swap::Guard<Arc<CliContext>> {
        GLOBAL_CLI_CONTEXT.get_or_init(Default::default).load()
    }

    /// Sets the global context to the given value. In general, this should be called once on CLI
    /// initialization.
    pub fn set_as_global(self) {
        GLOBAL_CLI_CONTEXT
            .get_or_init(Default::default)
            .store(Arc::new(self));
    }

    pub fn auto_confirm(&self) -> bool {
        self.confirm_mode.yes || env::var("CI").is_ok()
    }

    pub fn table_style(&self) -> TableStyle {
        self.ui.table_style
    }

    pub fn time_format(&self) -> TimeFormat {
        self.ui.time_format
    }

    pub fn colors_enabled(&self) -> bool {
        self.colors_enabled
    }

    pub fn connect_timeout(&self) -> std::time::Duration {
        Duration::from_millis(self.network.connect_timeout)
    }

    pub fn request_timeout(&self) -> std::time::Duration {
        Duration::from_millis(self.network.request_timeout)
    }

    pub fn insecure_skip_tls_verify(&self) -> bool {
        self.network.insecure_skip_tls_verify
    }

    pub fn loaded_dotenv(&self) -> Option<&Path> {
        self.loaded_dotenv.as_deref()
    }
}
