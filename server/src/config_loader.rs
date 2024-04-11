// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::Duration;

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use notify_debouncer_mini::{
    new_debouncer, DebounceEventResult, DebouncedEvent, DebouncedEventKind,
};
use restate_types::config::{CommonOptionCliOverride, Configuration};
use tracing::{error, info, warn};

#[derive(thiserror::Error, codederror::CodedError, Debug)]
#[code(restate_errors::RT0002)]
pub enum ConfigLoadError {
    #[error("configuration loading error: {0}")]
    Figment(#[from] figment::Error),
}

#[derive(Debug, Default, derive_builder::Builder)]
#[builder(default)]
pub struct ConfigLoader {
    path: Option<PathBuf>,
    load_env: bool,
    #[builder(setter(strip_option))]
    custom_default: Option<Configuration>,
    #[builder(setter(strip_option))]
    cli_override: Option<CommonOptionCliOverride>,
    disable_watch: bool,
}

impl ConfigLoader {
    pub fn load_once(&self) -> Result<Configuration, ConfigLoadError> {
        let defaults = self.custom_default.clone().unwrap_or_default();

        let mut figment = Figment::from(Serialized::defaults(defaults));
        // Load configuration file
        if let Some(path) = &self.path {
            figment = figment.merge(Toml::file_exact(path.as_path()));
        }

        // Merge with environment variables
        if self.load_env {
            figment = Self::merge_with_env(figment);
        }

        // Merge with CLI overrides
        if let Some(cli_overrides) = self.cli_override.clone() {
            figment = figment.merge(Figment::from(Serialized::defaults(cli_overrides)))
        }

        let config: Configuration = figment.extract()?;
        Ok(config.apply_rocksdb_common())
    }

    fn merge_with_env(figment: Figment) -> Figment {
        figment
            .merge(
                Env::prefixed("RESTATE_")
                    .split("__")
                    .map(|k| k.as_str().replace('_', "-").into()),
            )
            // Override tracing.log with RUST_LOG, if present
            .merge(Env::raw().only(&["RUST_LOG"]).map(|_| "log_filter".into()))
            .merge(
                Env::raw()
                    .only(&["HTTP_PROXY"])
                    .map(|_| "http_proxy".into()),
            )
            .merge(
                Env::raw()
                    .only(&["AWS_EXTERNAL_ID"])
                    .map(|_| "aws_assume_role_external_id".into()),
            )
            .merge(
                Env::raw()
                    .only(&["MEMORY_LIMIT"])
                    .map(|_| "rocksdb_total_memory_limit".into()),
            )
    }

    pub fn start(self) {
        if self.disable_watch || self.path.is_none() {
            return;
        }

        let path = self.path.clone().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        // Automatically select the best implementation for watching files on
        // the current platform.
        let Ok(mut debouncer) = new_debouncer(
            Duration::from_secs(3),
            move |res: DebounceEventResult| match res {
                Ok(events) => tx.send(events).unwrap(),
                Err(e) => warn!("Error {:?}", e),
            },
        ) else {
            warn!(
                "Couldn't initialize configuration watcher, config changes will not be monitored",
            );
            return;
        };

        info!("Installing watcher for config changes: {}", path.display());
        if let Err(e) = debouncer
            .watcher()
            .watch(&path, notify::RecursiveMode::NonRecursive)
        {
            warn!("Couldn't install configuration watcher: {}", e);
            return;
        };

        std::thread::Builder::new()
            .name("config-watcher".to_owned())
            .spawn(move || {
                // It's important that we capture the watcher in the thread,
                // otherwise it'll be dropped and we won't be watching anything!
                let _debouncer = debouncer;
                info!("Configuration watcher thread has started");
                let mut should_run = true;
                while should_run {
                    match rx.recv() {
                        Ok(evs) => {
                            self.handle_events(evs);
                        }
                        Err(e) => {
                            error!("Cannot continue watching configuration changes: '{}!", e);
                            should_run = false;
                        }
                    }
                }
                info!("Config watcher thread has terminated");
            })
            .expect("start config watcher thread");
    }

    fn handle_events(&self, events: Vec<DebouncedEvent>) {
        let mut should_update = false;
        for event in events.iter().filter(|e| e.kind == DebouncedEventKind::Any) {
            should_update = true;
            info!("Detected configuration file changes: {:?}", event.path);
        }

        if should_update {
            match self.load_once() {
                Ok(config) => {
                    restate_types::config::set_current_config(config);
                }
                Err(e) => {
                    warn!(
                        "Error updating configuration, config was not updated: {}",
                        e
                    );
                }
            }
        }
    }
}
