// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use notify::{EventKind, RecommendedWatcher, RecursiveMode};
use notify_debouncer_full::{
    new_debouncer, DebounceEventResult, DebouncedEvent, Debouncer, RecommendedCache,
};
use tracing::{error, info, warn};

use crate::config::Configuration;

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
    #[cfg(feature = "clap")]
    #[builder(setter(strip_option))]
    cli_override: Option<crate::config::CommonOptionCliOverride>,
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

        #[cfg(feature = "clap")]
        // Merge with CLI overrides
        if let Some(cli_overrides) = self.cli_override.clone() {
            figment = figment.merge(Figment::from(Serialized::defaults(cli_overrides)))
        }

        let mut config: Configuration = figment.extract()?;

        config.common.set_derived_values();
        Ok(config.apply_cascading_values())
    }

    fn merge_with_env(figment: Figment) -> Figment {
        let fig = figment
            .merge(
                Env::prefixed("RESTATE_")
                    .split("__")
                    .map(|k| k.as_str().replace('_', "-").into()),
            )
            // Override tracing.log with RUST_LOG, if present
            .merge(Env::raw().only(&["RUST_LOG"]).map(|_| "log-filter".into()))
            .merge(
                Env::raw()
                    .only(&["HTTP_PROXY"])
                    .map(|_| "http-proxy".into()),
            )
            .merge(Env::raw().only(&["NO_PROXY"]).map(|_| "no-proxy".into()))
            .merge(
                Env::raw()
                    .only(&["AWS_EXTERNAL_ID"])
                    .map(|_| "aws-assume-role-external-id".into()),
            )
            .merge(
                Env::raw()
                    .only(&["MEMORY_LIMIT"])
                    .map(|_| "rocksdb-total-memory-limit".into()),
            );

        let fig = match Env::var("DO_NOT_TRACK").as_deref() {
            Some("yes" | "1" | "true") => fig.join(("disable-telemetry", true)),
            Some("no" | "0" | "false") => fig.join(("disable-telemetry", false)),
            _ => fig,
        };

        if let Some(no_proxy) = Env::var("NO_PROXY") {
            fig.join((
                "no-proxy",
                no_proxy.split(',').map(str::trim).collect::<Vec<_>>(),
            ))
        } else {
            fig
        }
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
            None,
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
        if let Err(e) = debouncer.watch(&path, notify::RecursiveMode::NonRecursive) {
            warn!("Couldn't install configuration watcher: {}", e);
            return;
        };

        std::thread::Builder::new()
            .name("config-watcher".to_owned())
            .spawn(move || {
                // It's important that we capture the watcher in the thread,
                // otherwise it'll be dropped and we won't be watching anything!
                info!("Configuration watcher thread has started");
                let mut should_run = true;
                while should_run {
                    match rx.recv() {
                        Ok(evs) => {
                            self.handle_events(&mut debouncer, evs);
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

    fn handle_events(
        &self,
        debouncer: &mut Debouncer<RecommendedWatcher, RecommendedCache>,
        events: Vec<DebouncedEvent>,
    ) {
        let mut should_update = false;
        for event in events {
            match event.kind {
                EventKind::Modify(_) => {
                    if let Some(path) = event.paths.first() {
                        warn!("Detected configuration file changes: {:?}", path.display());
                    } else {
                        warn!("Detected configuration file changes");
                    }

                    should_update = true;
                }
                EventKind::Remove(_) => {
                    // some editors (looking at you vim) replaces the entire file
                    // on save. This triggers the `remove`` event, and then the watch
                    // stops (since the inode has changed) so we need to re-watch
                    // the file.
                    should_update = true;
                    for path in &event.event.paths {
                        warn!("Detected configuration file changes: {:?}", path.display());
                        _ = debouncer.unwatch(path);
                        if let Err(err) = debouncer.watch(path, RecursiveMode::NonRecursive) {
                            warn!(error = %err, "Failed to unwatch {}", path.display());
                        }
                    }
                }
                _ => continue,
            }
        }

        if should_update {
            match self.load_once() {
                Ok(config) => {
                    crate::config::set_current_config(config);
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
