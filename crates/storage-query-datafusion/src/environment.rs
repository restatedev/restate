// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SessionState, SessionStateBuilder, SessionStateDefaults};
use datafusion::logical_expr::registry::{ExtensionTypeRegistry, MemoryExtensionTypeRegistry};

use restate_types::config::QueryEngineOptions;

/// Shared DataFusion resources and defaults used to construct independent session state.
///
/// Clones share the runtime and its memory pool. Each call to [`Self::build_session_state`]
/// creates a fresh session identity, configuration copy, and catalog container while reusing
/// that runtime. The environment itself is not a user session and does not register Restate's
/// tables or views; callers install the catalog appropriate to their query-engine profile.
#[derive(Clone)]
pub struct DataFusionEnv {
    runtime: Arc<RuntimeEnv>,
    config: SessionConfig,
}

impl DataFusionEnv {
    pub fn from_options(options: &QueryEngineOptions) -> Result<Self, DataFusionError> {
        Self::new(
            options.memory_size.get(),
            options.tmp_dir.clone(),
            options.query_parallelism(),
            &options.datafusion_options,
        )
    }

    /// Initializes the shared runtime and session defaults.
    ///
    /// `memory_limit` is the runtime's memory-pool limit in bytes. `temp_folder` selects the
    /// spill-file directory; `None` retains DataFusion's default policy. `default_parallelism`
    /// sets the target partition count, with `None` or zero retaining DataFusion's default.
    ///
    /// Session defaults enable the information schema, use a batch size of 128, and resolve
    /// unqualified tables in `restate.public`. `datafusion_options` are applied last and may
    /// override these settings, including the target partition count.
    ///
    /// # Errors
    ///
    /// Returns an error if runtime initialization fails or a DataFusion configuration option
    /// is invalid.
    pub fn new(
        memory_limit: usize,
        temp_folder: Option<String>,
        default_parallelism: Option<usize>,
        datafusion_options: &HashMap<String, String>,
    ) -> Result<Self, DataFusionError> {
        //
        // build the runtime
        //
        let mut runtime_config = RuntimeEnvBuilder::default().with_memory_limit(memory_limit, 1.0);

        if let Some(folder) = temp_folder {
            // todo: consider using the os temp dir if the user doesn't specify a temp folder.
            runtime_config = runtime_config.with_temp_file_path(folder);
        }
        let runtime = runtime_config.build_arc()?;
        //
        // build the session
        //
        let mut config = SessionConfig::new();
        if let Some(target_partitions) = default_parallelism {
            config = config.with_target_partitions(target_partitions);
        }

        config = config
            .with_batch_size(128)
            .with_information_schema(true)
            .with_default_catalog_and_schema("restate", "public");

        for (k, v) in datafusion_options {
            config.options_mut().set(k, v)?;
        }

        Ok(Self { runtime, config })
    }

    /// Overrides the namespace used for catalog bootstrap and subsequent sessions.
    /// Other environment clones retain their defaults and continue sharing the runtime.
    pub(crate) fn with_default_catalog_and_schema(mut self, catalog: &str, schema: &str) -> Self {
        self.config = self.config.with_default_catalog_and_schema(catalog, schema);
        self
    }

    /// Builds state for a new session using the shared runtime and a copy of the defaults.
    ///
    /// DataFusion generates a new session ID on each call. SQL functions and planners,
    /// including JSON functions, expression rewrites, and operator planning, are initialized
    /// directly on the state. Restate tables and views are not registered here.
    ///
    /// Callers can install a profile's catalog before passing the state to
    /// [`SessionContext::new_with_state`](datafusion::execution::context::SessionContext::new_with_state).
    /// That context adopts the state's session ID. Alternatively, [`SessionState::task_ctx`]
    /// produces a context for physical-expression decoding without a `SessionContext`.
    ///
    /// # Errors
    ///
    /// Returns an error if registration of JSON functions, rewrites, or planners fails.
    pub fn build_session_state(&self) -> Result<SessionState, DataFusionError> {
        let builder = SessionStateBuilder::new()
            .with_config(self.config.clone())
            .with_runtime_env(Arc::clone(&self.runtime));
        let mut state = apply_default_features(builder).build();
        datafusion_functions_json::register_all(&mut state)?;
        Ok(state)
    }
}

/// Installs the default SQL functions, expression planners, and extension types.
/// File-format and external-table factories are omitted.
///
/// Replaces the corresponding builder collections; apply before custom registrations.
fn apply_default_features(builder: SessionStateBuilder) -> SessionStateBuilder {
    let extension_registry = MemoryExtensionTypeRegistry::new_empty();
    // currently empty but left to automatically follow df changes.
    extension_registry
        .extend(&SessionStateDefaults::default_extension_types())
        .expect("MemoryExtensionTypeRegistry is not read-only.");
    let extension_registry = Arc::new(extension_registry);

    builder
        .with_expr_planners(SessionStateDefaults::default_expr_planners())
        .with_scalar_functions(SessionStateDefaults::default_scalar_functions())
        .with_higher_order_functions(SessionStateDefaults::default_higher_order_functions())
        .with_aggregate_functions(SessionStateDefaults::default_aggregate_functions())
        .with_window_functions(SessionStateDefaults::default_window_functions())
        .with_extension_type_registry(extension_registry)
        .with_table_functions(HashMap::from_iter(
            SessionStateDefaults::default_table_functions()
                .into_iter()
                .map(|f| (f.name().to_string(), f)),
        ))
}
