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

use dashmap::{DashMap, mapref::entry::Entry};
use datafusion::catalog::{MemoryCatalogProvider, MemorySchemaProvider, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SessionState, SessionStateBuilder, SessionStateDefaults};
use datafusion::logical_expr::registry::{ExtensionTypeRegistry, MemoryExtensionTypeRegistry};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::SessionContext;

use restate_clock::{AtomicStorage, HlcClock, UniqueTimestamp, WallClock};
use restate_storage_query_api::{QueryEngineTable, SessionOptions, SessionTable};
use restate_types::config::{Configuration, QueryEngineOptions};
use restate_util_string::ReString;

pub(crate) enum QueryClock {
    Wall(HlcClock<WallClock, AtomicStorage>),
    #[cfg(any(test, feature = "test-util"))]
    Mock(HlcClock<restate_clock::MockClock, AtomicStorage>),
}

impl QueryClock {
    pub(crate) fn next(&self) -> UniqueTimestamp {
        match self {
            Self::Wall(clock) => clock.next(),
            #[cfg(any(test, feature = "test-util"))]
            Self::Mock(clock) => clock.next(),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> UniqueTimestamp {
        match self {
            Self::Wall(clock) => clock.snapshot(),
            Self::Mock(clock) => clock.snapshot(),
        }
    }
}

/// Shared DataFusion resources and defaults used to construct independent session state.
///
/// Clones share the runtime, its memory pool, and the query HLC. Each call to [`Self::build_session_state`]
/// creates a fresh session identity, configuration copy, and catalog container while reusing
/// that runtime. Components populate the shared provider inventory as dependencies become
/// available; sessions select and name providers in independent catalogs.
#[derive(Clone)]
pub struct DataFusionEnv {
    runtime: Arc<RuntimeEnv>,
    query_clock: Arc<QueryClock>,
    config: SessionConfig,
    tables: Arc<DashMap<ReString, RegisteredTable>>,
}

struct RegisteredTable {
    provider: Arc<dyn TableProvider>,
    is_visible: fn(&Configuration, &SessionOptions) -> bool,
}

impl DataFusionEnv {
    /// Opts this environment into the experimental, owner-bound task runtime.
    /// Register [`crate::distributed::DistributedQueryServer`] on storage owners.
    pub fn with_distributed_execution(
        mut self,
        network: impl restate_core::network::NetworkSender,
    ) -> Self {
        crate::distributed::configure(&mut self.config, network);
        self
    }

    /// Control experiment: identical sources, placement and RPC, with operators
    /// retained on the coordinator. There is no public runtime switch for this.
    #[cfg(test)]
    pub(crate) fn without_distributed_operator_pushdown(mut self) -> Self {
        self.config
            .set_extension(Arc::new(crate::distributed::DistributedExecution {
                operator_pushdown: false,
            }));
        self
    }

    /// Sets the storage serving policy passed through planning and worker binding.
    pub fn with_storage_placement(
        mut self,
        options: crate::placement::StoragePlacementOptions,
    ) -> Self {
        self.config.set_extension(Arc::new(options));
        self
    }

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
    /// Query execution requires the binary's [`restate_clock::ClockUpkeep`] to be running.
    ///
    /// # Errors
    ///
    /// Returns an error if runtime/clock initialization fails or a DataFusion configuration option
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

        Ok(Self {
            runtime,
            query_clock: Arc::new(QueryClock::Wall(
                HlcClock::new(None, WallClock, AtomicStorage::default())
                    .map_err(|err| DataFusionError::External(Box::new(err)))?,
            )),
            config,
            tables: Arc::new(DashMap::new()),
        })
    }

    /// Shared across environment clones and sessions; query timestamps are allocated at execution.
    pub(crate) fn query_clock(&self) -> &Arc<QueryClock> {
        &self.query_clock
    }

    /// Installs a test clock without upkeep. Call before cloning the environment or creating sessions.
    #[cfg(any(test, feature = "test-util"))]
    pub fn with_mock_clock(
        mut self,
        clock: restate_clock::MockClock,
    ) -> Result<Self, DataFusionError> {
        self.query_clock = Arc::new(QueryClock::Mock(
            HlcClock::new(None, clock, AtomicStorage::default())
                .map_err(|err| DataFusionError::External(Box::new(err)))?,
        ));
        Ok(self)
    }

    /// Registers a source once. SQL exposure names belong to session catalogs.
    pub fn register_table<T: QueryEngineTable>(
        &self,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), DataFusionError> {
        self.register_provider_with_visibility(T::identity(), provider, T::is_visible)
    }

    pub(crate) fn register_provider(
        &self,
        identity: ReString,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), DataFusionError> {
        self.register_provider_with_visibility(identity, provider, |_, _| true)
    }

    fn register_provider_with_visibility(
        &self,
        identity: ReString,
        provider: Arc<dyn TableProvider>,
        is_visible: fn(&Configuration, &SessionOptions) -> bool,
    ) -> Result<(), DataFusionError> {
        match self.tables.entry(identity) {
            Entry::Vacant(entry) => {
                entry.insert(RegisteredTable {
                    provider,
                    is_visible,
                });
                Ok(())
            }
            Entry::Occupied(entry) => Err(DataFusionError::Plan(format!(
                "duplicate query table identity '{}'",
                entry.key()
            ))),
        }
    }

    /// Clones a provider without retaining a DashMap guard across planning or execution.
    pub fn table_provider(&self, identity: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables
            .get(identity)
            .map(|entry| Arc::clone(&entry.provider))
    }

    /// Builds a session-local catalog from the currently available selected providers.
    /// Each source's visibility policy is evaluated with the same configuration snapshot.
    /// Later registrations affect subsequent sessions, never an existing catalog.
    pub(crate) fn create_session(
        &self,
        tables: &[SessionTable],
        config: &Configuration,
        options: &SessionOptions,
    ) -> Result<SessionContext, DataFusionError> {
        let ctx = SessionContext::new_with_state(self.build_session_state()?);
        for table in tables {
            let provider = {
                let Some(entry) = self.tables.get(&table.identity) else {
                    continue;
                };
                if !(entry.is_visible)(config, options) {
                    continue;
                }
                Arc::clone(&entry.provider)
            };
            let name = table.name.clone().resolve(
                &self.config.options().catalog.default_catalog,
                &self.config.options().catalog.default_schema,
            );
            let catalog = ctx.catalog(&name.catalog).unwrap_or_else(|| {
                let catalog = Arc::new(MemoryCatalogProvider::new());
                ctx.register_catalog(name.catalog.as_ref(), catalog.clone());
                catalog
            });
            if catalog.schema(&name.schema).is_none() {
                catalog.register_schema(&name.schema, Arc::new(MemorySchemaProvider::new()))?;
            }
            ctx.register_table(table.name.clone(), provider)?;
        }
        Ok(ctx)
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
        let mut physical_rules = PhysicalOptimizer::new().rules;
        physical_rules.insert(0, Arc::new(crate::distributed::source::PrimaryAccessRule));
        let builder = SessionStateBuilder::new()
            .with_config(self.config.clone())
            .with_physical_optimizer_rules(physical_rules)
            .with_runtime_env(Arc::clone(&self.runtime));
        let builder = if let Some(distributed) = self
            .config
            .get_extension::<crate::distributed::DistributedExecution>()
        {
            builder.with_physical_optimizer_rule(Arc::new(
                crate::distributed::DistributedPlanRule {
                    operator_pushdown: distributed.operator_pushdown,
                },
            ))
        } else {
            builder
        };
        let mut state = apply_default_features(builder).build();
        datafusion_functions_json::register_all(&mut state)?;
        Ok(state)
    }

    pub(crate) fn build_worker_state(
        &self,
        builder: SessionStateBuilder,
    ) -> Result<SessionState, DataFusionError> {
        let mut state =
            apply_default_features(builder.with_runtime_env(Arc::clone(&self.runtime))).build();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug)]
    enum TestTable {}

    impl QueryEngineTable for TestTable {
        fn identity() -> ReString {
            ReString::from_static("inventory_test")
        }

        fn is_visible(config: &Configuration, options: &SessionOptions) -> bool {
            config.common.experimental.is_track_status_gauges_enabled()
                && !options.headers.contains_key("x-test-hide-table")
        }
    }

    #[tokio::test]
    async fn catalogs_share_providers_but_isolate_names_and_late_registration() {
        let mut config = Configuration::default();
        config.common.experimental.set_track_status_gauges(true);
        let options = SessionOptions::default();
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new()).unwrap();
        let component = env.clone();
        let binding = SessionTable::for_table::<TestTable>("reports.visible.alias");
        let before = env
            .create_session(std::slice::from_ref(&binding), &config, &options)
            .unwrap();
        assert!(before.table_provider(binding.name.clone()).await.is_err());

        let bootstrap = env.create_session(&[], &config, &options).unwrap();
        bootstrap
            .sql("CREATE VIEW fixture AS SELECT 7 AS value")
            .await
            .unwrap();
        let provider = bootstrap.table_provider("fixture").await.unwrap();
        component
            .register_table::<TestTable>(Arc::clone(&provider))
            .unwrap();
        drop(bootstrap);

        let first = env
            .create_session(std::slice::from_ref(&binding), &config, &options)
            .unwrap();
        let second = env
            .create_session(std::slice::from_ref(&binding), &config, &options)
            .unwrap();
        assert!(Arc::ptr_eq(
            &first.table_provider(binding.name.clone()).await.unwrap(),
            &provider
        ));
        assert!(Arc::ptr_eq(
            &second.table_provider(binding.name.clone()).await.unwrap(),
            &provider
        ));
        assert!(before.table_provider(binding.name.clone()).await.is_err());
        assert!(first.table_provider("inventory_test").await.is_err());

        first.deregister_table(binding.name.clone()).unwrap();
        assert!(first.table_provider(binding.name.clone()).await.is_err());
        assert!(second.table_provider(binding.name.clone()).await.is_ok());
        assert!(Arc::ptr_eq(
            &env.table_provider(&TestTable::identity()).unwrap(),
            &provider
        ));
        assert!(
            component
                .register_table::<TestTable>(Arc::clone(&provider))
                .is_err()
        );
        assert!(
            env.create_session(&[binding.clone(), binding.clone()], &config, &options)
                .is_err()
        );
        assert!(
            env.create_session(&[], &config, &options)
                .unwrap()
                .table_provider("reports.visible.alias")
                .await
                .is_err()
        );

        config.common.experimental.set_track_status_gauges(false);
        let hidden = env
            .create_session(std::slice::from_ref(&binding), &config, &options)
            .unwrap();
        assert!(hidden.table_provider(binding.name.clone()).await.is_err());
        assert!(
            hidden
                .sql("SELECT * FROM reports.visible.alias")
                .await
                .is_err()
        );
        // A disabled source still exists in the inventory, and old sessions retain it.
        assert!(env.table_provider(&TestTable::identity()).is_some());
        assert!(second.table_provider(binding.name.clone()).await.is_ok());

        config.common.experimental.set_track_status_gauges(true);
        let enabled = env
            .create_session(std::slice::from_ref(&binding), &config, &options)
            .unwrap();
        assert!(enabled.table_provider(binding.name.clone()).await.is_ok());
        assert!(hidden.table_provider(binding.name.clone()).await.is_err());
        let mut options = options;
        options
            .headers
            .insert("x-test-hide-table", "true".parse().unwrap());
        let hidden_by_options = env
            .create_session(std::slice::from_ref(&binding), &config, &options)
            .unwrap();
        assert!(
            hidden_by_options
                .table_provider(binding.name)
                .await
                .is_err()
        );
    }
}
