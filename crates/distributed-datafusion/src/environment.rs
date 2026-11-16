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
use datafusion::common::TableReference;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::{SessionState, SessionStateBuilder, SessionStateDefaults};
use datafusion::logical_expr::registry::{ExtensionTypeRegistry, MemoryExtensionTypeRegistry};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::prelude::SessionContext;

use restate_clock::{AtomicStorage, HlcClock, UniqueTimestamp, WallClock};
use restate_storage_query_api::SessionOptions;
use restate_types::config::QueryEngineOptions;
use restate_util_string::ReString;

use crate::{BuildError, QuerySource, RegisterTable, TableInventoryBuilder};

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

struct RegisteredTable {
    name: TableReference,
    provider: Arc<dyn TableProvider>,
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

    /// Runs on `runtime`, and so its memory pool and spill files, instead of this
    /// environment's own. Node setup passes V1's runtime, so both engines draw on
    /// one `memory-size` budget.
    pub fn with_runtime(mut self, runtime: Arc<RuntimeEnv>) -> Self {
        self.runtime = runtime;
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

    /// Registers tables and views as their dependencies become available.
    /// Existing sessions retain their catalogs; subsequent sessions see the new inventory.
    pub async fn register_tables(&self, tables: impl RegisterTable) -> Result<(), BuildError> {
        tables.register(&mut TableInventoryBuilder::new(self)).await
    }

    /// Registers a source once with its canonical SQL name.
    /// Sessions may select it by schema or expose it under a different name.
    pub fn register_table<T: QuerySource>(
        &self,
        schema: &str,
        name: &str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), DataFusionError> {
        self.register_provider(
            T::identity(),
            TableReference::full("restate", schema, name),
            provider,
        )
    }

    pub(crate) fn register_provider(
        &self,
        identity: ReString,
        name: TableReference,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), DataFusionError> {
        match self.tables.entry(identity) {
            Entry::Vacant(entry) => {
                entry.insert(RegisteredTable { name, provider });
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
    /// Later registrations affect subsequent sessions, never an existing catalog.
    pub(crate) fn create_session(
        &self,
        options: &SessionOptions,
    ) -> Result<SessionContext, DataFusionError> {
        let mut state = self.build_session_state()?;
        if let Some(schema) = &options.default_schema {
            state.config_mut().options_mut().catalog.default_schema = schema.to_string();
        }
        let default_schema = options.default_schema.as_ref().map_or(
            self.config.options().catalog.default_schema.as_str(),
            ReString::as_str,
        );
        let ctx = SessionContext::new_with_state(state);
        // Release inventory guards before interacting with DataFusion's catalogs.
        let tables: Vec<_> = match &options.tables {
            Some(tables) => tables
                .iter()
                .filter(|table| !options.excluded_tables.contains(&table.identity))
                .filter_map(|table| {
                    self.table_provider(&table.identity)
                        .map(|provider| (table.name.clone(), provider))
                })
                .collect(),
            None => self
                .tables
                .iter()
                .filter(|entry| !options.excluded_tables.contains(entry.key()))
                .filter(|entry| {
                    entry.name.schema().is_some_and(|schema| {
                        options
                            .schemas
                            .iter()
                            .any(|selected| selected.as_str() == schema)
                    })
                })
                .map(|entry| (entry.name.clone(), Arc::clone(&entry.provider)))
                .collect(),
        };
        for (name, provider) in tables {
            let name = name.resolve(
                &self.config.options().catalog.default_catalog,
                default_schema,
            );
            let catalog = ctx.catalog(&name.catalog).unwrap_or_else(|| {
                let catalog = Arc::new(MemoryCatalogProvider::new());
                ctx.register_catalog(name.catalog.as_ref(), catalog.clone());
                catalog
            });
            if catalog.schema(&name.schema).is_none() {
                catalog.register_schema(&name.schema, Arc::new(MemorySchemaProvider::new()))?;
            }
            ctx.register_table(name, provider)?;
        }
        Ok(ctx)
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
        // After the rules that create filters, before distribution ships them.
        physical_rules.push(Arc::new(FilterBatchSize));
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

/// Gives every filter the session's batch size.
///
/// DataFusion's planner does, but its filter pushdown through a union doesn't:
/// when a union's inputs can't take a filter, it wraps each input in a new
/// filter with the default batch size of 8192 rows
/// (`UnionExec::handle_child_pushdown_result`). V2 plans a query over several
/// owners' partitions as such a union.
///
/// A filter holds the rows it matched until it has a batch, reaches its fetch
/// (from a LIMIT) or its input ends. So under `… LIMIT 250`, each owner's filter
/// emitted nothing until it had all 250 matches or ran out of input. With the
/// session's 128 it emits its first 128 matches as soon as it has them, and the
/// limit can be met from several owners.
#[derive(Debug)]
pub(crate) struct FilterBatchSize;

impl PhysicalOptimizerRule for FilterBatchSize {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch_size = usize::from(config.execution.batch_size);
        Ok(plan
            .transform_up(|node| match node.downcast_ref::<FilterExec>() {
                Some(filter) if filter.batch_size() != batch_size => Ok(Transformed::yes(
                    Arc::new(filter.with_batch_size(batch_size)?) as Arc<dyn ExecutionPlan>,
                )),
                _ => Ok(Transformed::no(node)),
            })?
            .data)
    }

    fn name(&self) -> &str {
        "restate_filter_batch_size"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::DataType;

    use restate_storage_query_api::{SessionTable, ShardingScheme};

    use super::*;

    restate_storage_query_api::define_schema! {
        inventory_test {
            name: "inventory_test",
            sharding: ShardingScheme::Coordinator,
            columns { value: i32 => DataType::Int32, }
        }
    }
    crate::tables::table_adapter!(TestTable, InventoryTest);

    /// A filter pushed through a union keeps the session's batch size, which
    /// DataFusion's union pushdown alone replaces with its default, and keeps
    /// the fetch a LIMIT gives it. The rows are the same either way; only the
    /// batches the inputs' filters emit differ.
    #[tokio::test]
    async fn filters_keep_the_session_batch_size_through_union_pushdown() {
        use datafusion::arrow::array::{Int32Array, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::tree_node::TreeNodeRecursion;
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{binary, col, lit};
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::filter::FilterExecBuilder;
        use datafusion::physical_plan::limit::GlobalLimitExec;
        use datafusion::physical_plan::union::UnionExec;
        use datafusion::physical_plan::{collect, execute_stream};
        use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};
        use futures::TryStreamExt;

        let options = HashMap::from([(
            "datafusion.execution.batch_size".to_owned(),
            "128".to_owned(),
        )]);
        let state = DataFusionEnv::new(64 * 1024 * 1024, None, Some(1), &options)
            .unwrap()
            .build_session_state()
            .unwrap();
        let task = state.task_ctx();
        // Two inputs (like two owners' partitions) of 1,000 rows each, read in
        // batches of 100, of which every fourth row matches `a % 4 = 0`.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let input = || {
            let batches = (0..10)
                .map(|n| {
                    let values = Int32Array::from_iter_values(n * 100..(n + 1) * 100);
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap()
                })
                .collect();
            MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap()
                as Arc<dyn ExecutionPlan>
        };
        let predicate = binary(
            binary(
                col("a", &schema).unwrap(),
                Operator::Modulo,
                lit(4),
                &schema,
            )
            .unwrap(),
            Operator::Eq,
            lit(0),
            &schema,
        )
        .unwrap();
        // `SELECT a FROM (input UNION ALL input) WHERE a % 4 = 0 LIMIT 250`. The
        // optimizer turns the limit into the fetch of a `CoalescePartitionsExec`
        // above the union, and gives each input's filter that fetch too.
        let plan: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            Arc::new(
                FilterExecBuilder::new(
                    predicate,
                    UnionExec::try_new(vec![input(), input()]).unwrap(),
                )
                .with_batch_size(128)
                .build()
                .unwrap(),
            ),
            0,
            Some(250),
        ));
        let optimize = |rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>]| {
            rules.iter().fold(Arc::clone(&plan), |plan, rule| {
                rule.optimize(plan, state.config_options()).unwrap()
            })
        };
        let filters = |plan: &Arc<dyn ExecutionPlan>| {
            let mut filters = Vec::new();
            plan.apply(|node| {
                if node.is::<FilterExec>() {
                    filters.push(Arc::clone(node));
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            filters
        };
        let rules = state.physical_optimizers();
        let without: Vec<_> = rules
            .iter()
            .filter(|rule| rule.name() != FilterBatchSize.name())
            .cloned()
            .collect();
        let mut outcomes = Vec::new();
        for plan in [optimize(&without), optimize(rules)] {
            let mut targets = Vec::new();
            let mut emitted = Vec::new();
            for filter in filters(&plan) {
                let exec = filter.downcast_ref::<FilterExec>().unwrap();
                targets.push((exec.batch_size(), filter.fetch()));
                let batches: Vec<RecordBatch> = execute_stream(filter, Arc::clone(&task))
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                emitted.push(
                    batches
                        .iter()
                        .map(RecordBatch::num_rows)
                        .collect::<Vec<_>>(),
                );
            }
            let mut rows: Vec<i32> = collect(plan, Arc::clone(&task))
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            rows.sort_unstable();
            outcomes.push((targets, emitted, rows));
        }
        let [
            (old_targets, old_emitted, old_rows),
            (targets, emitted, rows),
        ] = <[_; 2]>::try_from(outcomes).unwrap();
        // DataFusion's union pushdown gives each input a filter with the default
        // target; LIMIT gives it fetch 250, which bounds what it collects.
        assert_eq!(old_targets, [(8192, Some(250)), (8192, Some(250))]);
        assert_eq!(targets, [(128, Some(250)), (128, Some(250))]);
        // With the default target, each input's filter holds its rows until it
        // has 250; with the session's, it emits the first 128 as soon as it has
        // them.
        assert_eq!(old_emitted, [vec![250], vec![250]]);
        assert_eq!(emitted, [vec![128, 122], vec![128, 122]]);
        // The whole query returns its LIMIT of matching rows either way; which
        // matches it picks may differ, since unordered LIMIT keeps any 250.
        for rows in [&old_rows, &rows] {
            assert_eq!(rows.len(), 250);
            assert!(rows.iter().all(|a| a % 4 == 0 && (0..1000).contains(a)));
        }

        // The plan codec carries both to the workers.
        let filter: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExecBuilder::new(lit(true), Arc::new(EmptyExec::new(Arc::clone(&schema))))
                .with_batch_size(128)
                .with_fetch(Some(250))
                .build()
                .unwrap(),
        );
        let decoded =
            physical_plan_from_bytes(&physical_plan_to_bytes(filter).unwrap(), &task).unwrap();
        assert_eq!(
            (
                decoded.downcast_ref::<FilterExec>().unwrap().batch_size(),
                decoded.fetch()
            ),
            (128, Some(250))
        );
    }

    /// Node setup gives V2 the default engine's runtime and rate limiter: both
    /// engines draw on one memory pool and one `/query` budget.
    #[restate_core::test]
    async fn engines_share_the_node_query_memory_and_rate_limit() {
        use datafusion::execution::memory_pool::MemoryConsumer;
        use restate_storage_query_api::errors::SessionError;
        use restate_storage_query_api::{QueryEngineVersion, SessionOptions};
        use restate_storage_query_datafusion as v1;
        use restate_types::config::ThrottlingOptions;

        const MIB: usize = 1024 * 1024;
        let v1_env = v1::DataFusionEnv::new(MIB, None, Some(1), &HashMap::new())
            .unwrap()
            .with_mock_clock(restate_clock::MockClock::new())
            .unwrap();
        let v2_env = DataFusionEnv::new(MIB, None, Some(1), &HashMap::new())
            .unwrap()
            .with_mock_clock(restate_clock::MockClock::new())
            .unwrap()
            .with_runtime(Arc::clone(v1_env.runtime()));
        let v1_query = MemoryConsumer::new("v1-query").register(&v1_env.runtime().memory_pool);
        v1_query.try_grow(MIB).unwrap();
        assert!(
            MemoryConsumer::new("v2-query")
                .register(&v2_env.runtime.memory_pool)
                .try_grow(1)
                .is_err(),
            "one_memory_pool"
        );
        drop(v1_query);

        let limiter = v1::context::query_rate_limiter(Some(&ThrottlingOptions {
            rate: "1/h".parse().unwrap(),
            capacity: std::num::NonZeroU32::new(1),
        }));
        let engine = v1::context::DataFusionQueryEngine::with_rate_limiter(v1_env, limiter.clone())
            .with_engine(crate::context::DataFusionQueryEngine::with_rate_limiter(
                v2_env, limiter,
            ));
        let options = |version| SessionOptions {
            engine_version: Some(version),
            apply_rate_limit: true,
            ..Default::default()
        };
        assert!(
            engine
                .create_session(options(QueryEngineVersion::V1))
                .is_ok()
        );
        assert!(
            matches!(
                engine.create_session(options(QueryEngineVersion::V2)),
                Err(SessionError::RateLimited(_))
            ),
            "one_rate_limit"
        );
    }

    #[tokio::test]
    async fn catalogs_share_providers_but_isolate_names_and_late_registration() {
        let env = DataFusionEnv::new(10 * 1024 * 1024, None, None, &HashMap::new()).unwrap();
        let component = env.clone();
        let binding = SessionTable::new(TestTable::identity(), "reports.visible.alias");
        let selected = SessionOptions {
            tables: Some(vec![binding.clone()]),
            ..Default::default()
        };
        let before = env.create_session(&selected).unwrap();
        assert!(before.table_provider(binding.name.clone()).await.is_err());

        let bootstrap = env.create_session(&SessionOptions::default()).unwrap();
        bootstrap
            .sql("CREATE VIEW fixture AS SELECT 7::INT AS value")
            .await
            .unwrap();
        let provider = bootstrap.table_provider("fixture").await.unwrap();
        component
            .register_table::<TestTable>("public", "inventory_test", Arc::clone(&provider))
            .unwrap();
        drop(bootstrap);

        let first = env.create_session(&selected).unwrap();
        let second = env.create_session(&selected).unwrap();
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
        for tables in [None, selected.tables.clone()] {
            let excluded = env
                .create_session(&SessionOptions {
                    tables,
                    excluded_tables: vec![TestTable::identity()],
                    ..Default::default()
                })
                .unwrap();
            assert!(excluded.table_provider("inventory_test").await.is_err());
            assert!(excluded.table_provider(binding.name.clone()).await.is_err());
        }

        first.deregister_table(binding.name.clone()).unwrap();
        assert!(first.table_provider(binding.name.clone()).await.is_err());
        assert!(second.table_provider(binding.name.clone()).await.is_ok());
        assert!(Arc::ptr_eq(
            &env.table_provider(&TestTable::identity()).unwrap(),
            &provider
        ));
        assert!(
            component
                .register_table::<TestTable>("public", "inventory_test", Arc::clone(&provider))
                .is_err()
        );
        assert!(
            env.create_session(&SessionOptions {
                tables: Some(vec![binding.clone(), binding]),
                ..Default::default()
            })
            .is_err()
        );
        assert!(
            env.create_session(&SessionOptions {
                tables: Some(vec![]),
                ..Default::default()
            })
            .unwrap()
            .table_provider("inventory_test")
            .await
            .is_err()
        );
        assert!(
            env.create_session(&SessionOptions {
                schemas: vec![],
                ..Default::default()
            })
            .unwrap()
            .table_provider("inventory_test")
            .await
            .is_err()
        );
        assert!(
            env.create_session(&SessionOptions::default())
                .unwrap()
                .table_provider("inventory_test")
                .await
                .is_ok()
        );
    }
}
