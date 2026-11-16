// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::physical_plan::collect;
use serde_json::Value;

use restate_metadata_store::MetadataStoreClient;
use restate_types::cluster_state::NodeState;
use restate_types::deployment::{DeploymentAddress, Headers};
use restate_types::identifiers::{DeploymentId, ServiceRevision};
use restate_types::net::address::{AdvertisedAddress, HttpIngressPort};
use restate_types::schema::deployment::{Deployment, DeploymentResolver};
use restate_types::schema::service::test_util::MockServiceMetadataResolver;
use restate_types::schema::service::{ServiceMetadata, ServiceMetadataResolver};

use crate::local_scanners::register_metadata_scanners;
use crate::table_providers::GenericTableProvider;

use super::*;

restate_storage_query_api::define_schema! {
    coordinator_probe {
        name: "coordinator_probe",
        sharding: ShardingScheme::Coordinator,
        columns {
            plain_node_id: ReString => DataType::Utf8,
            gen_node_id: ReString => DataType::Utf8,
        }
    }
}
crate::tables::table_adapter!(CoordinatorProbeTable, CoordinatorProbe);

restate_storage_query_api::define_schema! {
    role_probe {
        name: "role_probe",
        sharding: ShardingScheme::Unsharded(Some(Role::LogServer)),
        columns {
            plain_node_id: ReString => DataType::Utf8,
            gen_node_id: ReString => DataType::Utf8,
        }
    }
}
crate::tables::table_adapter!(RoleProbeTable, RoleProbe);

#[derive(Clone, Debug, Default)]
struct ServiceCatalog(Arc<Mutex<MockServiceMetadataResolver>>);

impl ServiceMetadataResolver for ServiceCatalog {
    fn resolve_latest_service(&self, name: impl AsRef<str>) -> Option<ServiceMetadata> {
        self.0.lock().resolve_latest_service(name)
    }

    fn resolve_latest_service_openapi(
        &self,
        _: impl AsRef<str>,
        _: AdvertisedAddress<HttpIngressPort>,
    ) -> Option<Value> {
        None
    }

    fn list_services(&self) -> Vec<ServiceMetadata> {
        self.0.lock().list_services()
    }

    fn list_service_names(&self) -> Vec<String> {
        self.0.lock().list_service_names()
    }
}

impl DeploymentResolver for ServiceCatalog {
    fn resolve_latest_deployment_for_service(&self, name: impl AsRef<str>) -> Option<Deployment> {
        ().resolve_latest_deployment_for_service(name)
    }

    fn find_deployment(
        &self,
        address: &DeploymentAddress,
        headers: &Headers,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        ().find_deployment(address, headers)
    }

    fn get_deployment(&self, id: &DeploymentId) -> Option<Deployment> {
        ().get_deployment(id)
    }

    fn get_deployment_and_services(
        &self,
        id: &DeploymentId,
    ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
        ().get_deployment_and_services(id)
    }

    fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
        ().get_deployments()
    }
}

async fn query_error(session: &dyn QuerySession, sql: &str) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        match session.execute(sql, QueryOptions {}).await {
            Err(error) => error.to_string(),
            Ok(result) => {
                let error = result.stream.try_collect::<Vec<_>>().await.unwrap_err();
                assert!(
                    result.diagnostics.warnings().is_empty(),
                    "singleton_errors_are_terminal"
                );
                error.to_string()
            }
        }
    })
    .await
    .expect("singleton queries must terminate")
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn singleton_placement_preserves_coordinator_scope_roles_liveness_and_fresh_metadata() {
    let catalog = ServiceCatalog::default();
    for name in ["alpha", "beta", "gamma"] {
        catalog
            .0
            .lock()
            .add(ServiceMetadata::mock_service(name, ["handler"]));
    }
    let schemas = Live::from_value(catalog.clone());
    let probe_reads = Arc::new(Mutex::new(Vec::new()));
    let fixture = setup_with_scanners(|node, sources| {
        if node.raw_id() != 1 {
            register_metadata_scanners(
                schemas.clone(),
                MetadataStoreClient::new_in_memory(),
                None,
                sources,
            );
            let scanner = Arc::new(NodeRows {
                node,
                reads: Arc::clone(&probe_reads),
            });
            sources.register_node_scanner::<CoordinatorProbeTable>(scanner.clone());
            sources.register_node_scanner::<RoleProbeTable>(scanner);
        }
    })
    .await;
    let mut liveness = TaskCenter::current().cluster_state().clone().updater();
    for id in [2, 3] {
        liveness.upsert_node_state(GenerationalNodeId::new(id, 1), NodeState::Alive);
    }
    let local = fixture
        .scanners
        .local_node_scanner(&NodeRows::identity())
        .unwrap();
    let env = environment(4, 1).with_distributed_execution(fixture.network.clone());
    env.register_table::<SysServiceTable>(
        "public",
        "sys_service",
        Arc::new(GenericTableProvider::declared::<SysServiceTable>(
            None,
            Some(fixture.scanners.clone()),
        )),
    )
    .unwrap();
    env.register_provider(
        "local_service".into(),
        "restate.public.local_service".into(),
        Arc::new(GenericTableProvider::declared::<SysServiceTable>(
            Some(Arc::new(SysServiceTable::create_local_scanner(schemas))),
            Some(fixture.scanners.clone()),
        )),
    )
    .unwrap();
    env.register_table::<CoordinatorProbeTable>(
        "public",
        "coordinator_probe",
        Arc::new(GenericTableProvider::declared::<CoordinatorProbeTable>(
            Some(local.clone()),
            Some(fixture.scanners.clone()),
        )),
    )
    .unwrap();
    env.register_provider(
        "missing_coordinator".into(),
        "restate.public.missing_coordinator".into(),
        Arc::new(GenericTableProvider::declared::<CoordinatorProbeTable>(
            None,
            Some(fixture.scanners.clone()),
        )),
    )
    .unwrap();
    env.register_table::<RoleProbeTable>(
        "public",
        "role_probe",
        Arc::new(GenericTableProvider::declared::<RoleProbeTable>(
            Some(local),
            Some(fixture.scanners.clone()),
        )),
    )
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env.clone(), None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let session = session.as_ref();

    #[rustfmt::skip]
    let cases: &[(&str, &[&[&str]], usize)] = &[
        ("SELECT COUNT(*) FROM local_service",                                           &[&["3"]],     0),
        ("SELECT COUNT(*) FROM sys_service",                                             &[&["3"]],     1),
        ("SELECT name FROM sys_service ORDER BY name",                                    &[&["alpha"], &["beta"], &["gamma"]], 1),
        ("SELECT name FROM sys_service WHERE name <> 'alpha' ORDER BY name LIMIT 1",       &[&["beta"]],  1),
        ("SELECT plain_node_id FROM coordinator_probe",                                   &[&["N1"]],    0),
        ("SELECT COUNT(*) FROM coordinator_probe WHERE plain_node_id = 'N2'",               &[&["0"]],     0),
        ("SELECT plain_node_id, COUNT(*) FROM coordinator_probe CROSS JOIN sys_service GROUP BY plain_node_id", &[&["N1", "3"]], 1),
        ("SELECT plain_node_id FROM role_probe",                                          &[&["N2"]],    1),
        ("SELECT COUNT(*) FROM role_probe WHERE plain_node_id = 'N1'",                      &[&["0"]],     1),
    ];
    for &(sql, expected, task_count) in cases {
        let before = fixture.tasks.lock().len();
        assert_sql(
            session,
            "singleton_reads_exactly_one_complete_source",
            sql,
            expected,
        )
        .await;
        let tasks = fixture.tasks.lock();
        assert_eq!(
            tasks.len() - before,
            task_count,
            "local_first_and_single_remote_owner: {sql}"
        );
        assert!(
            tasks[before..]
                .iter()
                .all(|task| task.owner == GenerationalNodeId::new(2, 1))
        );
    }
    let before = fixture.tasks.lock().len();
    let error = query_error(session, "SELECT COUNT(*) FROM missing_coordinator").await;
    assert!(
        error.contains("local singleton source 'coordinator_probe' is unavailable"),
        "{error}"
    );
    assert_eq!(
        fixture.tasks.lock().len(),
        before,
        "coordinator_sources_never_fall_back_to_remote_copies"
    );

    // Reusable planning retains the source, not its current metadata contents.
    let mut state = env
        .create_session(&SessionOptions::default())
        .unwrap()
        .state();
    let query_ts = env.query_clock().next();
    let metadata = restate_storage_query_api::QueryMetadata {
        session_id: state.session_id().into(),
        query_ts,
        headers: Default::default(),
        redacted_sql: "SELECT COUNT(*)".into(),
        planning_duration: Duration::ZERO,
    };
    state.config_mut().set_extension(Arc::new(query_ts));
    state.config_mut().set_extension(Arc::new(metadata));
    let context = SessionContext::new_with_state(state);
    let mut plans = Vec::new();
    for sql in [
        "SELECT COUNT(*) FROM local_service",
        "SELECT COUNT(*) FROM sys_service",
    ] {
        plans.push(
            context
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap(),
        );
    }
    catalog
        .0
        .lock()
        .add(ServiceMetadata::mock_service("delta", ["handler"]));
    for plan in plans {
        let batches =
            tokio::time::timeout(Duration::from_secs(10), collect(plan, context.task_ctx()))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            array_value_to_string(batches[0].column(0), 0).unwrap(),
            "4",
            "metadata_is_captured_at_execution_on_either_owner"
        );
    }

    liveness.upsert_node_state(GenerationalNodeId::new(2, 1), NodeState::Dead);
    let before = fixture.tasks.lock().len();
    let error = query_error(session, "SELECT COUNT(*) FROM role_probe").await;
    assert!(error.contains("no alive eligible node"), "{error}");
    assert_eq!(
        fixture.tasks.lock().len(),
        before,
        "role_requirements_cannot_fall_back_to_another_role"
    );
    // A live replacement generation cannot stand in for the configured generation.
    liveness.upsert_node_state(GenerationalNodeId::new(2, 2), NodeState::Alive);
    assert_sql(
        session,
        "skip_dead_and_stale_candidates",
        "SELECT COUNT(*) FROM sys_service",
        &[&["4"]],
    )
    .await;
    assert_eq!(
        fixture.tasks.lock().last().unwrap().owner,
        GenerationalNodeId::new(3, 1)
    );

    liveness.upsert_node_state(GenerationalNodeId::new(3, 1), NodeState::Dead);
    let before = fixture.tasks.lock().len();
    assert_sql(
        session,
        "proven_empty_needs_no_serving_node",
        "SELECT COUNT(*) FROM sys_service WHERE name = 'alpha' AND name = 'beta'",
        &[&["0"]],
    )
    .await;
    let error = query_error(session, "SELECT COUNT(*) FROM sys_service").await;
    assert!(error.contains("no alive eligible node"), "{error}");
    assert_eq!(
        fixture.tasks.lock().len(),
        before,
        "unavailability_is_not_an_empty_table"
    );
    assert_sql(
        session,
        "self_optimization_needs_no_remote_liveness",
        "SELECT COUNT(*) FROM local_service",
        &[&["4"]],
    )
    .await;

    // An alive node can fail during worker binding; the whole-table read must fail.
    liveness.upsert_node_state(GenerationalNodeId::new(4, 1), NodeState::Alive);
    let error = query_error(session, "SELECT COUNT(*) FROM sys_service").await;
    assert!(
        error.contains("local node source 'sys_service' is unavailable"),
        "{error}"
    );
}
