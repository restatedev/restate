// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::util::display::array_value_to_string;
use futures::TryStreamExt;
use tokio::sync::watch;

use restate_clock::MockClock;
use restate_core::test_env::TestCoreEnv;
use restate_storage_query_api::{QueryOptions, QuerySession, SessionOptions, SessionTable};
use restate_storage_query_datafusion as v1;
use restate_types::cluster::cluster_state::LegacyClusterState;
use restate_types::config::Configuration;

use crate::context::{DataFusionQueryEngine, SelectPartitionsFromMetadata};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::{ClusterTables, DataFusionEnv, UserTables};

const STATS: [&str; 3] = [
    "sys_service_stats",
    "sys_deployment_stats",
    "sys_virtual_object_stats",
];
const INDEXES: [&str; 7] = [
    "_idx_entry_by_service",
    "_idx_entry_by_stage",
    "_idx_entry_next_at_by_service",
    "_idx_entry_by_virtual_object",
    "_idx_entry_next_at_by_virtual_object",
    "_idx_busy_vqueue",
    "_idx_entry_next_at_by_stage",
];

async fn table_names<T>(session: &dyn QuerySession<T>, schema: &str) -> BTreeSet<String> {
    let batches: Vec<_> = session
        .execute(
            &format!(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            ),
            QueryOptions {},
        )
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| array_value_to_string(batch.column(0), row).unwrap())
        })
        .collect()
}

#[tokio::test]
async fn experimental_table_visibility_is_fixed_at_session_creation_in_both_engines() {
    let previous_config = Configuration::pinned().clone();
    let mut config = Configuration::default();
    Configuration::set(config.clone());

    let v1 = v1::context::DataFusionQueryEngine::with_user_tables(
        v1::DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
            .unwrap()
            .with_mock_clock(MockClock::new())
            .unwrap(),
        None,
        v1::UserTables::new(
            v1::context::SelectPartitionsFromMetadata,
            v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
                restate_core::MetadataBuilder::default().to_metadata(),
            ),
        ),
    )
    .await
    .unwrap();
    let v2 = DataFusionQueryEngine::with_user_tables(
        DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
            .unwrap()
            .with_mock_clock(MockClock::new())
            .unwrap(),
        None,
        UserTables::new(
            SelectPartitionsFromMetadata,
            RemoteScannerManager::local_only(
                restate_core::MetadataBuilder::default().to_metadata(),
            ),
        ),
    )
    .await
    .unwrap();
    let engines = [("v1", v1, true), ("v2", v2, false)];
    let mut enabled_sessions = Vec::new();
    for (stats, indexes) in [
        (false, false),
        (true, false),
        (false, true),
        (true, true),
        (false, false),
    ] {
        config.common.experimental.set_track_status_gauges(stats);
        config.common.experimental.set_indexes_v1(indexes);
        Configuration::set(config.clone());
        for (engine_name, engine, supports_indexes) in &engines {
            let session = engine.create_session(SessionOptions::default()).unwrap();
            let names = table_names(session.as_ref(), "public").await;
            // Bootstrap views and ordinary tables remain usable even with both gates off.
            assert!(names.contains("state") && names.contains("sys_invocation"));
            let selected: Vec<_> = STATS
                .into_iter()
                .map(|name| (name, stats))
                .chain(
                    INDEXES
                        .into_iter()
                        .map(|name| (name, indexes && *supports_indexes)),
                )
                .collect();
            for (name, visible) in &selected {
                assert_eq!(names.contains(*name), *visible, "{engine_name}: {name}");
                if !visible {
                    assert!(
                        session
                            .execute(&format!("SELECT * FROM {name}"), QueryOptions {})
                            .await
                            .is_err()
                    );
                }
            }

            // Explicit selections, including renamed identities, cannot bypass a gate.
            let aliases = engine
                .create_session(SessionOptions {
                    tables: Some(
                        selected
                            .iter()
                            .enumerate()
                            .map(|(i, (name, _))| SessionTable::new(*name, format!("selected_{i}")))
                            .collect(),
                    ),
                    ..Default::default()
                })
                .unwrap();
            let expected: BTreeSet<_> = selected
                .iter()
                .enumerate()
                .filter(|(_, (_, visible))| *visible)
                .map(|(i, _)| format!("selected_{i}"))
                .collect();
            assert_eq!(
                table_names(aliases.as_ref(), "public").await,
                expected,
                "{engine_name}"
            );
            if stats && indexes {
                enabled_sessions.push((session, *supports_indexes));
            }
        }
    }
    // Disabling a flag affects only new sessions, not catalogs already constructed.
    for (session, supports_indexes) in enabled_sessions {
        let names = table_names(session.as_ref(), "public").await;
        for table in STATS {
            assert!(names.contains(table));
        }
        for table in INDEXES {
            assert_eq!(names.contains(table), supports_indexes);
        }
    }
    Configuration::set(previous_config);
}

#[restate_core::test]
async fn config_table_can_be_enabled_after_registration_in_both_engines() {
    let previous_config = Configuration::pinned().clone();
    let mut config = Configuration::default();
    config.common.disable_config_sql_table = true;
    Configuration::set(config.clone());
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let (_tx, cluster_state) = watch::channel(Arc::new(LegacyClusterState::empty()));

    let v1_env = v1::DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let v2_env = DataFusionEnv::new(10 * 1024 * 1024, None, Some(1), &HashMap::new())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let v1 = v1::context::DataFusionQueryEngine::with_cluster_tables(
        v1_env.clone(),
        v1::ClusterTables::new(
            Default::default(),
            cluster_state.clone(),
            v1::remote_query_scanner_manager::RemoteScannerManager::local_only(
                core.metadata.clone(),
            ),
        ),
    )
    .await
    .unwrap();
    let v2 = DataFusionQueryEngine::with_cluster_tables(
        v2_env.clone(),
        ClusterTables::new(
            Default::default(),
            cluster_state,
            RemoteScannerManager::local_only(core.metadata.clone()),
        ),
    )
    .await
    .unwrap();
    assert!(v1_env.table_provider("config").is_some());
    assert!(v2_env.table_provider("config").is_some());

    let engines = [("v1", v1), ("v2", v2)];
    let mut sessions = Vec::new();
    for disabled in [true, false, true] {
        config.common.disable_config_sql_table = disabled;
        Configuration::set(config.clone());
        for (name, engine) in &engines {
            let session = engine.create_session(SessionOptions::default()).unwrap();
            let names = table_names(session.as_ref(), "cluster").await;
            assert_eq!(names.contains("config"), !disabled, "{name}");
            assert!(names.contains("nodes") && names.contains("logs_tail_segments"));
            if disabled {
                assert!(
                    session
                        .execute("SELECT * FROM restate.cluster.config", QueryOptions {})
                        .await
                        .is_err()
                );
            }
            let alias = engine
                .create_session(SessionOptions {
                    tables: Some(vec![SessionTable::new("config", "settings")]),
                    ..Default::default()
                })
                .unwrap();
            let names = table_names(alias.as_ref(), "cluster").await;
            assert_eq!(names.contains("settings"), !disabled, "{name}");
            assert_eq!(names.len(), usize::from(!disabled));
            sessions.push((session, disabled));
        }
    }
    for (session, disabled) in sessions {
        assert_eq!(
            table_names(session.as_ref(), "cluster")
                .await
                .contains("config"),
            !disabled
        );
    }
    Configuration::set(previous_config);
}
