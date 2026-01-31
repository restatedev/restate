// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Integration tests for the deterministic simulation framework.

use std::ops::RangeInclusive;

use googletest::prelude::*;
use test_log::test;
use tracing::info;

use restate_core::TaskCenter;
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_simulation::{InvokerBehavior, PartitionSimulation, PartitionSimulationConfig};
use restate_types::config::StorageOptions;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::partitions::Partition;
use restate_worker::state_machine::Action;

/// Creates a test storage setup.
async fn create_test_storage() -> restate_partition_store::PartitionStore {
    RocksDbManager::init();
    let storage_options = StorageOptions::default();
    info!(
        "Using RocksDB temp directory {}",
        storage_options.data_dir("db").display()
    );
    let manager = PartitionStoreManager::create().await.unwrap();
    manager
        .open(
            &Partition::new(
                PartitionId::MIN,
                RangeInclusive::new(PartitionKey::MIN, PartitionKey::MAX),
            ),
            None,
        )
        .await
        .unwrap()
}

/// Shuts down the test environment.
async fn shutdown_test_env() {
    TaskCenter::shutdown_node("test complete", 0).await;
    RocksDbManager::get().shutdown().await;
}

/// Tests that the simulation produces expected actions for an invocation
/// and completes successfully.
#[test(restate_core::test)]
async fn test_invocation_actions_sequence() -> googletest::Result<()> {
    let storage = create_test_storage().await;

    let config = PartitionSimulationConfig {
        seed: 123,
        max_steps: 100,
        partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
        check_invariants: true,
    };

    let mut sim = PartitionSimulation::new(config, storage, InvokerBehavior::ImmediateSuccess);

    // Enqueue an invocation
    let invocation = sim.random_invocation();
    sim.enqueue_invocation(invocation);

    // Step through and collect actions
    let mut all_actions: Vec<Action> = Vec::new();
    let mut saw_invoke_action = false;
    let mut step_count = 0;

    // Use should_continue() to check before each step
    while sim.should_continue() {
        match sim.step().await {
            Ok(result) => {
                step_count += 1;
                info!(
                    step = step_count,
                    command = ?result.command,
                    num_actions = result.actions.len(),
                    "Step completed"
                );

                for action in &result.actions {
                    if matches!(action, Action::Invoke { .. } | Action::VQInvoke { .. }) {
                        saw_invoke_action = true;
                    }
                }
                all_actions.extend(result.actions);
            }
            Err(restate_simulation::SimulationError::NoPendingWork) => break,
            Err(e) => return Err(e.into()),
        }
    }

    // The first command (Invoke) should produce an Action::Invoke or Action::VQInvoke
    assert_that!(saw_invoke_action, eq(true));
    // Should have processed 4 commands: Invoke + 3 InvokerEffects
    assert_that!(step_count, eq(4));

    shutdown_test_env().await;
    Ok(())
}

/// Tests that the seeded RNG produces deterministic results.
///
/// This verifies that with the same seed, we get identical random sequences,
/// which is the foundation of deterministic simulation.
#[test]
fn test_seeded_rng_determinism() {
    use rand::{Rng, SeedableRng, rngs::StdRng};

    let seed = 999u64;

    // Create two RNGs with the same seed
    let mut rng1 = StdRng::seed_from_u64(seed);
    let mut rng2 = StdRng::seed_from_u64(seed);

    // Generate sequences of random values - they should be identical
    for _ in 0..100 {
        let v1: u64 = rng1.random();
        let v2: u64 = rng2.random();
        assert_that!(v1, eq(v2));
    }

    // Verify that different seeds produce different results
    let mut rng3 = StdRng::seed_from_u64(seed + 1);
    let v1: u64 = StdRng::seed_from_u64(seed).random();
    let v3: u64 = rng3.random();
    assert_that!(v1, not(eq(v3)));
}
