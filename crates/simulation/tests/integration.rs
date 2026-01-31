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

/// Main integration test that runs all simulation scenarios.
///
/// Note: All RocksDB-dependent tests must be in a single test function
/// because RocksDB is a singleton that cannot be restarted within a process.
#[test(restate_core::test)]
async fn test_partition_simulation() -> googletest::Result<()> {
    let storage = create_test_storage().await;

    // Test 1: Basic invocation completes with expected actions
    info!("=== Test 1: Basic invocation sequence ===");
    {
        let config = PartitionSimulationConfig {
            seed: 123,
            max_steps: 100,
            partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
            check_invariants: true,
        };

        let mut sim =
            PartitionSimulation::new(config, storage.clone(), InvokerBehavior::ImmediateSuccess);

        let invocation = sim.random_invocation();
        sim.enqueue_invocation(invocation);

        let mut saw_invoke_action = false;
        let mut step_count = 0;

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
                }
                Err(restate_simulation::SimulationError::NoPendingWork) => break,
                Err(e) => return Err(e.into()),
            }
        }

        assert_that!(saw_invoke_action, eq(true));
        assert_that!(step_count, eq(4)); // Invoke + 3 InvokerEffects
        info!(
            "Test 1 passed: Basic invocation completed in {} steps",
            step_count
        );
    }

    // Test 2: VO exclusivity stress test with probabilistic invoker
    info!("=== Test 2: VO exclusivity stress test ===");
    {
        let config = PartitionSimulationConfig {
            seed: 42,
            max_steps: 500,
            partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
            check_invariants: true,
        };

        let mut sim = PartitionSimulation::new(
            config,
            storage.clone(),
            InvokerBehavior::Probabilistic {
                success_rate: 0.6,
                failure_rate: 0.2,
            },
        );

        // Enqueue many invocations targeting small key space
        for _ in 0..20 {
            let invocation = sim.random_vo_invocation();
            info!(
                invocation_id = ?invocation.invocation_id,
                target = ?invocation.invocation_target,
                "Enqueueing VO invocation"
            );
            sim.enqueue_invocation(invocation);
        }

        let outcome = sim.run().await?;

        info!(
            steps = outcome.steps_executed,
            success = outcome.success,
            violations = ?outcome.violations,
            "Stress test completed"
        );

        assert_that!(outcome.success, eq(true));
        assert_that!(outcome.violations, empty());
        assert_that!(outcome.steps_executed, gt(0));
        info!(
            "Test 2 passed: VO exclusivity held after {} steps",
            outcome.steps_executed
        );
    }

    // Test 3: Lock release on failure
    info!("=== Test 3: Lock release on failure ===");
    {
        let config = PartitionSimulationConfig {
            seed: 456,
            max_steps: 100,
            partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
            check_invariants: true,
        };

        let mut sim = PartitionSimulation::new(
            config,
            storage.clone(),
            InvokerBehavior::Probabilistic {
                success_rate: 0.0,
                failure_rate: 1.0,
            },
        );

        let inv1 = sim.invocation_for_key("key-a");
        let inv2 = sim.invocation_for_key("key-a");
        info!(inv1 = ?inv1.invocation_id, inv2 = ?inv2.invocation_id, "Testing lock release");
        sim.enqueue_invocation(inv1);
        sim.enqueue_invocation(inv2);

        let outcome = sim.run().await?;

        assert_that!(outcome.success, eq(true));
        assert_that!(outcome.violations, empty());
        info!("Test 3 passed: Lock released after failure");
    }

    shutdown_test_env().await;
    info!("=== All tests passed ===");
    Ok(())
}

/// Tests that the seeded RNG produces deterministic results.
/// This test doesn't need RocksDB so it can run separately.
#[test]
fn test_seeded_rng_determinism() {
    use rand::{Rng, SeedableRng, rngs::StdRng};

    let seed = 999u64;

    let mut rng1 = StdRng::seed_from_u64(seed);
    let mut rng2 = StdRng::seed_from_u64(seed);

    for _ in 0..100 {
        let v1: u64 = rng1.random();
        let v2: u64 = rng2.random();
        assert_that!(v1, eq(v2));
    }

    let mut rng3 = StdRng::seed_from_u64(seed + 1);
    let v1: u64 = StdRng::seed_from_u64(seed).random();
    let v3: u64 = rng3.random();
    assert_that!(v1, not(eq(v3)));
}
