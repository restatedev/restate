// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(feature = "cloud")]
pub mod cloud;
pub mod completions;
pub mod config;
pub mod deployments;
#[cfg(feature = "dev-cmd")]
pub mod dev;
pub mod examples;
pub mod invocations;
pub mod kafkaclusters;
pub mod openapi;
pub mod rules;
pub mod services;
pub mod sql;
pub mod state;
pub mod subscriptions;
pub mod vqueues;
pub mod whoami;

/// Shown before `kafka-clusters` / `subscriptions` commands: points to the new Kafka
/// integration. Human output only (stderr), never in `--json`.
pub fn kafka_integration_notice() {
    if !restate_cli_util::CliContext::get().json_output() {
        restate_cli_util::c_eprintln!();
        restate_cli_util::c_tip!(
            "The new Restate Kafka integration is out, check it out: \
             https://github.com/restatedev/ingress-integration-kafka"
        );
    }
}
