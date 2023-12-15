// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_schema_api::subscription::{Subscription, SubscriptionValidator};
use restate_types::invocation::InvocationTermination;
use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("worker is unreachable")]
    Unreachable,
}

// This is just an interface to isolate the interaction between meta and subscription controller.
// Depending on how we evolve the Kafka ingress deployment, this might end up living in a separate process.
pub trait SubscriptionController: SubscriptionValidator {
    type Future: Future<Output = Result<(), Error>> + Send;

    fn start_subscription(&self, subscription: Subscription) -> Self::Future;
    fn stop_subscription(&self, subscription_id: String) -> Self::Future;
}

pub trait Handle {
    type Future: Future<Output = Result<(), Error>> + Send;
    type SubscriptionControllerHandle: SubscriptionController + Send + Sync;

    /// Send a command to terminate an invocation. This command is best-effort.
    fn terminate_invocation(&self, invocation_termination: InvocationTermination) -> Self::Future;

    fn subscription_controller_handle(&self) -> Self::SubscriptionControllerHandle;
}
