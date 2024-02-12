// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_schema_api::subscription::Subscription;
use restate_types::identifiers::SubscriptionId;
use restate_types::invocation::InvocationTermination;
use restate_types::state_mut::ExternalStateMutation;
use std::future::Future;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("worker is unreachable")]
    Unreachable,
}

// This is just an interface to isolate the interaction between meta and subscription controller.
// Depending on how we evolve the Kafka ingress deployment, this might end up living in a separate process.
pub trait SubscriptionController {
    fn start_subscription(
        &self,
        subscription: Subscription,
    ) -> impl Future<Output = Result<(), Error>> + Send;
    fn stop_subscription(
        &self,
        id: SubscriptionId,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Updates the subscription controller with the provided set of subscriptions. The subscription controller
    /// is supposed to only run the set of provided subscriptions after this call succeeds.
    fn update_subscriptions(
        &self,
        subscriptions: Vec<Subscription>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

pub trait Handle {
    /// Send a command to terminate an invocation. This command is best-effort.
    fn terminate_invocation(
        &self,
        invocation_termination: InvocationTermination,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Send a command to mutate a state. This command is best-effort.
    fn external_state_mutation(
        &self,
        mutation: ExternalStateMutation,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}
