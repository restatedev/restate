// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use restate_types::identifiers::SubscriptionId;
use restate_types::schema::subscriptions::Subscription;

use crate::WorkerHandleError;

// This is just an interface to isolate the interaction between meta and subscription controller.
// Depending on how we evolve the Kafka ingress deployment, this might end up living in a separate process.
pub trait SubscriptionController {
    fn start_subscription(
        &self,
        subscription: Subscription,
    ) -> impl Future<Output = Result<(), WorkerHandleError>> + Send;
    fn stop_subscription(
        &self,
        id: SubscriptionId,
    ) -> impl Future<Output = Result<(), WorkerHandleError>> + Send;

    /// Updates the subscription controller with the provided set of subscriptions. The subscription controller
    /// is supposed to only run the set of provided subscriptions after this call succeeds.
    fn update_subscriptions(
        &self,
        subscriptions: Vec<Subscription>,
    ) -> impl Future<Output = Result<(), WorkerHandleError>> + Send;
}
