// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_ingress_kafka::SubscriptionCommandSender;
use restate_schema_api::subscription::{Subscription, SubscriptionValidator};
use restate_worker_api::SubscriptionController;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SubscriptionControllerHandle(
    Arc<restate_ingress_kafka::Options>,
    SubscriptionCommandSender,
);

impl SubscriptionControllerHandle {
    pub(crate) fn new(
        kafka_options: restate_ingress_kafka::Options,
        commands_tx: SubscriptionCommandSender,
    ) -> Self {
        Self(Arc::new(kafka_options), commands_tx)
    }
}

impl SubscriptionValidator for SubscriptionControllerHandle {
    type Error = <restate_ingress_kafka::Options as SubscriptionValidator>::Error;

    fn validate(&self, subscription: Subscription) -> Result<Subscription, Self::Error> {
        SubscriptionValidator::validate(self.0.deref(), subscription)
    }
}

impl SubscriptionController for SubscriptionControllerHandle {
    async fn start_subscription(
        &self,
        subscription: Subscription,
    ) -> Result<(), restate_worker_api::Error> {
        self.1
            .send(restate_ingress_kafka::Command::StartSubscription(
                subscription,
            ))
            .await
            .map_err(|_| restate_worker_api::Error::Unreachable)
    }

    async fn stop_subscription(
        &self,
        subscription_id: String,
    ) -> Result<(), restate_worker_api::Error> {
        self.1
            .send(restate_ingress_kafka::Command::StopSubscription(
                subscription_id,
            ))
            .await
            .map_err(|_| restate_worker_api::Error::Unreachable)
    }
}
