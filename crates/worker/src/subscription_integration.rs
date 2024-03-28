// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{SubscriptionController, WorkerHandleError};
use restate_ingress_kafka::SubscriptionCommandSender;
use restate_schema_api::subscription::{Subscription, SubscriptionValidator};
use restate_types::identifiers::SubscriptionId;
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
    ) -> Result<(), WorkerHandleError> {
        self.1
            .send(restate_ingress_kafka::Command::StartSubscription(
                subscription,
            ))
            .await
            .map_err(|_| WorkerHandleError::Unreachable)
    }

    async fn stop_subscription(&self, id: SubscriptionId) -> Result<(), WorkerHandleError> {
        self.1
            .send(restate_ingress_kafka::Command::StopSubscription(id))
            .await
            .map_err(|_| WorkerHandleError::Unreachable)
    }

    async fn update_subscriptions(
        &self,
        subscriptions: Vec<Subscription>,
    ) -> Result<(), WorkerHandleError> {
        self.1
            .send(restate_ingress_kafka::Command::UpdateSubscriptions(
                subscriptions,
            ))
            .await
            .map_err(|_| WorkerHandleError::Unreachable)
    }
}
