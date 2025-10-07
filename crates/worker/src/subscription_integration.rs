// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_ingress_kafka::SubscriptionCommandSender;
use restate_types::identifiers::SubscriptionId;
use restate_types::schema::subscriptions::Subscription;

use crate::{SubscriptionController, WorkerHandleError};

#[derive(Debug, Clone)]
pub struct SubscriptionControllerHandle(SubscriptionCommandSender);

impl SubscriptionControllerHandle {
    pub(crate) fn new(commands_tx: SubscriptionCommandSender) -> Self {
        Self(commands_tx)
    }
}

impl SubscriptionController for SubscriptionControllerHandle {
    async fn start_subscription(
        &self,
        subscription: Subscription,
    ) -> Result<(), WorkerHandleError> {
        self.0
            .send(restate_ingress_kafka::Command::StartSubscription(
                subscription,
            ))
            .await
            .map_err(|_| WorkerHandleError::Unreachable)
    }

    async fn stop_subscription(&self, id: SubscriptionId) -> Result<(), WorkerHandleError> {
        self.0
            .send(restate_ingress_kafka::Command::StopSubscription(id))
            .await
            .map_err(|_| WorkerHandleError::Unreachable)
    }

    async fn update_subscriptions(
        &self,
        subscriptions: Vec<Subscription>,
    ) -> Result<(), WorkerHandleError> {
        self.0
            .send(restate_ingress_kafka::Command::UpdateSubscriptions(
                subscriptions,
            ))
            .await
            .map_err(|_| WorkerHandleError::Unreachable)
    }
}
