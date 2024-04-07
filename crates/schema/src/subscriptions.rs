// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{SchemaInformation, UpdatingSchemaInformation};

use restate_schema_api::subscription::{
    ListSubscriptionFilter, Subscription, SubscriptionResolver,
};
use restate_types::identifiers::SubscriptionId;

impl SubscriptionResolver for SchemaInformation {
    fn get_subscription(&self, id: SubscriptionId) -> Option<Subscription> {
        self.subscriptions.get(&id).cloned()
    }

    fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription> {
        self.subscriptions
            .values()
            .filter(|sub| {
                for f in filters {
                    if !f.matches(sub) {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }
}

impl SubscriptionResolver for UpdatingSchemaInformation {
    fn get_subscription(&self, id: SubscriptionId) -> Option<Subscription> {
        self.0.load().get_subscription(id)
    }

    fn list_subscriptions(&self, filters: &[ListSubscriptionFilter]) -> Vec<Subscription> {
        self.0.load().list_subscriptions(filters)
    }
}
