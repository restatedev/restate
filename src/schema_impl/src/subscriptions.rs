// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Schemas;

use restate_schema_api::subscription::{Subscription, SubscriptionResolver};

impl SubscriptionResolver for Schemas {
    fn get_subscription(&self, id: &str) -> Option<Subscription> {
        let schemas = self.0.load();
        schemas.subscriptions.get(id).cloned()
    }
}
