// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::sharding::WithPartitionKey;

pub trait OutboxMessage: super::Command + WithPartitionKey {
    /// Helper function that is only here to facilitate migration to opaque outbox message
    ///
    /// Drop in v1.9
    fn into_outbox_message(self) -> restate_storage_api::outbox_table::OutboxMessage;
}
