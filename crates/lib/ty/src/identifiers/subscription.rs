// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ulid_backed_id;

ulid_backed_id!(Subscription @with_resource_id);

#[cfg(test)]
mod tests {
    use crate::identifiers::TimestampAwareId;

    use super::*;

    #[test]
    fn test_subscription_id_format() {
        let a = SubscriptionId::new();
        assert!(a.timestamp_ms() > 0);
        let a_str = a.to_string();
        assert!(a_str.starts_with("sub_"));
    }

    #[test]
    fn test_subscription_roundtrip() {
        let a = SubscriptionId::new();
        let b: SubscriptionId = a.to_string().parse().unwrap();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), b.to_string());
    }
}
