// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use sha2::{Digest, Sha256};

use restate_limiter::LimitKey;
use restate_types::Scope;
use restate_types::identifiers::PartitionKey;
use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType};
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;

pub fn generate_vqueue_id(
    partition_key: PartitionKey,
    scope: Option<&Scope>,
    limit_key: &LimitKey<ReString>,
    is_exclusive: bool,
    service_name: &str,
    key: Option<&str>,
) -> VQueueId {
    const SEP_CHAR: u8 = 0xFF;
    // separator is 0xFF. (notation: `||`)
    const HASH_SEPARATOR: &[u8] = &[SEP_CHAR];
    /// Shared handlers get constant value b`0`
    const SHARED_HANDLER: &[u8] = b"0";
    /// Exclusive handlers get constant value b`1`
    const EXCLUSIVE_HANDLER: &[u8] = b"1";
    // ty is reserved for future use (for now, it's always 0)
    const INTERNAL_VQUEUE_TY: &[u8] = &[SEP_CHAR, b'0', SEP_CHAR];

    let mut hasher = Sha256::new();
    // for length-prefixed values, the notation is len(). and 0 is used if the value is not
    // present. This is used in optional fields in the middle of the template design.
    // it's assumed that lengths all fit into u32 which is a very reasonable assumption to make for
    // the values we're hashing.
    //
    // let example = "<partition-key> || ty || len(scope) || len(limit-key) || <exclusive/shared?> || service || key";

    // Partition key
    hasher.update(partition_key.to_le_bytes());
    hasher.update(INTERNAL_VQUEUE_TY);

    // Scope
    if let Some(scope) = scope {
        hasher.update((scope.len() as u32).to_le_bytes());
        hasher.update(scope.as_bytes());
    } else {
        hasher.update(0u32.to_le_bytes());
    }

    hasher.update(HASH_SEPARATOR);

    // Limit Key
    match limit_key {
        LimitKey::None => {
            hasher.update(0u32.to_le_bytes());
        }
        LimitKey::L1(l1) => {
            hasher.update((l1.len() as u32).to_le_bytes());
            hasher.update(l1.as_bytes());
        }
        LimitKey::L2(l1, l2) => {
            hasher.update(((l1.len() + l2.len()) as u32).to_le_bytes());
            hasher.update(l1.as_bytes());
            hasher.update([b'/']);
            hasher.update(l2.as_bytes());
        }
    }

    hasher.update(HASH_SEPARATOR);

    // Exclusive?
    if is_exclusive {
        hasher.update(EXCLUSIVE_HANDLER);
    } else {
        hasher.update(SHARED_HANDLER);
    }

    hasher.update(HASH_SEPARATOR);

    // Service Name
    hasher.update(service_name);

    // Key (if any)
    if let Some(key) = key {
        hasher.update(HASH_SEPARATOR);
        hasher.update(key);
    }

    let bytes = hasher.finalize();

    VQueueId::new(partition_key, &bytes)
}

pub fn infer_vqueue_id_from_invocation(
    partition_key: PartitionKey,
    invocation_target: &InvocationTarget,
    limit_key: &LimitKey<ReString>,
) -> VQueueId {
    match invocation_target {
        InvocationTarget::Service { name, scope, .. } => {
            // Shared service.
            generate_vqueue_id(partition_key, scope.as_ref(), limit_key, false, name, None)
        }
        InvocationTarget::VirtualObject {
            handler_ty,
            key,
            name,
            scope,
            ..
        } => {
            match handler_ty {
                VirtualObjectHandlerType::Exclusive => generate_vqueue_id(
                    partition_key,
                    scope.as_ref(),
                    limit_key,
                    true,
                    name,
                    Some(key),
                ),
                VirtualObjectHandlerType::Shared => {
                    // Note: we don't use the "key" here to reduce cardinality.
                    // the downside of this is that we get less scheduler-level fairness
                    // shared handler calls across multiple VOs, but that's on-par with
                    // what we do with normal services.
                    generate_vqueue_id(partition_key, scope.as_ref(), limit_key, false, name, None)
                }
            }
        }
        InvocationTarget::Workflow { name, scope, .. } => {
            // Workflows behave like shared services.
            generate_vqueue_id(partition_key, scope.as_ref(), limit_key, false, name, None)
        }
    }
}
